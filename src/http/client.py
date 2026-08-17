"""
The single outbound HTTP path for the whole pipeline.

Centralised because politeness and resilience are properties of the *system*,
not of individual call sites. If rate limiting lives in each source adapter,
one adapter eventually forgets and the whole project gets throttled or banned.

What this module guarantees for every request the pipeline makes:
  * an identifying User-Agent (Wikimedia's policy requires one; anonymous
    scrapers get blocked)
  * a per-host minimum delay
  * retries with exponential backoff and jitter, on retryable statuses only
  * `Retry-After` is honoured when the server sends it
  * connect and read timeouts, always — a request without a timeout can hang
    forever and there is no scenario where that is what we wanted
  * robots.txt consulted before scraping
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterator
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

from ..config import HttpSettings
from ..logging_setup import get_logger

log = get_logger(__name__)


class RateLimitedError(RuntimeError):
    """Retry budget exhausted against a server that kept saying 429."""


class NotAnImageError(RuntimeError):
    """The response body is not an image (usually an HTML error page)."""


class TooLargeError(RuntimeError):
    """Response exceeded max_download_bytes; aborted mid-stream."""


@dataclass
class _HostThrottle:
    """
    Per-host minimum spacing, shared by every worker thread.

    Two things here are load-bearing and were both wrong in the first version:

    * **The sleep happens outside the lock.** Holding the lock while sleeping
      serialises the entire thread pool — `download_workers=8` then behaves
      exactly like a serial loop, because worker 2 cannot even *compute* its
      own delay until worker 1 has finished sleeping.
    * **Each host gets its own slot in the schedule.** With one shared lock,
      waiting on a slow image CDN also blocked requests to Commons, which share
      nothing with it. Reserving a departure time per host lets different hosts
      proceed in parallel while each host is still politely rate limited.

    The lock is therefore held only long enough to *reserve* a departure slot:
    each caller takes the next free moment for its host and pushes the host's
    next slot `min_delay` further out. Reservation is atomic, waiting is not.
    """

    min_delay: float
    #: host -> earliest monotonic time at which the next request may depart.
    _next_allowed: dict[str, float]
    _lock: threading.Lock
    #: Hosts that need more room than the default. See HttpSettings.
    _overrides: dict[str, float] = field(default_factory=dict)

    def delay_for(self, host: str) -> float:
        return self._overrides.get(host, self.min_delay)

    def wait(self, host: str) -> None:
        with self._lock:
            now = time.monotonic()
            departure = max(now, self._next_allowed.get(host, 0.0))
            self._next_allowed[host] = departure + self.delay_for(host)
        delay = departure - now
        if delay > 0:
            time.sleep(delay)


class HttpClient:
    """
    A thin, deliberately boring wrapper over `requests`.

    Retries are hand-rolled rather than delegated to urllib3's `Retry` adapter
    for one reason: we need to distinguish "server asked us to slow down" from
    "server is broken", honour `Retry-After` when present, and log each attempt
    with its reason. urllib3's retry is silent, and a pipeline that retries
    invisibly is a pipeline you cannot debug.
    """

    def __init__(self, settings: HttpSettings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": settings.user_agent,
                "Accept-Encoding": "gzip, deflate",
            }
        )
        self._throttle = _HostThrottle(
            settings.min_delay_seconds,
            {},
            threading.Lock(),
            dict(settings.min_delay_by_host),
        )
        self._robots: dict[str, RobotFileParser | None] = {}
        self._robots_lock = threading.Lock()

    # -------------------------------------------------------------------------
    #  robots.txt
    # -------------------------------------------------------------------------

    def may_fetch(self, url: str) -> bool:
        """
        Consult robots.txt before scraping. Fails *open* on a robots.txt we
        cannot retrieve: an unreachable robots file is not a disallow, and
        treating it as one would silently produce an empty dataset.
        """
        if not self.settings.respect_robots_txt:
            return True
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        with self._robots_lock:
            if origin not in self._robots:
                self._robots[origin] = self._load_robots(origin)
            parser = self._robots[origin]

        if parser is None:
            return True

        allowed = parser.can_fetch(self.settings.user_agent, url)
        if not allowed:
            # Distinguishable from "could not read robots.txt": this is the
            # site genuinely telling us not to.
            log.warning("robots.txt explicitly disallows this path",
                        extra={"url": url, "user_agent": self.settings.user_agent})
        return allowed

    def _load_robots(self, origin: str) -> RobotFileParser | None:
        """
        Fetch and parse robots.txt **through our own session**.

        This is deliberately not `RobotFileParser.read()`, and the reason is the
        bug that made the scraped source contribute zero images.

        `RobotFileParser.read()` fetches via `urllib.request.urlopen`, which
        sends `User-Agent: Python-urllib/3.x`. Wikimedia rejects that UA with
        **403 Forbidden**. And `read()` handles 401/403 by setting
        `self.disallow_all = True` — silently, without raising. Every
        subsequent `can_fetch()` then returned False, so the scraper politely
        refused to fetch a single page from a site that had never actually
        disallowed anything. The failure looked exactly like correct
        robots-compliance, which is why it survived review.

        Fetching through `self.request` sends our real, identifying User-Agent
        — the one Wikimedia's own policy asks for — and gets the real file.

        Failure to *read* robots.txt fails **open**: an unreachable or absent
        robots file is not a disallow, and treating it as one silently produces
        an empty dataset. A `Disallow` rule in a file we successfully read is
        still fully respected.
        """
        url = f"{origin}/robots.txt"
        try:
            response = self.request("GET", url)
        except requests.RequestException as exc:
            log.warning("robots.txt unreachable, proceeding",
                        extra={"origin": origin, "error": str(exc)})
            return None

        if response.status_code != 200 or not response.text.strip():
            log.warning(
                "robots.txt not served, proceeding",
                extra={"origin": origin, "status": response.status_code},
            )
            return None

        parser = RobotFileParser()
        try:
            parser.parse(response.text.splitlines())
        except Exception as exc:
            log.warning("robots.txt unparseable, proceeding",
                        extra={"origin": origin, "error": str(exc)})
            return None

        log.info("robots.txt loaded", extra={"origin": origin,
                                             "bytes": len(response.text)})
        return parser

    # -------------------------------------------------------------------------
    #  Core request with retries
    # -------------------------------------------------------------------------

    def request(
        self, method: str, url: str, *, stream: bool = False, **kwargs: Any
    ) -> requests.Response:
        host = urlparse(url).netloc
        timeout = (self.settings.connect_timeout_seconds, self.settings.read_timeout_seconds)
        last_exc: Exception | None = None

        for attempt in range(1, self.settings.max_retries + 1):
            self._throttle.wait(host)
            try:
                response = self.session.request(
                    method, url, timeout=timeout, stream=stream, **kwargs
                )
            except requests.RequestException as exc:
                # Network-level failure (DNS, connection reset, read timeout).
                last_exc = exc
                if attempt == self.settings.max_retries:
                    break
                self._sleep_backoff(attempt, url, reason=type(exc).__name__)
                continue

            if response.status_code not in self.settings.retry_on_status:
                # Includes 4xx: a 404 will never become a 200, and retrying a
                # 403 is how a scraper earns an IP ban. Let the caller decide.
                return response

            retry_after = self._parse_retry_after(response)
            log.warning(
                "retryable status",
                extra={
                    "url": url,
                    "status": response.status_code,
                    "attempt": attempt,
                    "retry_after": retry_after,
                },
            )
            response.close()
            if attempt == self.settings.max_retries:
                if response.status_code == 429:
                    raise RateLimitedError(f"429 from {host} after {attempt} attempts")
                last_exc = requests.HTTPError(f"{response.status_code} from {url}")
                break
            self._sleep_backoff(attempt, url, reason=str(response.status_code), floor=retry_after)

        raise requests.RequestException(
            f"{method} {url} failed after {self.settings.max_retries} attempts: {last_exc}"
        ) from last_exc

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        response = self.get(url, **kwargs)
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------------------------------
    #  Streaming download
    # -------------------------------------------------------------------------

    def stream_bytes(self, url: str, max_bytes: int | None = None) -> Iterator[bytes]:
        """
        Yield the body in chunks, aborting the moment the size cap is exceeded.

        Enforcing the cap *during* streaming rather than by trusting
        `Content-Length` matters: the header is advisory, frequently absent on
        chunked responses, and trivially wrong. A hostile or misconfigured
        server should not be able to fill the volume.
        """
        cap = max_bytes if max_bytes is not None else self.settings.max_download_bytes
        response = self.request("GET", url, stream=True)
        try:
            response.raise_for_status()

            declared = (response.headers.get("Content-Type") or "").split(";")[0].strip()
            # Cheap early exit for the classic failure: a dead image link that
            # 200s with an HTML error page. Empty Content-Type is tolerated —
            # some CDNs omit it — and caught later by decoding the bytes.
            if declared and not declared.startswith("image/"):
                raise NotAnImageError(f"Content-Type: {declared}")

            total = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > cap:
                    raise TooLargeError(f"exceeded {cap} bytes")
                yield chunk
        finally:
            response.close()

    # -------------------------------------------------------------------------
    #  Internals
    # -------------------------------------------------------------------------

    def _sleep_backoff(
        self, attempt: int, url: str, reason: str, floor: float | None = None
    ) -> None:
        """
        Exponential backoff with jitter.

        Jitter is not decoration: without it, N parallel workers that hit a 429
        together retry together, re-trigger the limit together, and synchronise
        into a thundering herd. Randomising spreads them out.
        """
        delay = self.settings.backoff_factor * (2 ** (attempt - 1))
        delay *= 0.5 + random.random()  # jitter: 50%-150% of nominal
        if floor is not None:
            delay = max(delay, floor)
        log.debug("backing off", extra={"url": url, "seconds": round(delay, 2), "reason": reason})
        time.sleep(delay)

    @staticmethod
    def _parse_retry_after(response: requests.Response) -> float | None:
        """Honour the server's own instruction when it gives one."""
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None  # HTTP-date form; backoff default is close enough

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
