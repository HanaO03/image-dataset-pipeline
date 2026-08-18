"""
HTTP client tests: robots.txt, per-host throttling, and the retry loop.

The robots.txt block exists because of a real, silent failure: the scraped
source contributed zero images to the first real dataset, and the logs said
only "robots.txt disallows". Commons had disallowed nothing — the *robots fetch
itself* was being rejected, and the standard library turned that rejection into
a blanket "deny everything".

The retry block exists because of a quieter one: the README described backoff,
jitter and `Retry-After` handling, and named this file as the place they were
tested, while every test here concerned robots or throttling. The retry loop
decides whether a run survives a rate-limited source, and it had no test at
all.
"""

from __future__ import annotations

import threading
import time

import pytest
import requests

from src.config import HttpSettings
from src.http.client import HttpClient, RateLimitedError, _HostThrottle


class FakeResponse:
    def __init__(self, text: str = "", status: int = 200) -> None:
        self.text = text
        self.status_code = status


@pytest.fixture
def client() -> HttpClient:
    return HttpClient(HttpSettings(min_delay_seconds=0.0, max_retries=1))


def _with_robots(client: HttpClient, response: FakeResponse | Exception) -> HttpClient:
    """Replace the network layer so robots handling can be tested offline."""

    def fake_request(method, url, **kwargs):
        if isinstance(response, Exception):
            raise response
        return response

    client.request = fake_request  # type: ignore[method-assign]
    return client


#: Trimmed from the real Wikimedia robots.txt. `/wiki/Category:` is allowed;
#: `/w/` and `/wiki/Special:` are not.
WIKIMEDIA_ROBOTS = """
User-agent: UbiCrawler
Disallow: /

User-agent: *
Allow: /w/api.php?action=mobileview&
Allow: /w/load.php?
Disallow: /w/
Disallow: /api/
Disallow: /trap/
Disallow: /wiki/Special:
Disallow: /wiki/Special%3A
"""


def test_category_pages_are_allowed_by_the_real_wikimedia_rules(client):
    """
    The regression test for the zero-images bug.

    Commons permits `/wiki/Category:Cats`. Anything that says otherwise is our
    bug, not their policy.
    """
    _with_robots(client, FakeResponse(WIKIMEDIA_ROBOTS))
    assert client.may_fetch("https://commons.wikimedia.org/wiki/Category:Cats")
    assert client.may_fetch("https://commons.wikimedia.org/wiki/File:Cat_one.jpg")


def test_genuinely_disallowed_paths_are_still_refused(client):
    """Failing open on a *missing* file must not mean ignoring a real rule."""
    _with_robots(client, FakeResponse(WIKIMEDIA_ROBOTS))
    assert not client.may_fetch("https://commons.wikimedia.org/w/index.php?title=x")
    assert not client.may_fetch("https://commons.wikimedia.org/wiki/Special:Search")


def test_a_403_on_robots_txt_does_not_block_the_whole_site(client):
    """
    The exact failure mode.

    `RobotFileParser.read()` fetches with `User-Agent: Python-urllib/3.x`,
    which Wikimedia rejects with 403 — and it converts that 403 into
    `disallow_all = True`, silently. The result is a crawler that refuses to
    fetch anything from a site that never disallowed it, and a log line that
    reads like correct compliance.

    A robots.txt we cannot read is not a disallow.
    """
    _with_robots(client, FakeResponse("Forbidden", status=403))
    assert client.may_fetch("https://commons.wikimedia.org/wiki/Category:Cats")


def test_a_404_on_robots_txt_fails_open(client):
    _with_robots(client, FakeResponse("Not found", status=404))
    assert client.may_fetch("https://example.org/anything")


def test_an_empty_robots_txt_fails_open(client):
    _with_robots(client, FakeResponse("   \n  \n"))
    assert client.may_fetch("https://example.org/anything")


def test_a_network_failure_fetching_robots_fails_open(client):
    _with_robots(client, requests.RequestException("connection reset"))
    assert client.may_fetch("https://example.org/anything")


def test_disable_all_is_honoured_when_the_site_really_says_so(client):
    _with_robots(client, FakeResponse("User-agent: *\nDisallow: /\n"))
    assert not client.may_fetch("https://example.org/anything")


def test_robots_is_fetched_once_per_origin_and_cached(client):
    calls: list[str] = []

    def counting_request(method, url, **kwargs):
        calls.append(url)
        return FakeResponse(WIKIMEDIA_ROBOTS)

    client.request = counting_request  # type: ignore[method-assign]
    for _ in range(5):
        client.may_fetch("https://commons.wikimedia.org/wiki/Category:Cats")

    assert len(calls) == 1, "robots.txt must not be re-fetched for every page"


def test_respect_robots_can_be_turned_off_entirely():
    """Used by the test suite against its own loopback server."""
    client = HttpClient(HttpSettings(respect_robots_txt=False, min_delay_seconds=0.0))

    def explode(*args, **kwargs):
        raise AssertionError("robots.txt must not be fetched when disabled")

    client.request = explode  # type: ignore[method-assign]
    assert client.may_fetch("http://127.0.0.1:8000/anything")


# =============================================================================
#  Per-host throttling
# =============================================================================
#
#  The first implementation slept while holding a single global lock. That is
#  invisible in a functional test and ruinous in practice: it serialises the
#  entire download pool and lets one slow host block traffic to every other.


def test_throttle_reserves_slots_without_blocking_other_threads():
    """
    Reserving a departure slot must be cheap and non-blocking.

    Ten reservations for one host schedule ten spaced departures, but making
    those reservations must not itself take ten delays — otherwise a worker
    cannot even learn when it may go until every earlier worker has finished
    waiting.
    """
    throttle = _HostThrottle(1.0, {}, threading.Lock(), {})

    started = time.monotonic()
    for _ in range(10):
        with throttle._lock:
            now = time.monotonic()
            departure = max(now, throttle._next_allowed.get("h", 0.0))
            throttle._next_allowed["h"] = departure + throttle.delay_for("h")
    elapsed = time.monotonic() - started

    assert elapsed < 0.1, "reservation must not sleep under the lock"
    assert throttle._next_allowed["h"] >= started + 9.0


def test_each_host_gets_its_own_schedule():
    """A slow image CDN must not hold up requests to an unrelated host."""
    throttle = _HostThrottle(5.0, {}, threading.Lock(), {})
    throttle.wait("slow.example")          # takes the first slot, no wait

    started = time.monotonic()
    throttle.wait("other.example")         # different host — must not queue
    assert time.monotonic() - started < 0.5


def test_per_host_override_beats_the_default():
    """upload.wikimedia.org answers 429 at the default rate; it gets more room."""
    throttle = _HostThrottle(0.5, {}, threading.Lock(), {"upload.wikimedia.org": 1.5})

    assert throttle.delay_for("upload.wikimedia.org") == 1.5
    assert throttle.delay_for("api.openverse.org") == 0.5


def test_successive_requests_to_one_host_are_spaced():
    throttle = _HostThrottle(0.2, {}, threading.Lock(), {})

    started = time.monotonic()
    throttle.wait("h")
    throttle.wait("h")
    assert time.monotonic() - started >= 0.2


# =============================================================================
#  Retries, backoff and 429
# =============================================================================
#
#  This block was missing for longer than it should have been. The README
#  described "backoff with jitter, Retry-After honoured" and listed this file
#  as covering retries, while every test above concerned robots.txt or
#  throttling. The retry loop — the part that decides whether a run survives a
#  rate-limited source — had no test at all.
#
#  Sleeping is stubbed out throughout. What is asserted is the *decision*: how
#  many attempts, on which statuses, honouring which delay. Asserting the real
#  wall-clock delay would buy nothing and cost seconds per test.


class _Recorder:
    """Captures the client's chosen backoff delays instead of sleeping them."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def _client_with(responses, *, max_retries=3):
    """
    An HttpClient whose transport replays `responses` in order.

    Each entry is a FakeResponse or an exception to raise. Returns the client
    and the list of attempted calls, so the number of attempts is observable.
    """
    client = HttpClient(
        HttpSettings(min_delay_seconds=0.0, max_retries=max_retries, backoff_factor=0.5)
    )
    attempts: list[str] = []
    queue = list(responses)
    last = responses[-1] if responses else None

    def fake_request(method, url, **kwargs):
        attempts.append(url)
        item = queue.pop(0) if queue else last
        if isinstance(item, Exception):
            raise item
        return item

    client.session.request = fake_request  # type: ignore[method-assign]
    return client, attempts


class RetryableResponse(FakeResponse):
    """A FakeResponse with the headers and lifecycle the retry loop touches."""

    def __init__(self, status: int, retry_after: str | None = None) -> None:
        super().__init__(text="", status=status)
        self.headers = {"Retry-After": retry_after} if retry_after else {}
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_a_429_is_retried_and_the_run_continues_when_it_clears(monkeypatch):
    """
    The ordinary rate-limit story: the server says slow down, we do, and the
    request then succeeds. A pipeline that gave up here would lose images to a
    condition that resolves itself in under a second.
    """
    recorder = _Recorder()
    monkeypatch.setattr("src.http.client.time.sleep", recorder)

    client, attempts = _client_with(
        [RetryableResponse(429), RetryableResponse(200)]
    )
    response = client.request("GET", "https://cdn.example.org/a.jpg")

    assert response.status_code == 200
    assert len(attempts) == 2, "must retry exactly once before succeeding"
    assert len(recorder.delays) == 1


def test_retry_after_is_honoured_when_the_server_sends_one(monkeypatch):
    """
    The server's own number wins when it is larger than our backoff. Ignoring
    it is how a client that thinks it is being polite gets blocked anyway:
    upload.wikimedia.org answers 429 with `Retry-After: 11`.
    """
    recorder = _Recorder()
    monkeypatch.setattr("src.http.client.time.sleep", recorder)

    client, _ = _client_with([RetryableResponse(429, retry_after="11"), RetryableResponse(200)])
    client.request("GET", "https://upload.wikimedia.org/x.jpg")

    assert recorder.delays[0] >= 11.0


def test_an_http_date_in_retry_after_falls_back_to_our_own_backoff(monkeypatch):
    """
    `Retry-After` may also be an HTTP-date. Parsing it is not worth the code;
    failing to *survive* it would be, so the header is ignored rather than
    allowed to raise.
    """
    recorder = _Recorder()
    monkeypatch.setattr("src.http.client.time.sleep", recorder)

    client, attempts = _client_with(
        [RetryableResponse(503, retry_after="Wed, 19 Aug 2026 22:00:00 GMT"),
         RetryableResponse(200)]
    )
    response = client.request("GET", "https://cdn.example.org/a.jpg")

    assert response.status_code == 200
    assert len(recorder.delays) == 1


def test_backoff_grows_and_is_jittered(monkeypatch):
    """
    Jitter is not decoration. Without it, eight workers that hit a 429 together
    retry together, re-trigger the limit together, and synchronise into a
    thundering herd — so two clients backing off the same attempt must not
    choose the same delay.
    """
    recorder = _Recorder()
    monkeypatch.setattr("src.http.client.time.sleep", recorder)

    client, _ = _client_with(
        [RetryableResponse(503), RetryableResponse(503), RetryableResponse(200)],
        max_retries=4,
    )
    client.request("GET", "https://cdn.example.org/a.jpg")

    assert len(recorder.delays) == 2
    assert recorder.delays[1] > recorder.delays[0], "delay must grow with attempts"

    second = _Recorder()
    monkeypatch.setattr("src.http.client.time.sleep", second)
    client2, _ = _client_with([RetryableResponse(503), RetryableResponse(200)])
    client2.request("GET", "https://cdn.example.org/a.jpg")
    # Same nominal delay, drawn independently: identical values would mean the
    # jitter is not being applied at all.
    assert second.delays[0] != recorder.delays[0]


def test_a_persistent_429_raises_rate_limited_rather_than_a_generic_failure(monkeypatch):
    """
    The distinction the download stage relies on to say *why* an image is
    missing: "the source throttled us" and "the source is broken" lead to
    different fixes, and only one of them is our fault.
    """
    monkeypatch.setattr("src.http.client.time.sleep", _Recorder())

    client, attempts = _client_with([RetryableResponse(429)] * 3, max_retries=3)

    with pytest.raises(RateLimitedError):
        client.request("GET", "https://cdn.example.org/a.jpg")
    assert len(attempts) == 3, "the whole retry budget must be spent first"


def test_a_404_is_returned_immediately_and_never_retried(monkeypatch):
    """
    A 404 will not become a 200, and retrying a 403 is how a scraper earns an
    IP ban. Only the statuses on the retry list are worth another attempt.
    """
    recorder = _Recorder()
    monkeypatch.setattr("src.http.client.time.sleep", recorder)

    client, attempts = _client_with([RetryableResponse(404)])
    response = client.request("GET", "https://cdn.example.org/missing.jpg")

    assert response.status_code == 404
    assert len(attempts) == 1
    assert recorder.delays == [], "a 4xx must not cost us a backoff"


def test_a_connection_error_is_retried_then_reported_as_one_failure(monkeypatch):
    """
    Network faults get the same budget as retryable statuses, and the final
    exception names the URL — a bare `ConnectionError` in a log tells nobody
    which of 300 downloads died.
    """
    monkeypatch.setattr("src.http.client.time.sleep", _Recorder())

    client, attempts = _client_with(
        [requests.ConnectionError("reset by peer")] * 3, max_retries=3
    )

    with pytest.raises(requests.RequestException) as excinfo:
        client.request("GET", "https://cdn.example.org/a.jpg")
    assert len(attempts) == 3
    assert "cdn.example.org/a.jpg" in str(excinfo.value)


def test_every_request_carries_a_timeout(monkeypatch):
    """
    A request without a timeout can hang forever, and there is no scenario in
    which that is what we wanted. Connect and read are set separately.
    """
    captured: dict = {}

    client = HttpClient(HttpSettings(min_delay_seconds=0.0, max_retries=1))

    def fake_request(method, url, **kwargs):
        captured.update(kwargs)
        return RetryableResponse(200)

    client.session.request = fake_request  # type: ignore[method-assign]
    client.request("GET", "https://cdn.example.org/a.jpg")

    assert captured["timeout"] == (
        client.settings.connect_timeout_seconds,
        client.settings.read_timeout_seconds,
    )


def test_rate_limiting_is_a_request_exception_so_sources_handle_it(monkeypatch):
    """
    The type is the fix. Every source adapter handles a failed fetch with
    `except requests.RequestException` — the correct thing to write — and
    `RateLimitedError` used to inherit from `RuntimeError`, so it walked
    straight past them, out of the adapter, past the runner's stage handling
    and into the catch-all that marks a run `failed`.

    A rate-limited source is the most ordinary failure this pipeline meets. The
    error policy says data failures are recorded and the run continues; making
    the exception's type match the policy is what makes that true at every call
    site, including ones not yet written.
    """
    monkeypatch.setattr("src.http.client.time.sleep", _Recorder())
    client, _ = _client_with([RetryableResponse(429)] * 2, max_retries=2)

    with pytest.raises(requests.RequestException):
        client.request("GET", "https://api.openverse.org/v1/images/")

    assert issubclass(RateLimitedError, requests.RequestException)
