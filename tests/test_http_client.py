"""
HTTP client tests, focused on robots.txt handling.

These exist because of a real, silent failure: the scraped source contributed
zero images to the first real dataset, and the logs said only "robots.txt
disallows". Commons had disallowed nothing — the *robots fetch itself* was
being rejected, and the standard library turned that rejection into a blanket
"deny everything".
"""

from __future__ import annotations

import threading
import time

import pytest

from src.config import HttpSettings
from src.http.client import HttpClient, _HostThrottle


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
    import requests

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
