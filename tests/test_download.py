"""
Download-stage rejections.

Four reason codes are produced by this stage and by nothing else —
`NOT_AN_IMAGE`, `TOO_LARGE`, `EMPTY_RESPONSE`, `TIMEOUT` — and until these
tests existed, none of them had one. That gap is worth naming rather than
quietly closing: the README claimed every failure mode had a reason code *and
a test*, and for these four the second half was not true. A claim in a README
is a test that has not been written yet.

Each case here is driven through the real `download_one`, with only the
network substituted. The temp-file handling, the streaming hash, the atomic
rename and the exception mapping are all the production ones — which is the
point, because the mapping from exception to reason code is exactly what was
untested.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import requests

from src.config import PathSettings, Settings
from src.http.client import NotAnImageError, RateLimitedError, TooLargeError
from src.models import RawRecord, RejectionReason, SourceName
from src.pipeline.download import download_one


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(paths=PathSettings(data_dir=tmp_path))


@pytest.fixture
def record() -> RawRecord:
    return RawRecord(
        source=SourceName.OPENVERSE,
        class_label="cat",
        image_url="https://cdn.example.org/photo.jpg",
        source_id="ov-1",
        license_raw="by-4.0",
    )


class FakeClient:
    """
    Stands in for HttpClient with one method: the one download_one calls.

    `chunks` is either an iterable of byte strings to yield, or an exception
    to raise — which is how each failure mode below is reproduced without a
    server.
    """

    def __init__(self, chunks) -> None:
        self.chunks = chunks

    def stream_bytes(self, url: str, max_bytes: int | None = None):
        if isinstance(self.chunks, Exception):
            raise self.chunks
        yield from self.chunks


# =============================================================================
#  The four codes this stage owns
# =============================================================================


def test_an_html_error_page_at_an_image_url_is_not_an_image(settings, record):
    """
    The classic dead link: 200 OK, `Content-Type: text/html`, an error page in
    the body. The client raises before a byte is written; the stage must turn
    that into a reason code rather than an exception.
    """
    result, rejection = download_one(
        record, FakeClient(NotAnImageError("Content-Type: text/html")), settings
    )

    assert result is None
    assert rejection.reason_code is RejectionReason.NOT_AN_IMAGE
    assert "text/html" in rejection.detail


def test_a_file_over_the_cap_is_rejected_not_written(settings, record):
    """
    The size cap is enforced mid-stream, so this fires after some bytes have
    already been written to the temp file. Nothing may survive in the store.
    """
    result, rejection = download_one(
        record, FakeClient(TooLargeError("exceeded 20971520 bytes")), settings
    )

    assert result is None
    assert rejection.reason_code is RejectionReason.TOO_LARGE
    assert list(settings.paths.images_dir.rglob("*")) == [], "no partial file may remain"


def test_a_zero_byte_response_is_rejected(settings, record):
    """
    A 200 with an empty body. It hashes and stores perfectly happily — to a
    zero-byte file that fails much later, inside a training loop.
    """
    result, rejection = download_one(record, FakeClient([]), settings)

    assert result is None
    assert rejection.reason_code is RejectionReason.EMPTY_RESPONSE


def test_a_timeout_is_its_own_reason_code(settings, record):
    """
    Distinct from HTTP_ERROR on purpose: a run whose rejections are mostly
    timeouts is a network or rate-limit story, not a source-quality one, and
    the two lead to different fixes.
    """
    result, rejection = download_one(
        record, FakeClient(requests.Timeout("read timed out")), settings
    )

    assert result is None
    assert rejection.reason_code is RejectionReason.TIMEOUT


# =============================================================================
#  Neighbouring paths, so the four above are not passing by accident
# =============================================================================


def test_exhausted_rate_limiting_is_recorded_as_an_http_error(settings, record):
    """A 429 we could not outwait is still a fetch failure, with the cause kept."""
    result, rejection = download_one(
        record, FakeClient(RateLimitedError("429 from cdn.example.org")), settings
    )

    assert result is None
    assert rejection.reason_code is RejectionReason.HTTP_ERROR
    assert "rate limited" in rejection.detail


def test_a_successful_download_lands_at_its_content_address(settings, record):
    """
    The happy path, kept alongside the failures so a change that rejects
    everything cannot make this file pass.
    """
    payload = b"\xff\xd8\xff" + b"jpeg-ish bytes " * 100
    result, rejection = download_one(record, FakeClient([payload]), settings)

    assert rejection is None
    assert result.file_size_bytes == len(payload)
    assert result.storage_path.exists()
    assert result.storage_path.name.startswith(result.sha256)
    # Content-addressed: the two-character shard directory is derived from the
    # hash, not from the class or the source.
    assert result.storage_path.parent.name == result.sha256[:2]


def test_a_failed_download_leaves_no_temp_files_behind(settings, record):
    """
    The `finally` clause, pinned. A stage that leaks a `.part-` file per broken
    link fills the volume over a few hundred runs, and the leak is invisible
    until it is not.
    """
    for _ in range(5):
        download_one(record, FakeClient(requests.ConnectionError("reset")), settings)

    leftovers = list(settings.paths.images_dir.glob(".part-*"))
    assert leftovers == []
