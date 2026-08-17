"""
Stage 2 — materialise bytes on disk.

Storage is **content-addressed**:

    /data/images/<sha256[:2]>/<sha256>.<ext>

Three properties fall out of that choice for free, which is why it is worth the
small amount of ceremony:

* **Exact deduplication is structural.** Identical bytes always resolve to the
  same path, so a duplicate costs zero extra disk and needs no lookup table.
* **Re-runs are cheap.** A URL we have downloaded before, whose file is still in
  the store, is reused without touching the network — see `run()` below for why
  the lookup has to be by URL and not by hash.
* **Writes are atomic.** Bytes stream into a temp file and are renamed into
  place only after the hash is known. A crash mid-download leaves a stray temp
  file, never a corrupt image at a valid path.

The hash is computed *during* streaming, so the file is never read twice.

Concurrency is a bounded thread pool: the work is I/O-bound, so threads are the
right tool and processes would only add overhead. The pool size interacts with
the per-host delay in HttpClient — raising workers without raising the delay is
how a pipeline earns a 429.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests

from ..config import Settings
from ..http.client import HttpClient, NotAnImageError, RateLimitedError, TooLargeError
from ..logging_setup import get_logger
from ..models import (
    DownloadedImage,
    RawRecord,
    Rejection,
    RejectionReason,
    Stage,
    StageResult,
)

log = get_logger(__name__)


def _extension_from_url(url: str) -> str:
    """
    Provisional extension, taken from the URL.

    Explicitly *not* trusted: the validate stage compares it against the format
    decoded from the actual bytes and rejects mismatches. It exists only so the
    file on disk has a plausible name before we know better.
    """
    suffix = Path(urlparse(url).path).suffix.lower()
    return suffix if 1 < len(suffix) <= 5 else ".bin"


def storage_path_for(images_dir: Path, sha256: str, extension: str) -> Path:
    """
    Content-addressed location. The two-character prefix directory keeps any
    single directory from accumulating an unreasonable number of entries — a
    non-issue at 150 images, a real one at 150,000.
    """
    return images_dir / sha256[:2] / f"{sha256}{extension}"


def download_one(
    record: RawRecord, client: HttpClient, settings: Settings
) -> tuple[DownloadedImage | None, Rejection | None]:
    """
    Fetch one image. Returns exactly one of (result, rejection).

    Never raises on a data problem — a broken link is expected, routine, and
    must not take down the run. Exceptions are reserved for the filesystem
    being unwritable, which is infrastructure and should stop everything.
    """
    images_dir = settings.paths.images_dir
    tmp_path: Path | None = None

    try:
        digest = hashlib.sha256()
        total = 0

        # Temp file in the destination directory so the final rename is atomic
        # (same filesystem). A rename across devices is a copy, and copies are
        # not atomic.
        images_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=images_dir, prefix=".part-")
        tmp_path = Path(tmp_name)

        with os.fdopen(fd, "wb") as handle:
            for chunk in client.stream_bytes(record.image_url):
                handle.write(chunk)
                digest.update(chunk)
                total += len(chunk)

        if total == 0:
            return None, Rejection.from_raw(
                record, Stage.DOWNLOAD, RejectionReason.EMPTY_RESPONSE, "zero bytes received"
            )

        sha256 = digest.hexdigest()
        destination = storage_path_for(images_dir, sha256, _extension_from_url(record.image_url))
        destination.parent.mkdir(parents=True, exist_ok=True)

        if destination.exists():
            # Same bytes already on disk from an earlier run or an earlier
            # record in this one. Discard the temp copy and reuse the original.
            tmp_path.unlink(missing_ok=True)
            tmp_path = None
        else:
            os.replace(tmp_path, destination)
            tmp_path = None

        return (
            DownloadedImage(
                raw=record,
                storage_path=destination,
                sha256=sha256,
                file_size_bytes=total,
                declared_content_type=None,
            ),
            None,
        )

    except NotAnImageError as exc:
        # The classic dead image link: 200 OK with an HTML error page.
        return None, Rejection.from_raw(
            record, Stage.DOWNLOAD, RejectionReason.NOT_AN_IMAGE, str(exc)
        )
    except TooLargeError as exc:
        return None, Rejection.from_raw(
            record, Stage.DOWNLOAD, RejectionReason.TOO_LARGE, str(exc)
        )
    except RateLimitedError as exc:
        return None, Rejection.from_raw(
            record, Stage.DOWNLOAD, RejectionReason.HTTP_ERROR, f"rate limited: {exc}"
        )
    except requests.Timeout as exc:
        return None, Rejection.from_raw(
            record, Stage.DOWNLOAD, RejectionReason.TIMEOUT, str(exc)
        )
    except requests.RequestException as exc:
        return None, Rejection.from_raw(
            record, Stage.DOWNLOAD, RejectionReason.HTTP_ERROR, str(exc)[:400]
        )
    finally:
        # Never leave partial downloads behind, on any path out of this function.
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)


def reuse_stored(
    record: RawRecord, known_urls: dict[str, tuple[str, str]]
) -> DownloadedImage | None:
    """
    Rebuild a DownloadedImage from the store, if this URL was fetched before.

    Returns None when the URL is unknown *or* when the file it names has since
    been deleted from the store — in which case the caller downloads it again.
    Checking the file rather than trusting the database row is deliberate: the
    image directory is a cache that a reviewer may legitimately delete, and a
    stale row must not turn into a missing file three stages later.
    """
    entry = known_urls.get(record.image_url)
    if entry is None:
        return None
    sha256, storage_path = entry
    path = Path(storage_path)
    try:
        size = path.stat().st_size
    except OSError:
        return None
    if size == 0:
        return None
    return DownloadedImage(
        raw=record, storage_path=path, sha256=sha256, file_size_bytes=size
    )


def run(
    records: list[RawRecord],
    client: HttpClient,
    settings: Settings,
    known_sha256: set[str] | None = None,
    known_urls: dict[str, tuple[str, str]] | None = None,
    refetch: bool = False,
) -> StageResult:
    """
    Download every candidate concurrently, bounded by `download_workers`.

    **Why the skip is keyed on the URL and not the hash.** A content hash is the
    obvious idempotency key and the wrong one here: it is only knowable *after*
    the bytes have arrived, so `known_sha256` can report that a re-run fetched
    nothing new but cannot prevent the transfer. The URL is known beforehand,
    which is what makes it the only key capable of saving the bandwidth. Hashes
    still guard correctness — the file is re-validated and re-hashed downstream,
    and `images.sha256` remains the unique key — while the URL index guards cost.

    That trades one assumption: the URL still serves the bytes it served last
    time. True for both sources here (immutable asset URLs) and false in general,
    so `refetch=True` (`--refetch` on the CLI) forces the download. The stricter
    form is a conditional GET with the stored ETag; noted in the README.
    """
    result = StageResult()
    known = known_sha256 or set()
    urls = {} if refetch else (known_urls or {})
    seen_this_run: set[str] = set()

    if not records:
        return result

    pending: list[RawRecord] = []
    reused: list[DownloadedImage] = []
    for record in records:
        cached = reuse_stored(record, urls)
        if cached is None:
            pending.append(record)
        else:
            reused.append(cached)

    if reused:
        log.info("download: reusing bytes already in the store",
                 extra={"reused": len(reused), "to_fetch": len(pending)})

    for downloaded in reused:
        result.add_metric("reused_from_store")
        if downloaded.sha256 in seen_this_run:
            continue
        seen_this_run.add(downloaded.sha256)
        result.add_metric("already_in_store")
        result.kept.append(downloaded)

    with ThreadPoolExecutor(max_workers=settings.download_workers) as pool:
        futures = {pool.submit(download_one, record, client, settings): record
                   for record in pending}
        for future in as_completed(futures):
            downloaded, rejection = future.result()
            if rejection is not None:
                result.rejections.append(rejection)
                result.add_metric("failed")
                continue

            assert downloaded is not None
            result.add_metric("downloaded")

            if downloaded.sha256 in seen_this_run:
                # Two different search results pointing at identical bytes.
                # Reported, not stored twice.
                result.rejections.append(
                    Rejection.from_raw(
                        downloaded.raw,
                        Stage.DOWNLOAD,
                        RejectionReason.EXACT_DUPLICATE,
                        f"identical bytes already fetched in this run ({downloaded.sha256[:12]})",
                    )
                )
                result.add_metric("exact_duplicates_in_run")
                continue

            seen_this_run.add(downloaded.sha256)
            if downloaded.sha256 in known:
                result.add_metric("already_in_store")
            result.kept.append(downloaded)

    log.info("download complete", extra=dict(result.metrics))
    return result
