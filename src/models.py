"""
Data contracts for the pipeline.

Design note — why frozen dataclasses and not Pydantic models:
Pydantic earns its keep at trust boundaries, where untyped external data enters
the system. Here that boundary is exactly one place: parsing an upstream payload
into a RawRecord, which the source adapters do explicitly and defensively (they
must, since upstream fields are frequently missing). Everything downstream is
data we produced ourselves, so runtime re-validation on every stage buys nothing
and costs clarity. `frozen=True` gives the property that actually matters here:
stages cannot mutate their inputs, so the pipeline stays a chain of pure
transformations and each stage is trivially unit-testable.

The record types mirror the pipeline stages:

    RawRecord  --download-->  DownloadedImage  --validate-->  ValidatedImage
                                                                    |
                                              normalize + dedupe + split
                                                                    v
                                                              ImageRecord
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any


# =============================================================================
#  Closed vocabularies
# =============================================================================


class SourceName(StrEnum):
    """Must stay in sync with the CHECK constraint on images.source."""

    OPENVERSE = "openverse"
    WIKIMEDIA_COMMONS = "wikimedia_commons"


class Stage(StrEnum):
    """Pipeline stages. Mirrors the CHECK constraint on rejections.stage."""

    INGEST = "ingest"
    DOWNLOAD = "download"
    VALIDATE = "validate"
    DEDUPE = "dedupe"
    NORMALIZE = "normalize"
    SELECT = "select"
    SPLIT = "split"
    EXPORT = "export"


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    #: The run completed, but a quality gate failed (e.g. a class came back
    #: under the configured minimum). Deliberately distinct from FAILED:
    #: silently under-delivering a class is the failure mode that actually
    #: damages a downstream model, so it must be visible, not swallowed.
    PARTIAL = "partial"
    FAILED = "failed"


class Split(StrEnum):
    TRAIN = "train"
    VAL = "val"


class RejectionReason(StrEnum):
    """
    Closed vocabulary of *why* an item was discarded.

    This is the pipeline's diagnostic language. Rejection counts grouped by
    reason_code, compared run over run, are how you notice that a source
    changed its HTML or started serving WebP — which a free-text log message
    would never surface.
    """

    # -- ingest ---------------------------------------------------------------
    MISSING_IMAGE_URL = "MISSING_IMAGE_URL"
    MISSING_LICENSE = "MISSING_LICENSE"          # strict licence policy
    UNRECOGNISED_LICENSE = "UNRECOGNISED_LICENSE"
    PARSE_ERROR = "PARSE_ERROR"                   # scraper could not read the item

    # -- download -------------------------------------------------------------
    HTTP_ERROR = "HTTP_ERROR"
    TIMEOUT = "TIMEOUT"
    TOO_LARGE = "TOO_LARGE"                       # exceeded max_download_bytes
    EMPTY_RESPONSE = "EMPTY_RESPONSE"
    NOT_AN_IMAGE = "NOT_AN_IMAGE"                 # Content-Type says text/html etc.

    # -- validate -------------------------------------------------------------
    UNREADABLE_IMAGE = "UNREADABLE_IMAGE"         # Pillow cannot decode it
    TRUNCATED_IMAGE = "TRUNCATED_IMAGE"           # decodes partially then fails
    EXTENSION_MISMATCH = "EXTENSION_MISMATCH"     # .jpg that is really a PNG/HTML
    UNSUPPORTED_FORMAT = "UNSUPPORTED_FORMAT"
    DIMENSIONS_TOO_SMALL = "DIMENSIONS_TOO_SMALL"
    DIMENSIONS_TOO_LARGE = "DIMENSIONS_TOO_LARGE"
    ASPECT_RATIO_EXTREME = "ASPECT_RATIO_EXTREME"  # banners/spritesheets, not subjects
    FILESIZE_TOO_SMALL = "FILESIZE_TOO_SMALL"
    IMAGE_BOMB = "IMAGE_BOMB"                     # decompression bomb guard

    # -- dedupe ---------------------------------------------------------------
    EXACT_DUPLICATE = "EXACT_DUPLICATE"           # identical sha256
    NEAR_DUPLICATE = "NEAR_DUPLICATE"             # phash within threshold

    # -- select ---------------------------------------------------------------
    #: Not a defect. The image was valid, licensed and unique — its class had
    #: simply already reached the target size. Recorded rather than dropped
    #: silently, so an over-fetch is visible and reversible by raising the target.
    OVER_TARGET = "OVER_TARGET"


# =============================================================================
#  Records
# =============================================================================


def _utcnow() -> datetime:
    """Always timezone-aware UTC. Naive datetimes are how timezone bugs start."""
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class RawRecord:
    """
    One candidate image as reported by a source, before we have any bytes.

    Every field except `source`, `class_label` and `image_url` is optional
    on purpose: real sources omit things. The pipeline decides what to do
    about missing values; the record type does not pretend they are present.
    """

    source: SourceName
    class_label: str
    image_url: str
    #: Upstream's stable identifier. Falls back to the image URL for scraped
    #: pages, which frequently have no id of their own.
    source_id: str
    landing_url: str | None = None
    license_raw: str | None = None
    license_url: str | None = None
    attribution: str | None = None
    title: str | None = None
    #: The untouched upstream payload, persisted to raw_records.payload.
    #: This is what makes re-normalisation possible without re-fetching.
    payload: dict[str, Any] = field(default_factory=dict)
    fetched_at: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True, slots=True)
class DownloadedImage:
    """A RawRecord whose bytes are now on disk, at a content-addressed path."""

    raw: RawRecord
    storage_path: Path
    #: sha256 of the bytes, computed during streaming — single pass, no re-read.
    sha256: str
    file_size_bytes: int
    #: As reported by the server. Advisory only — we verify the real format
    #: from the bytes at validation, never from this header or the URL.
    declared_content_type: str | None = None


@dataclass(frozen=True, slots=True)
class ValidatedImage:
    """A DownloadedImage that decoded cleanly and passed every sanity check."""

    downloaded: DownloadedImage
    #: Pillow's detected format ("JPEG", "PNG", ...) — measured, not declared.
    image_format: str
    width: int
    height: int
    #: 64-bit perceptual hash, 16 hex chars. None only if hashing is disabled.
    phash: str | None = None

    @property
    def sha256(self) -> str:
        return self.downloaded.sha256

    @property
    def raw(self) -> RawRecord:
        return self.downloaded.raw


@dataclass(frozen=True, slots=True)
class ImageRecord:
    """
    The curated row: normalised, deduplicated, split-assigned.
    Maps 1:1 onto the `images` table.
    """

    sha256: str
    class_label: str
    source: SourceName
    source_id: str
    origin_url: str
    #: Normalised to an SPDX-like identifier (e.g. "CC-BY-4.0").
    #: Never None — the strict licence policy rejects unlicensed images
    #: before they reach this type, and the DB column is NOT NULL to match.
    license: str
    storage_path: str
    image_format: str
    width: int
    height: int
    file_size_bytes: int
    retrieved_at: datetime
    phash: str | None = None
    source_url: str | None = None
    license_url: str | None = None
    attribution: str | None = None
    split: Split | None = None
    #: Set when this image is a near-duplicate of an already-kept image.
    #: Marked rather than deleted, so the audit trail survives.
    duplicate_of_sha256: str | None = None
    duplicate_distance: int | None = None

    @property
    def is_duplicate(self) -> bool:
        return self.duplicate_of_sha256 is not None

    def with_split(self, split: Split) -> "ImageRecord":
        return replace(self, split=split)

    def marked_duplicate_of(self, sha256: str, distance: int) -> "ImageRecord":
        return replace(self, duplicate_of_sha256=sha256, duplicate_distance=distance)


@dataclass(frozen=True, slots=True)
class Rejection:
    """A discarded item and the machine-readable reason for discarding it."""

    stage: Stage
    reason_code: RejectionReason
    source: SourceName
    class_label: str | None = None
    source_id: str | None = None
    source_url: str | None = None
    #: Free text for the human debugging it later: the exception message,
    #: the offending dimensions, the actual Content-Type received.
    detail: str | None = None
    occurred_at: datetime = field(default_factory=_utcnow)

    @classmethod
    def from_raw(
        cls,
        raw: RawRecord,
        stage: Stage,
        reason: RejectionReason,
        detail: str | None = None,
    ) -> "Rejection":
        """Build a rejection from the record that failed, keeping its provenance."""
        return cls(
            stage=stage,
            reason_code=reason,
            source=raw.source,
            class_label=raw.class_label,
            source_id=raw.source_id,
            source_url=raw.landing_url or raw.image_url,
            detail=detail,
        )


@dataclass(slots=True)
class StageResult:
    """
    The uniform contract every pipeline stage returns.

    Stages never raise on bad *data* — they return what survived plus a list of
    rejections. Exceptions are reserved for bad *infrastructure* (no database,
    unwritable volume, invalid config), which should stop the run immediately.
    That is the fail-soft / fail-fast boundary, expressed in a type.
    """

    kept: list[Any] = field(default_factory=list)
    rejections: list[Rejection] = field(default_factory=list)
    metrics: dict[str, int] = field(default_factory=dict)
    #: Stage-specific side outputs that are neither "kept" nor "rejected".
    #: Currently only dedupe uses it, to report images already in the database
    #: that a better copy has superseded. Keeping this generic avoids giving
    #: every stage a bespoke return type for one stage's needs.
    extras: dict[str, Any] = field(default_factory=dict)

    def add_metric(self, name: str, value: int = 1) -> None:
        self.metrics[name] = self.metrics.get(name, 0) + value
