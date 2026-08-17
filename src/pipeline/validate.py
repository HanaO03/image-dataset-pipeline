"""
Stage 3 — is this actually a usable image?

Pure functions over a file path and a settings object: no network, no database,
no global state. That is what makes this the most heavily unit-tested module in
the project — every check below has a deliberately corrupt fixture proving it
fires.

The checks, and why each one exists:

| check                | catches                                                    |
|----------------------|------------------------------------------------------------|
| decode twice         | truncated files that `verify()` alone passes               |
| format vs extension  | `.jpg` URLs serving PNG, or an HTML error page saved as jpg |
| allowed formats      | SVG/ICO/animated formats a CV loader will choke on          |
| dimension bounds     | 1×1 tracking pixels, icons, scanned panoramas              |
| aspect ratio         | banners, spritesheets, diagrams — not a single subject      |
| file size floor      | placeholder graphics and error thumbnails                   |
| pixel-count ceiling  | decompression bombs                                         |

**The `verify()` trap.** Pillow's `Image.verify()` checks structural integrity
without decoding pixel data, so a file truncated halfway through its image data
passes it and then explodes later inside the training loop. The only reliable
test is to decode the pixels — and since `verify()` leaves the file object
unusable, that means opening the file a second time. Both steps are needed:
`verify()` catches malformed headers cheaply, `load()` catches truncated data.
"""

from __future__ import annotations

from pathlib import Path

import PIL
from PIL import Image, ImageFile

from ..config import ValidationSettings
from ..logging_setup import get_logger
from ..models import (
    DownloadedImage,
    Rejection,
    RejectionReason,
    Stage,
    StageResult,
    ValidatedImage,
)

log = get_logger(__name__)

#: Refuse to reconstruct truncated files. Pillow can be told to tolerate them,
#: which is exactly wrong here: we would rather reject a damaged image than
#: silently feed half a photograph and grey padding into a training set.
ImageFile.LOAD_TRUNCATED_IMAGES = False

#: Extensions that legitimately map to a different Pillow format name, so the
#: mismatch check does not fire on `.jpg` -> "JPEG" or `.tif` -> "TIFF".
_EXTENSION_ALIASES = {
    "jpg": "JPEG", "jpeg": "JPEG", "jpe": "JPEG", "jfif": "JPEG",
    "png": "PNG", "webp": "WEBP", "gif": "GIF", "bmp": "BMP",
    "tif": "TIFF", "tiff": "TIFF",
}


class ValidationError(Exception):
    """Carries the machine-readable reason alongside the human explanation."""

    def __init__(self, reason: RejectionReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def validate_file(
    path: Path,
    file_size_bytes: int,
    declared_extension: str,
    settings: ValidationSettings,
) -> tuple[str, int, int]:
    """
    Validate one file on disk. Returns (format, width, height) or raises
    ValidationError.

    Deliberately takes primitives rather than a DownloadedImage so it can be
    unit-tested against a fixture file with no pipeline objects involved.
    """
    Image.MAX_IMAGE_PIXELS = settings.max_image_pixels

    if file_size_bytes < settings.min_file_size_bytes:
        raise ValidationError(
            RejectionReason.FILESIZE_TOO_SMALL,
            f"{file_size_bytes} bytes < {settings.min_file_size_bytes}",
        )

    # --- pass 1: structural integrity ----------------------------------------
    try:
        with Image.open(path) as img:
            img.verify()
    except PIL.Image.DecompressionBombError as exc:
        raise ValidationError(RejectionReason.IMAGE_BOMB, str(exc)) from exc
    except Exception as exc:
        raise ValidationError(
            RejectionReason.UNREADABLE_IMAGE, f"{type(exc).__name__}: {exc}"
        ) from exc

    # --- pass 2: actually decode the pixels -----------------------------------
    # verify() invalidates the file object, so this must be a fresh open.
    # This is the pass that catches truncation.
    try:
        with Image.open(path) as img:
            image_format = (img.format or "").upper()
            width, height = img.size
            img.load()
    except PIL.Image.DecompressionBombError as exc:
        raise ValidationError(RejectionReason.IMAGE_BOMB, str(exc)) from exc
    except OSError as exc:
        # Pillow signals truncated image data as OSError from load().
        raise ValidationError(
            RejectionReason.TRUNCATED_IMAGE, f"decode failed: {exc}"
        ) from exc
    except Exception as exc:
        raise ValidationError(
            RejectionReason.UNREADABLE_IMAGE, f"{type(exc).__name__}: {exc}"
        ) from exc

    # --- format ---------------------------------------------------------------
    if image_format not in settings.allowed_formats:
        raise ValidationError(
            RejectionReason.UNSUPPORTED_FORMAT, f"format={image_format or 'unknown'}"
        )

    expected = _EXTENSION_ALIASES.get(declared_extension.lstrip(".").lower())
    if expected is not None and expected != image_format:
        # Not fatal to a CV loader on its own, but it is a strong signal that
        # the source is unreliable, and it breaks any consumer that trusts the
        # file name. Reject and count it — the rate is a source-health metric.
        raise ValidationError(
            RejectionReason.EXTENSION_MISMATCH,
            f"url said .{declared_extension.lstrip('.')}, bytes are {image_format}",
        )

    # --- geometry -------------------------------------------------------------
    smallest, largest = min(width, height), max(width, height)
    if smallest < settings.min_dimension_px:
        raise ValidationError(
            RejectionReason.DIMENSIONS_TOO_SMALL,
            f"{width}x{height} < {settings.min_dimension_px}px",
        )
    if largest > settings.max_dimension_px:
        raise ValidationError(
            RejectionReason.DIMENSIONS_TOO_LARGE,
            f"{width}x{height} > {settings.max_dimension_px}px",
        )
    if smallest > 0 and (largest / smallest) > settings.max_aspect_ratio:
        raise ValidationError(
            RejectionReason.ASPECT_RATIO_EXTREME,
            f"{width}x{height} ratio {largest / smallest:.1f} > {settings.max_aspect_ratio}",
        )

    return image_format, width, height


def run(
    downloads: list[DownloadedImage], settings: ValidationSettings
) -> StageResult:
    result = StageResult()
    for item in downloads:
        extension = item.storage_path.suffix
        try:
            image_format, width, height = validate_file(
                item.storage_path, item.file_size_bytes, extension, settings
            )
        except ValidationError as exc:
            result.rejections.append(
                Rejection.from_raw(item.raw, Stage.VALIDATE, exc.reason, exc.detail)
            )
            result.add_metric(f"rejected_{exc.reason.value.lower()}")
            result.add_metric("rejected")
            continue

        result.kept.append(
            ValidatedImage(
                downloaded=item, image_format=image_format, width=width, height=height
            )
        )
        result.add_metric("validated")

    log.info("validation complete", extra=dict(result.metrics))
    return result
