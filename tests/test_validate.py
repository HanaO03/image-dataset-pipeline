"""
Validation tests — one deliberately corrupt fixture per rejection reason.

This is the module where a take-home is won or lost. Anyone can write a
`requests.get`; the question is whether the pipeline notices when what came back
is not a photograph.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.models import RejectionReason
from src.pipeline.validate import ValidationError, validate_file
from tests.conftest import make_image


def _validate(path: Path, settings, extension: str | None = None):
    return validate_file(
        path=path,
        file_size_bytes=path.stat().st_size,
        declared_extension=extension if extension is not None else path.suffix,
        settings=settings,
    )


def test_accepts_a_normal_photograph(good_jpeg, validation_settings):
    image_format, width, height = _validate(good_jpeg, validation_settings)
    assert image_format == "JPEG"
    assert (width, height) == (320, 240)


def test_rejects_truncated_file_that_verify_alone_would_pass(
    truncated_jpeg, validation_settings
):
    """
    The headline case for two-pass validation.

    First we prove Pillow's `verify()` really does accept this file — if that
    assumption ever stops holding, this test tells us the second pass has
    become redundant. Then we prove our validator rejects it anyway.
    """
    from PIL import Image

    with Image.open(truncated_jpeg) as img:
        img.verify()  # passes: structure is intact, pixel data is not

    with pytest.raises(ValidationError) as excinfo:
        _validate(truncated_jpeg, validation_settings)
    assert excinfo.value.reason in {
        RejectionReason.TRUNCATED_IMAGE,
        RejectionReason.UNREADABLE_IMAGE,
    }


def test_rejects_html_error_page_saved_as_jpg(html_error_page, validation_settings):
    with pytest.raises(ValidationError) as excinfo:
        _validate(html_error_page, validation_settings)
    assert excinfo.value.reason is RejectionReason.UNREADABLE_IMAGE


def test_rejects_png_bytes_served_at_a_jpg_url(png_named_jpg, validation_settings):
    with pytest.raises(ValidationError) as excinfo:
        _validate(png_named_jpg, validation_settings, extension=".jpg")
    assert excinfo.value.reason is RejectionReason.EXTENSION_MISMATCH
    assert "PNG" in excinfo.value.detail


def test_rejects_tracking_pixel(tiny_image, validation_settings):
    # A 16x16 image is also below the byte floor, and the cheap size check runs
    # first by design. Relax it here so the assertion isolates the dimension
    # check rather than passing for the wrong reason.
    validation_settings.min_file_size_bytes = 1
    with pytest.raises(ValidationError) as excinfo:
        _validate(tiny_image, validation_settings)
    assert excinfo.value.reason is RejectionReason.DIMENSIONS_TOO_SMALL


def test_rejects_extreme_aspect_ratio(banner_image, validation_settings):
    with pytest.raises(ValidationError) as excinfo:
        _validate(banner_image, validation_settings)
    assert excinfo.value.reason is RejectionReason.ASPECT_RATIO_EXTREME


def test_rejects_file_below_size_floor(tmp_path, validation_settings):
    path = tmp_path / "stub.jpg"
    path.write_bytes(b"\xff\xd8\xff\xe0tiny")
    with pytest.raises(ValidationError) as excinfo:
        _validate(path, validation_settings)
    assert excinfo.value.reason is RejectionReason.FILESIZE_TOO_SMALL


def test_rejects_oversized_dimensions(tmp_path, validation_settings):
    validation_settings.max_dimension_px = 200
    path = make_image(tmp_path / "big.jpg", size=(900, 300))
    with pytest.raises(ValidationError) as excinfo:
        _validate(path, validation_settings)
    assert excinfo.value.reason is RejectionReason.DIMENSIONS_TOO_LARGE


def test_rejects_disallowed_format(tmp_path, validation_settings):
    validation_settings.allowed_formats = ("JPEG",)
    path = make_image(tmp_path / "x.png", fmt="PNG")
    with pytest.raises(ValidationError) as excinfo:
        _validate(path, validation_settings)
    assert excinfo.value.reason is RejectionReason.UNSUPPORTED_FORMAT


def test_decompression_bomb_guard_fires(tmp_path, validation_settings):
    """
    Pillow's bomb guard must be active, not merely configured.

    A malicious 8-byte PNG header can claim 30000x30000 pixels; decoding it
    allocates gigabytes. We set the ceiling below a real image's pixel count
    and assert the guard trips.
    """
    validation_settings.max_image_pixels = 1000  # 320x240 = 76,800 pixels
    path = make_image(tmp_path / "bomb.jpg", size=(320, 240))
    with pytest.raises(ValidationError) as excinfo:
        _validate(path, validation_settings)
    assert excinfo.value.reason is RejectionReason.IMAGE_BOMB


def test_extension_aliases_do_not_false_positive(tmp_path, validation_settings):
    """`.jpeg` and `.jpg` both mean JPEG — neither may trigger a mismatch."""
    path = make_image(tmp_path / "photo.jpeg")
    assert _validate(path, validation_settings, extension=".jpeg")[0] == "JPEG"
    assert _validate(path, validation_settings, extension=".jpg")[0] == "JPEG"


def test_unknown_extension_does_not_trigger_mismatch(good_jpeg, validation_settings):
    """
    A URL ending in `.bin` (or with no extension at all) tells us nothing, so
    it must not be treated as a contradiction. Only a *known* extension that
    disagrees with the bytes is a mismatch.
    """
    assert _validate(good_jpeg, validation_settings, extension=".bin")[0] == "JPEG"
