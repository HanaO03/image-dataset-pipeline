"""
Shared fixtures.

The guiding rule: **tests never touch the network.** Every "messy data" case is
constructed locally, on purpose, so the suite is deterministic, fast, and runs
in CI without credentials. Testing against a live API would test the API, not
our code — and would fail on a Sunday for reasons nobody can reproduce.

The corrupt fixtures below are not hypothetical. Each one reproduces a failure
mode actually observed while collecting from Openverse and Commons.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from src.config import (
    DedupeSettings,
    SplitSettings,
    ValidationSettings,
)


@pytest.fixture
def validation_settings() -> ValidationSettings:
    return ValidationSettings()


@pytest.fixture
def dedupe_settings() -> DedupeSettings:
    return DedupeSettings()


@pytest.fixture
def split_settings() -> SplitSettings:
    return SplitSettings()


# =============================================================================
#  Image factories
# =============================================================================


def make_image(
    path: Path,
    size: tuple[int, int] = (320, 240),
    fmt: str = "JPEG",
    seed: int = 1,
) -> Path:
    """
    A synthetic image that behaves like a photograph under perceptual hashing.

    This detail matters more than it looks. pHash is a DCT over the *low*
    frequency components — the broad structure a human recognises — and is
    deliberately insensitive to fine detail, which is exactly why it survives
    resizing and re-compression.

    An early version of this fixture painted high-frequency pixel noise. Every
    near-duplicate test failed, because resizing destroyed the only signal the
    image had. That was the fixture being unrealistic, not the hash being
    wrong: real photographs are dominated by smooth gradients and large shapes.
    So this generator produces exactly that, and the pHash tests then measure
    what they claim to measure.
    """
    import math

    width, height = size
    image = Image.new("RGB", size)
    pixels = image.load()
    for x in range(width):
        for y in range(height):
            r = int(127 + 120 * math.sin((x / width) * 3.1 * seed + 0.5))
            g = int(127 + 120 * math.sin((y / height) * 2.2 * seed + 1.0))
            b = int(127 + 120 * math.sin(((x + y) / (width + height)) * 5.0 * seed))
            pixels[x, y] = (r, g, b)

    draw = ImageDraw.Draw(image)
    draw.ellipse(
        [width * 0.2, height * 0.2, width * 0.6, height * 0.7],
        fill=(240, 240, (20 * seed) % 255),
    )
    draw.rectangle(
        [width * 0.65, height * 0.1, width * 0.95, height * 0.5], fill=(20, 40, 200)
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path, format=fmt, quality=95) if fmt == "JPEG" else image.save(path, format=fmt)
    return path


@pytest.fixture
def good_jpeg(tmp_path: Path) -> Path:
    return make_image(tmp_path / "good.jpg")


@pytest.fixture
def truncated_jpeg(tmp_path: Path) -> Path:
    """
    A JPEG whose header is intact but whose image data stops halfway.

    This is the fixture that justifies the two-pass validation: `verify()`
    inspects structure and passes this file happily. Only decoding the pixels
    catches it. Observed in the wild whenever a CDN drops a connection
    mid-transfer.
    """
    buffer = io.BytesIO()
    image = Image.new("RGB", (400, 400), (200, 30, 30))
    image.save(buffer, format="JPEG", quality=95)
    data = buffer.getvalue()

    path = tmp_path / "truncated.jpg"
    path.write_bytes(data[: len(data) // 2])
    return path


@pytest.fixture
def png_named_jpg(tmp_path: Path) -> Path:
    """A real PNG served at a .jpg URL — routine on aggregator APIs."""
    path = tmp_path / "actually_png.jpg"
    make_image(path.with_suffix(".png"), fmt="PNG")
    path.write_bytes((path.with_suffix(".png")).read_bytes())
    return path


@pytest.fixture
def html_error_page(tmp_path: Path) -> Path:
    """A dead image link that returned 200 with an HTML error page."""
    path = tmp_path / "notfound.jpg"
    path.write_bytes(
        b"<!DOCTYPE html><html><head><title>404 Not Found</title></head>"
        b"<body><h1>Not Found</h1><p>The requested resource does not exist.</p>"
        b"</body></html>" + b" " * 1200  # pad past the min-file-size check
    )
    return path


@pytest.fixture
def tiny_image(tmp_path: Path) -> Path:
    """A tracking pixel / favicon — technically a valid image, useless to a model."""
    return make_image(tmp_path / "tiny.jpg", size=(16, 16))


@pytest.fixture
def banner_image(tmp_path: Path) -> Path:
    """Extreme aspect ratio: a page banner, not a photograph of a subject."""
    return make_image(tmp_path / "banner.jpg", size=(2000, 100))
