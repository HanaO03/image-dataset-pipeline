"""
Deduplication tests.

The property that matters: a re-encoded, resized or slightly re-compressed copy
of a photograph must be recognised as the same photograph, while two genuinely
different photographs must not be collapsed into one. Getting the first wrong
poisons the train/val split; getting the second wrong silently deletes data.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image

from src.pipeline.dedupe import compute_phash, hamming_distance
from tests.conftest import make_image


# =============================================================================
#  Hamming distance
# =============================================================================


def test_distance_of_identical_hashes_is_zero():
    assert hamming_distance("ffee001122334455", "ffee001122334455") == 0


def test_distance_counts_differing_bits():
    # 0x0 -> 0000, 0xF -> 1111 : four differing bits.
    assert hamming_distance("000000000000000f", "0000000000000000") == 4


def test_mismatched_lengths_return_max_distance_instead_of_raising():
    """A malformed stored hash must not be able to abort a run."""
    assert hamming_distance("ffff", "ffffffffffffffff") == 16


# =============================================================================
#  Perceptual hashing
# =============================================================================


def test_phash_is_stable_for_the_same_bytes(good_jpeg):
    assert compute_phash(str(good_jpeg)) == compute_phash(str(good_jpeg))


def test_recompressed_copy_is_a_near_duplicate(tmp_path, dedupe_settings):
    """
    Same photograph, saved again at much lower JPEG quality. Different bytes,
    therefore a different sha256 — exact dedup cannot see it. pHash must.
    """
    original = make_image(tmp_path / "original.jpg", size=(400, 300))
    recompressed = tmp_path / "recompressed.jpg"
    with Image.open(original) as img:
        img.save(recompressed, format="JPEG", quality=30)

    assert original.read_bytes() != recompressed.read_bytes()

    distance = hamming_distance(
        compute_phash(str(original)), compute_phash(str(recompressed))
    )
    assert distance <= dedupe_settings.max_hamming_distance


def test_resized_copy_is_a_near_duplicate(tmp_path, dedupe_settings):
    """Thumbnails and full-size versions of one image are one image."""
    original = make_image(tmp_path / "original.jpg", size=(600, 450))
    resized = tmp_path / "resized.jpg"
    with Image.open(original) as img:
        img.resize((300, 225)).save(resized, format="JPEG", quality=90)

    distance = hamming_distance(
        compute_phash(str(original)), compute_phash(str(resized))
    )
    assert distance <= dedupe_settings.max_hamming_distance


def test_greyscale_conversion_does_not_change_identity(tmp_path, dedupe_settings):
    """
    A greyscale copy is the same photograph.

    pHash reduces to luminance internally, so this holds by construction — the
    test exists to pin the behaviour, since a future change to the hashing step
    (a different algorithm, or hashing colour channels separately) would break
    it silently and start admitting duplicate pairs.
    """
    original = make_image(tmp_path / "original.jpg", size=(400, 300))
    grey = tmp_path / "grey.jpg"
    with Image.open(original) as img:
        img.convert("L").save(grey, format="JPEG", quality=95)

    distance = hamming_distance(compute_phash(str(original)), compute_phash(str(grey)))
    assert distance <= dedupe_settings.max_hamming_distance


def test_different_photographs_are_not_collapsed(tmp_path, dedupe_settings):
    """
    The false-positive guard. A threshold tuned too loosely would quietly
    delete real data, and nothing downstream would ever report it.
    """
    a = make_image(tmp_path / "a.jpg", size=(400, 300), seed=1)
    b = make_image(tmp_path / "b.jpg", size=(400, 300), seed=4)

    distance = hamming_distance(compute_phash(str(a)), compute_phash(str(b)))
    assert distance > dedupe_settings.max_hamming_distance


# =============================================================================
#  Stage behaviour
# =============================================================================


def _validated(path: Path, sha: str, class_label: str = "cat"):
    from datetime import datetime, timezone

    from src.models import DownloadedImage, RawRecord, SourceName, ValidatedImage

    raw = RawRecord(
        source=SourceName.OPENVERSE,
        class_label=class_label,
        image_url=f"http://example.org/{sha}.jpg",
        source_id=sha,
        license_raw="by-4.0",
        fetched_at=datetime.now(timezone.utc),
    )
    with Image.open(path) as img:
        width, height = img.size
    return ValidatedImage(
        downloaded=DownloadedImage(
            raw=raw,
            storage_path=path,
            sha256=sha,
            file_size_bytes=path.stat().st_size,
        ),
        image_format="JPEG",
        width=width,
        height=height,
    )


def test_stage_marks_near_duplicate_and_keeps_both_rows(tmp_path, dedupe_settings):
    """
    Near-duplicates are *marked*, never dropped from the result. The audit trail
    is the whole reason the `duplicate_of` column exists.
    """
    from src.pipeline import dedupe

    original = make_image(tmp_path / "o.jpg", size=(600, 450))
    copy = tmp_path / "c.jpg"
    with Image.open(original) as img:
        img.resize((300, 225)).save(copy, format="JPEG", quality=80)

    items = [_validated(original, "a" * 64), _validated(copy, "b" * 64)]
    result = dedupe.run(items, existing=[], settings=dedupe_settings)

    assert len(result.kept) == 2, "both rows must survive; duplicates are marked"
    marked = [k for k in result.kept if k[1] is not None]
    assert len(marked) == 1
    assert result.metrics.get("near_duplicates") == 1
    assert result.rejections[0].reason_code.value == "NEAR_DUPLICATE"


def test_stage_detects_duplicates_against_previously_stored_images(
    tmp_path, dedupe_settings
):
    """
    A duplicate of something collected last week must still be caught. If dedup
    only compared within a batch, it would be a function of batch boundaries —
    which is not a property anyone wants.
    """
    from src.pipeline import dedupe

    stored_file = make_image(tmp_path / "stored.jpg", size=(600, 450))
    incoming_file = tmp_path / "incoming.jpg"
    with Image.open(stored_file) as img:
        img.resize((300, 225)).save(incoming_file, format="JPEG", quality=80)

    existing = [
        {
            "sha256": "e" * 64,
            "phash": compute_phash(str(stored_file)),
            "class_label": "cat",
            "width": 600,
            "height": 450,
        }
    ]
    result = dedupe.run(
        [_validated(incoming_file, "f" * 64)], existing=existing, settings=dedupe_settings
    )

    assert result.metrics.get("near_duplicates") == 1
    assert result.kept[0][1] == "e" * 64


def test_higher_resolution_copy_supersedes_the_stored_one(tmp_path, dedupe_settings):
    """keep_strategy='highest_resolution': the better image wins the slot."""
    from src.pipeline import dedupe

    small = make_image(tmp_path / "small.jpg", size=(200, 150))
    large = tmp_path / "large.jpg"
    with Image.open(small) as img:
        img.resize((800, 600)).save(large, format="JPEG", quality=95)

    existing = [
        {
            "sha256": "1" * 64,
            "phash": compute_phash(str(small)),
            "class_label": "cat",
            "width": 200,
            "height": 150,
        }
    ]
    result = dedupe.run(
        [_validated(large, "2" * 64)], existing=existing, settings=dedupe_settings
    )

    assert result.metrics.get("superseded_existing") == 1
    assert result.kept[0][1] is None, "the higher-resolution image is kept, not marked"


def test_processing_order_is_deterministic(tmp_path, dedupe_settings):
    """
    Two runs over the same images in different input order must produce the
    same outcome. Otherwise thread completion order decides which of two
    near-duplicates survives, and identical runs yield different datasets.
    """
    from src.pipeline import dedupe

    original = make_image(tmp_path / "o.jpg", size=(600, 450))
    copy = tmp_path / "c.jpg"
    with Image.open(original) as img:
        img.resize((300, 225)).save(copy, format="JPEG", quality=80)

    items = [_validated(original, "a" * 64), _validated(copy, "b" * 64)]

    forward = dedupe.run(list(items), [], dedupe_settings)
    backward = dedupe.run(list(reversed(items)), [], dedupe_settings)

    def marked(result):
        return {k[0].sha256 for k in result.kept if k[1] is not None}

    assert marked(forward) == marked(backward)


def test_an_image_is_never_its_own_near_duplicate(tmp_path, dedupe_settings):
    """
    Regression test for a bug that only appeared on the *second* run.

    On a re-run the pHash index is seeded from the database, which already
    contains every image about to be re-processed. Without an identity check
    each image matches itself at distance 0 and is marked as a duplicate of
    itself — which the DB rejects outright, killing the run. A pipeline tested
    only once would never see it.
    """
    from src.pipeline import dedupe

    image = make_image(tmp_path / "img.jpg", size=(400, 300))
    sha = "d" * 64
    existing = [
        {
            "sha256": sha,  # same image, already stored
            "phash": compute_phash(str(image)),
            "class_label": "cat",
            "width": 400,
            "height": 300,
        }
    ]
    result = dedupe.run([_validated(image, sha)], existing=existing, settings=dedupe_settings)

    assert result.kept[0][1] is None, "an image must not be a duplicate of itself"
    assert result.metrics.get("near_duplicates") is None
    assert result.metrics.get("unique") == 1
