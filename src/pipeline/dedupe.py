"""
Stage 4 — deduplication, at two levels.

**Exact duplicates** are not handled here at all. They are handled by the
`UNIQUE` constraint on `images.sha256` plus content-addressed storage, so the
guarantee holds even across concurrent workers, crashes and re-runs. Enforcing
it in Python instead would be a guarantee only as good as our control flow.

**Near duplicates** are what this module is for: the same photograph re-encoded,
resized, cropped or watermarked. Their bytes differ, so sha256 sees two distinct
images, but a model sees one — and if the two land on opposite sides of the
train/val boundary, validation accuracy becomes a lie. This is the single most
important correctness issue in a small image dataset, which is why dedup runs
*before* splitting rather than after.

Method: 64-bit perceptual hash (DCT-based pHash), compared by Hamming distance.

    distance   meaning
    --------   -------------------------------------------------------
    0          identical after re-encoding (resize, JPEG quality change)
    1-5        same photograph: crop, watermark, colour grade   <-- rejected
    6-10       visually similar, frequently genuinely different
    >10        unrelated

The threshold is configurable and defaults to 5. Anything in the 6-10 band is
where false positives start — two photographs of the same species in the same
pose can land there — and discarding real data is worse than keeping a rare
near-miss.

Complexity is O(n²) within a run plus O(n·m) against stored images. That is
correct and fast at this volume. At ~10⁵+ images the answer is a BK-tree over
the Hamming metric, or storing the hash as a bit vector in pgvector with an
approximate index; see the note in schema.sql.
"""

from __future__ import annotations

from typing import Any, Iterable

import imagehash
from PIL import Image

from ..config import DedupeSettings
from ..logging_setup import get_logger
from ..models import (
    Rejection,
    RejectionReason,
    Stage,
    StageResult,
    ValidatedImage,
)

log = get_logger(__name__)


def compute_phash(path: str, hash_size: int = 8) -> str:
    """
    64-bit perceptual hash as 16 hex characters (hash_size=8 -> 8x8 bits).

    Converted to RGB first for mode compatibility, not for hash stability:
    pHash reduces to luminance internally, so greyscale and colour versions of
    one photograph already agree. The conversion is there because palette (P),
    transparency (RGBA/LA) and CMYK images are common on Commons and would
    otherwise raise or hash from the wrong channel data.
    """
    with Image.open(path) as img:
        return str(imagehash.phash(img.convert("RGB"), hash_size=hash_size))


def hamming_distance(hex_a: str, hex_b: str) -> int:
    """
    Bit distance between two hex-encoded hashes.

    Implemented directly rather than via imagehash's own subtraction so that
    it works on the hex strings we read back from Postgres, with no need to
    reconstruct hash objects. Mismatched lengths return the maximum distance
    rather than raising — a malformed stored hash should not stop a run.
    """
    if len(hex_a) != len(hex_b):
        return len(hex_a) * 4
    return bin(int(hex_a, 16) ^ int(hex_b, 16)).count("1")


def _prefer(candidate: dict[str, Any], incumbent: dict[str, Any], strategy: str) -> bool:
    """
    Should `candidate` replace `incumbent` as the kept image?

    Ties always break on first-seen, which keeps the outcome deterministic
    across re-runs — otherwise dict ordering would decide, and the dataset
    would quietly change between identical runs.
    """
    if strategy == "first_seen":
        return False
    cand_pixels = candidate["width"] * candidate["height"]
    inc_pixels = incumbent["width"] * incumbent["height"]
    return cand_pixels > inc_pixels


def run(
    images: list[ValidatedImage],
    existing: Iterable[dict[str, Any]],
    settings: DedupeSettings,
) -> StageResult:
    """
    Attach a pHash to every image and mark near-duplicates.

    `existing` is the pHash index of already-stored, non-duplicate images, so a
    later run cannot re-introduce a near-duplicate of something collected last
    week. Comparing only within the current batch would make dedup a function
    of batch boundaries, which is not a property anyone wants.

    Kept images come back as `(ValidatedImage, duplicate_of_sha256 | None,
    distance | None)`, so the caller can persist the marking. Near-duplicates
    are marked rather than dropped — the row stays queryable and the audit
    trail survives.
    """
    result = StageResult()

    if not settings.enable_perceptual:
        result.kept = [(img, None, None) for img in images]
        return result

    # Mutable working index: starts as what the database already holds and
    # grows as this run accepts images, so duplicates within the batch are
    # caught by the same code path as duplicates against history.
    index: list[dict[str, Any]] = [
        {
            "sha256": row["sha256"],
            "phash": row["phash"],
            "class_label": row["class_label"],
            "width": row["width"],
            "height": row["height"],
        }
        for row in existing
        if row.get("phash")
    ]
    prior_count = len(index)
    #: Stored images that an incoming higher-resolution copy has superseded.
    #: (old_sha256, new_sha256, distance) — the caller marks the old row.
    supersessions: list[tuple[str, str, int]] = []

    # Processing order is load-bearing, in two ways:
    #
    #   * highest resolution first — so within a near-duplicate group the best
    #     copy is always the incumbent and every later copy is simply marked.
    #     Without this, a thumbnail processed first would claim the slot and
    #     the full-size image would have to displace it, leaving the thumbnail
    #     silently unmarked.
    #   * sha256 as the tie-breaker — so the order is total and deterministic.
    #     Otherwise thread completion order from the download stage decides
    #     which copy survives, and two identical runs produce different datasets.
    for item in sorted(images, key=lambda i: (-(i.width * i.height), i.sha256)):
        try:
            phash = compute_phash(str(item.downloaded.storage_path))
        except Exception as exc:
            # Hashing failure is not fatal: keep the image, skip near-dup
            # checking for it. Losing a usable image over a hash is a worse
            # outcome than a missed duplicate.
            log.warning("phash failed, keeping image unhashed",
                        extra={"sha256": item.sha256[:12], "error": str(exc)})
            result.kept.append((item, None, None))
            result.add_metric("phash_failed")
            continue

        candidate = {
            "sha256": item.sha256,
            "phash": phash,
            "class_label": item.raw.class_label,
            "width": item.width,
            "height": item.height,
        }

        match, distance = _find_near_duplicate(candidate, index, settings)

        if match is None:
            index.append(candidate)
            result.kept.append((_with_phash(item, phash), None, None))
            result.add_metric("unique")
            continue

        if _prefer(candidate, match, settings.keep_strategy):
            # The incumbent is a stored image of lower resolution — incoming
            # images are processed largest-first, so this can only happen
            # against the database, never within the batch. Keep the newcomer
            # and hand the caller an instruction to demote the stored row.
            # Emitting an instruction rather than mutating here keeps this
            # stage free of database access.
            index[index.index(match)] = candidate
            supersessions.append((match["sha256"], item.sha256, distance))
            result.kept.append((_with_phash(item, phash), None, None))
            result.add_metric("unique")
            result.add_metric("superseded_existing")
            log.info("near-duplicate supersedes stored image",
                     extra={"kept": item.sha256[:12], "superseded": match["sha256"][:12],
                            "distance": distance})
            continue

        result.kept.append((_with_phash(item, phash), match["sha256"], distance))
        result.rejections.append(
            Rejection.from_raw(
                item.raw,
                Stage.DEDUPE,
                RejectionReason.NEAR_DUPLICATE,
                f"pHash distance {distance} from {match['sha256'][:12]} "
                f"(threshold {settings.max_hamming_distance})",
            )
        )
        result.add_metric("near_duplicates")

    result.extras["supersessions"] = supersessions
    log.info(
        "dedupe complete",
        extra={**result.metrics, "index_size_before": prior_count, "index_size_after": len(index)},
    )
    return result


def _find_near_duplicate(
    candidate: dict[str, Any], index: list[dict[str, Any]], settings: DedupeSettings
) -> tuple[dict[str, Any] | None, int | None]:
    """
    Closest indexed image within the threshold, or (None, None).

    Compares across *all* classes, not just the candidate's own. A photograph
    filed under both "dog" and "bird" by two different sources is a labelling
    conflict, and keeping both copies would inject contradictory labels into
    the training set.
    """
    best: dict[str, Any] | None = None
    best_distance: int | None = None
    for other in index:
        # An image is not its own near-duplicate.
        #
        # This guard is what makes re-runs safe. On a second run the index is
        # seeded from the database, which already contains every image we are
        # about to re-process; without the check each one matches itself at
        # distance 0 and gets marked as a duplicate of itself. The database
        # rejects that outright (ck_images_not_self_duplicate), so the whole
        # run dies — a failure that only ever appears on the *second* run and
        # is therefore invisible to any test that runs the pipeline once.
        if other["sha256"] == candidate["sha256"]:
            continue
        distance = hamming_distance(candidate["phash"], other["phash"])
        if distance <= settings.max_hamming_distance:
            if best_distance is None or distance < best_distance:
                best, best_distance = other, distance
                if distance == 0:
                    break  # cannot do better
    return best, best_distance


def _with_phash(item: ValidatedImage, phash: str) -> ValidatedImage:
    from dataclasses import replace

    return replace(item, phash=phash)
