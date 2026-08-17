"""
Stage 6 — train/val assignment.

Deliberately **not** `train_test_split(..., random_state=42)`. A seeded shuffle
is reproducible only for a fixed input set: add ten images next week and every
existing image can move between train and val. A model evaluated last week is
then evaluated on data it was trained on, and nobody notices.

Assignment is derived from each image's own content hash instead:

    score(image) = sha256(salt + image_sha256)  ->  a number in [0, 1)

Two strategies use that score, and the choice is a real trade-off:

`stratified_rank` (default)
    Within each class, rank images by score and take the lowest-scoring
    `train_percent` as train. Gives the **exact** requested ratio inside every
    class — which is what "stratified" means — and is fully deterministic for a
    given set. Cost: adding images later can shift a few near the boundary.

`stable_bucket`
    Assign purely by threshold: `score < train_percent/100 -> train`. An image's
    split then depends on *nothing but the image itself*, so it never changes,
    ever. Cost: the realised ratio only approximates the target, and with 60
    images per class it can drift by a few percentage points.

Default is `stratified_rank` because the brief explicitly asks for a stratified
split and exactness inside each class is the stated requirement. `stable_bucket`
is one config change away for a growing dataset, where permanence matters more.

Either way there is no global shuffle, no ordering dependency, and no seed to
lose. Crucially, splitting runs **after** deduplication and operates on the full
stored dataset rather than the current batch — so near-duplicates cannot land on
opposite sides of the boundary, and ratios stay exact as the dataset grows.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Iterable, NamedTuple, Sequence

from ..config import SplitSettings
from ..logging_setup import get_logger
from ..models import ImageRecord, Split

log = get_logger(__name__)


class SplitCandidate(NamedTuple):
    """The only two facts splitting needs. Keeps this stage free of I/O types."""

    sha256: str
    class_label: str


def split_score(sha256: str, salt: str) -> float:
    """
    Map an image to a stable, uniformly distributed number in [0, 1).

    Salting means a fresh split is available without abandoning content-based
    assignment: change the salt, get a completely different — still fully
    deterministic — partition.
    """
    digest = hashlib.sha256(f"{salt}:{sha256}".encode()).hexdigest()
    return int(digest[:16], 16) / float(1 << 64)


def assign(
    candidates: Iterable[SplitCandidate], settings: SplitSettings
) -> dict[str, Split]:
    """
    Core assignment. Returns {sha256: Split}.

    Pure: no database, no filesystem, no randomness. Given the same inputs it
    returns the same mapping, which is what makes the reproducibility claim
    testable rather than aspirational.
    """
    grouped: dict[str, list[SplitCandidate]] = defaultdict(list)
    for candidate in candidates:
        key = candidate.class_label if settings.stratify_by_class else "__all__"
        grouped[key].append(candidate)

    assignment: dict[str, Split] = {}
    for class_label, group in sorted(grouped.items()):
        assignment.update(_assign_group(class_label, group, settings))
    return assignment


def _assign_group(
    class_label: str, group: list[SplitCandidate], settings: SplitSettings
) -> dict[str, Split]:
    if not group:
        return {}

    # Sort by score, tie-broken by hash, so the ordering is total and stable.
    scored = sorted(group, key=lambda c: (split_score(c.sha256, settings.salt), c.sha256))

    if settings.strategy == "stable_bucket":
        threshold = settings.train_percent / 100.0
        out = {
            c.sha256: (
                Split.TRAIN if split_score(c.sha256, settings.salt) < threshold else Split.VAL
            )
            for c in scored
        }
    else:  # stratified_rank
        n_train = round(len(scored) * settings.train_percent / 100)
        # Guarantee both sides are non-empty when the class has >= 2 images.
        # A validation set of zero silently disables evaluation for that class,
        # a far worse outcome than being one image off the target ratio.
        if len(scored) >= 2:
            n_train = min(max(n_train, 1), len(scored) - 1)
        else:
            n_train = len(scored)
        out = {
            c.sha256: (Split.TRAIN if i < n_train else Split.VAL)
            for i, c in enumerate(scored)
        }

    n_train_actual = sum(1 for s in out.values() if s is Split.TRAIN)
    n_val = len(out) - n_train_actual
    log.info(
        "split assigned",
        extra={
            "class": class_label,
            "train": n_train_actual,
            "val": n_val,
            "achieved_pct": round(100 * n_train_actual / len(out), 1),
            "strategy": settings.strategy,
        },
    )
    if n_val == 0 and len(out) > 1:
        log.warning("class has no validation images", extra={"class": class_label})
    return out


def assign_to_records(
    records: Sequence[ImageRecord], settings: SplitSettings
) -> list[ImageRecord]:
    """
    Convenience wrapper for in-memory records (used by tests and dry runs).

    Duplicates are passed through untouched with `split=None`, so a near-
    duplicate can never enter either side of the dataset.
    """
    eligible = [r for r in records if not r.is_duplicate]
    assignment = assign(
        (SplitCandidate(r.sha256, r.class_label) for r in eligible), settings
    )
    return [
        r.with_split(assignment[r.sha256]) if r.sha256 in assignment else r
        for r in records
    ]
