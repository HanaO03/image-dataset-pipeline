"""
Stage 6 — hold the dataset to its target size per class.

The brief asks for roughly 40-60 images per class. The pipeline deliberately
*fetches* more than that (`overfetch_factor`), because validation, licence
mapping and deduplication all discard candidates and a run that under-delivers
is worse than one that over-fetches. Nothing, however, was previously bringing
the surviving images back down to the target — so the target was documentation
rather than behaviour, and a run delivered whatever happened to survive.

This stage closes that gap. It is the mirror image of the quality gate: the gate
catches a class that came in short, this catches a class that came in long.

**Selection must be deterministic, and it must be stable across runs.** Both
fall out of ranking candidates by the same content-derived score the splitter
uses:

* deterministic — the winners depend only on the image bytes, never on thread
  completion order or dict iteration, so two identical runs select identically;
* stable — an image already stored is never displaced by a newcomer, and the
  budget is what remains between the stored count and the target. A second run
  therefore selects the same images, adds nothing, and the class count stays
  exactly where it was rather than creeping up by one batch per run.

Trimmed images are recorded as rejections with `OVER_TARGET`, not dropped in
silence. They are perfectly good images that were not needed, and "why is this
image not in the dataset?" deserves the same answer here as everywhere else.
Near-duplicates are exempt from the budget: they are kept so they can be marked
`duplicate_of` for the audit trail, and they never count towards a class total
because every count in the schema excludes them.
"""

from __future__ import annotations

from collections import defaultdict

from ..config import SourceSettings, SplitSettings
from ..logging_setup import get_logger
from ..models import (
    ImageRecord,
    Rejection,
    RejectionReason,
    Stage,
    StageResult,
)
from .split import split_score

log = get_logger(__name__)


def run(
    records: list[ImageRecord],
    stored_counts: dict[str, int],
    stored_sha256: set[str],
    sources: SourceSettings,
    split_settings: SplitSettings,
) -> StageResult:
    """
    Keep at most `target_per_class` images per class, counting what is stored.

    `stored_counts` is the per-class count of non-duplicate images already in
    the database, and `stored_sha256` is every hash it holds. A record whose
    hash is already stored is always kept — it is a no-op upsert and is already
    inside the count — so only genuinely new images consume budget.
    """
    result = StageResult()
    target = sources.target_per_class

    duplicates: list[ImageRecord] = []
    new_by_class: dict[str, list[ImageRecord]] = defaultdict(list)
    #: Hashes that will exist in `images` once this batch is persisted. A
    #: near-duplicate may only be kept if the image it points at is in here:
    #: `duplicate_of` is a foreign key, so a marking whose target was trimmed
    #: away would be an orphan the database is right to refuse.
    surviving: set[str] = set(stored_sha256)

    for record in records:
        if record.is_duplicate:
            duplicates.append(record)
        elif record.sha256 in stored_sha256:
            # Already stored: a re-confirmation, not an addition. Inside the
            # count already, so it spends no budget.
            result.kept.append(record)
        else:
            new_by_class[record.class_label].append(record)

    for class_label, candidates in sorted(new_by_class.items()):
        budget = max(0, target - stored_counts.get(class_label, 0))
        ranked = sorted(
            candidates, key=lambda r: (split_score(r.sha256, split_settings.salt), r.sha256)
        )
        for record in ranked[:budget]:
            result.kept.append(record)
            surviving.add(record.sha256)
            result.add_metric("selected")

        for record in ranked[budget:]:
            result.rejections.append(
                Rejection(
                    stage=Stage.SELECT,
                    reason_code=RejectionReason.OVER_TARGET,
                    source=record.source,
                    class_label=class_label,
                    source_id=record.source_id,
                    source_url=record.source_url or record.origin_url,
                    detail=(
                        f"class already at target ({target}); this image was "
                        f"valid but not needed"
                    ),
                )
            )
            result.add_metric("trimmed_over_target")

        if len(ranked) > budget:
            log.info(
                "select: class trimmed to target",
                extra={
                    "class": class_label,
                    "target": target,
                    "already_stored": stored_counts.get(class_label, 0),
                    "new_candidates": len(ranked),
                    "selected": min(budget, len(ranked)),
                    "trimmed": len(ranked) - budget,
                },
            )

    # Near-duplicates last, once the survivors are known. A duplicate is worth
    # storing only for the sake of its marking, so it follows the image it
    # points at: if that image was trimmed, the marking would dangle and the
    # row would carry no information at all.
    for record in duplicates:
        if record.duplicate_of_sha256 in surviving:
            result.kept.append(record)
            result.add_metric("duplicates_retained_for_audit")
        else:
            result.rejections.append(
                Rejection(
                    stage=Stage.SELECT,
                    reason_code=RejectionReason.OVER_TARGET,
                    source=record.source,
                    class_label=record.class_label,
                    source_id=record.source_id,
                    source_url=record.source_url or record.origin_url,
                    detail=(
                        "near-duplicate of an image that was itself trimmed at "
                        "the class target; nothing left to point at"
                    ),
                )
            )
            result.add_metric("trimmed_orphan_duplicate")

    log.info("selection complete", extra=dict(result.metrics))
    return result
