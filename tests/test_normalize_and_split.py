"""
Licence normalisation and train/val splitting.

Both are pure functions, and both encode a policy decision that an interviewer
will ask about — so both are tested against the awkward cases, not the happy path.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

import pytest

from src.config import SplitSettings
from src.models import ImageRecord, SourceName, Split
from src.pipeline.normalize import normalise_license, requires_attribution
from src.pipeline.split import SplitCandidate, assign, split_score


# =============================================================================
#  Licence normalisation
# =============================================================================


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Openverse coded form: license + license_version joined by the adapter
        ("by-4.0", "CC-BY-4.0"),
        ("by-sa-3.0", "CC-BY-SA-3.0"),
        ("by-nc-nd-2.0", "CC-BY-NC-ND-2.0"),
        # Commons rendered form
        ("CC BY-SA 4.0", "CC-BY-SA-4.0"),
        ("CC BY 2.0", "CC-BY-2.0"),
        ("Creative Commons Attribution-Share Alike 3.0", "CC-BY-SA-3.0"),
        ("Creative Commons Attribution 4.0", "CC-BY-4.0"),
        # Public domain, in its many disguises
        ("CC0", "CC0-1.0"),
        ("cc0 1.0", "CC0-1.0"),
        ("Public domain", "PDM-1.0"),
        ("PD-self", "PDM-1.0"),
        ("public domain dedication", "PDM-1.0"),
    ],
)
def test_normalises_the_forms_both_sources_actually_produce(raw, expected):
    assert normalise_license(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "All rights reserved", "unknown", "©"])
def test_unmappable_licences_return_none_so_the_image_is_rejected(raw):
    """
    The strict policy in action: if we cannot say what the licence is, we do not
    store the image. Returning a default here would be the dangerous choice.
    """
    assert normalise_license(raw) is None


def test_element_order_is_canonical_regardless_of_input_order():
    """`nc-by` and `by-nc` are the same licence; SPDX has one spelling for it."""
    assert normalise_license("nc-by-4.0") == normalise_license("by-nc-4.0") == "CC-BY-NC-4.0"


def test_missing_version_falls_back_to_a_stated_default():
    assert normalise_license("CC BY-SA") == "CC-BY-SA-4.0"


def test_attribution_requirement_is_derived_from_the_licence():
    assert requires_attribution("CC-BY-4.0")
    assert requires_attribution("CC-BY-SA-3.0")
    assert not requires_attribution("CC0-1.0")
    assert not requires_attribution("PDM-1.0")


# =============================================================================
#  Splitting
# =============================================================================


def _candidates(n: int, class_label: str) -> list[SplitCandidate]:
    import hashlib

    return [
        SplitCandidate(
            sha256=hashlib.sha256(f"{class_label}-{i}".encode()).hexdigest(),
            class_label=class_label,
        )
        for i in range(n)
    ]


def test_score_is_deterministic_and_in_range():
    score = split_score("a" * 64, "salt")
    assert score == split_score("a" * 64, "salt")
    assert 0.0 <= score < 1.0


def test_changing_the_salt_changes_the_partition():
    """The documented escape hatch for 'we need a fresh split'."""
    assert split_score("a" * 64, "v1") != split_score("a" * 64, "v2")


def test_split_is_exactly_stratified_within_every_class(split_settings):
    candidates = _candidates(60, "cat") + _candidates(40, "dog") + _candidates(50, "bird")
    assignment = assign(candidates, split_settings)

    per_class: dict[str, Counter] = {}
    for candidate in candidates:
        per_class.setdefault(candidate.class_label, Counter())[
            assignment[candidate.sha256].value
        ] += 1

    assert per_class["cat"] == Counter({"train": 48, "val": 12})   # 80/20 of 60
    assert per_class["dog"] == Counter({"train": 32, "val": 8})    # 80/20 of 40
    assert per_class["bird"] == Counter({"train": 40, "val": 10})  # 80/20 of 50


def test_split_is_reproducible_across_runs_and_input_orderings(split_settings):
    """
    The reproducibility claim, made checkable.

    Shuffling the input must not change a single assignment — there is no
    ordering dependency and no seed involved.
    """
    import random

    candidates = _candidates(50, "cat")
    shuffled = candidates[:]
    random.Random(7).shuffle(shuffled)

    assert assign(candidates, split_settings) == assign(shuffled, split_settings)


def test_no_image_ends_up_in_both_splits(split_settings):
    candidates = _candidates(60, "cat")
    assignment = assign(candidates, split_settings)
    assert len(assignment) == len(candidates)
    assert set(assignment.values()) == {Split.TRAIN, Split.VAL}


def test_stable_bucket_strategy_never_moves_an_image_as_the_dataset_grows():
    """
    The trade-off the two strategies exist to express.

    Under `stable_bucket`, an image's split is a property of the image alone,
    so growing the dataset cannot reassign anything. That is the property you
    want once a model has already been trained against a split.
    """
    settings = SplitSettings(strategy="stable_bucket")
    small = _candidates(30, "cat")
    grown = small + _candidates(30, "dog")[:0] + _candidates(60, "cat")[30:]

    before = assign(small, settings)
    after = assign(grown, settings)

    assert all(after[c.sha256] == before[c.sha256] for c in small)


def test_tiny_class_still_gets_a_validation_image(split_settings):
    """
    A class of 3 at 80% rounds to 2.4 -> 2 train, 1 val. The guard matters at
    the extreme: an empty val set silently disables evaluation for that class,
    which is worse than being one image off the target ratio.
    """
    assignment = assign(_candidates(3, "rare"), split_settings)
    counts = Counter(s.value for s in assignment.values())
    assert counts["val"] >= 1
    assert counts["train"] >= 1


def test_single_image_class_does_not_crash(split_settings):
    assignment = assign(_candidates(1, "solo"), split_settings)
    assert len(assignment) == 1


def test_duplicates_are_never_assigned_a_split(split_settings):
    """
    The leakage guard. A near-duplicate that reached the splitter would put the
    same photograph on both sides of the boundary and inflate validation
    accuracy — the exact failure this pipeline is built to prevent.
    """
    from src.pipeline.split import assign_to_records

    def record(sha: str, duplicate: bool) -> ImageRecord:
        base = ImageRecord(
            sha256=sha,
            class_label="cat",
            source=SourceName.OPENVERSE,
            source_id=sha,
            origin_url="http://example.org/x.jpg",
            license="CC-BY-4.0",
            storage_path="/data/images/xx/x.jpg",
            image_format="JPEG",
            width=100,
            height=100,
            file_size_bytes=1000,
            retrieved_at=datetime.now(timezone.utc),
        )
        return base.marked_duplicate_of("z" * 64, 2) if duplicate else base

    records = [record("a" * 64, False), record("b" * 64, True), record("c" * 64, False)]
    out = assign_to_records(records, split_settings)

    by_sha = {r.sha256: r for r in out}
    assert by_sha["b" * 64].split is None, "a duplicate must never receive a split"
    assert by_sha["a" * 64].split is not None
    assert by_sha["c" * 64].split is not None
