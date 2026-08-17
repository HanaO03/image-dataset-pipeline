"""
Class-target selection.

This stage is the one that keeps the delivered dataset at the size the brief
asks for, and it is easy to get subtly wrong in ways that only show up on the
*second* run — a class creeping past its target, or a near-duplicate marking
left pointing at an image that was trimmed away. Both are pinned here.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest

from src.config import SourceSettings, SplitSettings
from src.models import ImageRecord, RejectionReason, SourceName
from src.pipeline import select


def make_record(seed: str, class_label: str = "cat") -> ImageRecord:
    sha = hashlib.sha256(seed.encode()).hexdigest()
    return ImageRecord(
        sha256=sha,
        class_label=class_label,
        source=SourceName.OPENVERSE,
        source_id=seed,
        origin_url=f"https://example.org/{seed}.jpg",
        license="CC-BY-4.0",
        storage_path=f"/data/images/{sha[:2]}/{sha}.jpg",
        image_format="JPEG",
        width=800,
        height=600,
        file_size_bytes=50_000,
        retrieved_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sources() -> SourceSettings:
    return SourceSettings(classes=("cat", "dog"), target_per_class=5, min_per_class=1)


@pytest.fixture
def split_settings() -> SplitSettings:
    return SplitSettings()


def run(records, sources, split_settings, stored_counts=None, stored_sha256=None):
    return select.run(
        records,
        stored_counts=stored_counts or {},
        stored_sha256=stored_sha256 or set(),
        sources=sources,
        split_settings=split_settings,
    )


def test_trims_each_class_to_its_target(sources, split_settings):
    records = [make_record(f"cat-{i}", "cat") for i in range(12)]
    records += [make_record(f"dog-{i}", "dog") for i in range(9)]

    result = run(records, sources, split_settings)

    kept = [r.class_label for r in result.kept]
    assert kept.count("cat") == 5
    assert kept.count("dog") == 5
    assert len(result.rejections) == 11
    assert {r.reason_code for r in result.rejections} == {RejectionReason.OVER_TARGET}


def test_a_class_under_target_is_untouched(sources, split_settings):
    """Selection is a ceiling, never a floor — the quality gate handles shortfall."""
    records = [make_record(f"cat-{i}", "cat") for i in range(3)]

    result = run(records, sources, split_settings)

    assert len(result.kept) == 3
    assert result.rejections == []


def test_selection_is_deterministic(sources, split_settings):
    """
    Same inputs, same winners — regardless of the order they arrive in.

    Without this the dataset would change between two identical runs, since the
    order records reach this stage is decided by thread completion in the
    download pool.
    """
    records = [make_record(f"cat-{i}") for i in range(12)]

    first = run(records, sources, split_settings)
    second = run(list(reversed(records)), sources, split_settings)

    assert [r.sha256 for r in first.kept] == [r.sha256 for r in second.kept]


def test_budget_accounts_for_what_is_already_stored(sources, split_settings):
    """A class two short takes exactly two newcomers, not a fresh five."""
    records = [make_record(f"cat-{i}") for i in range(6)]

    result = run(records, sources, split_settings, stored_counts={"cat": 3})

    assert sum(1 for _ in result.kept) == 2
    assert len(result.rejections) == 4


def test_a_full_class_admits_nothing_new(sources, split_settings):
    """The re-run case: the dataset must not creep past the target."""
    records = [make_record(f"cat-{i}") for i in range(6)]

    result = run(records, sources, split_settings, stored_counts={"cat": 5})

    assert result.kept == []
    assert len(result.rejections) == 6


def test_already_stored_images_are_kept_and_spend_no_budget(sources, split_settings):
    """
    Re-confirmations are not additions.

    On a re-run every record is already in the database. Charging them against
    the budget would trim images that are already part of the dataset, and the
    exported set would shrink a little on every run.
    """
    records = [make_record(f"cat-{i}") for i in range(5)]
    stored = {r.sha256 for r in records}

    result = run(records, sources, split_settings,
                 stored_counts={"cat": 5}, stored_sha256=stored)

    assert len(result.kept) == 5
    assert result.rejections == []


def test_near_duplicates_are_kept_when_their_original_survives(sources, split_settings):
    """Duplicates exist for the audit trail, so they never consume budget."""
    originals = [make_record(f"cat-{i}") for i in range(5)]
    duplicate = make_record("cat-dup").marked_duplicate_of(originals[0].sha256, 2)

    result = run(originals + [duplicate], sources, split_settings)

    assert duplicate.sha256 in {r.sha256 for r in result.kept}
    assert len(result.kept) == 6  # 5 against the budget + 1 marked duplicate


def test_a_duplicate_of_a_trimmed_image_is_dropped(sources, split_settings):
    """
    The orphan case, and the reason this test exists at all.

    `images.duplicate_of` is a foreign key. If the image a duplicate points at
    was itself trimmed at the class target, the marking has nothing to reference
    — the row would carry no information and the write would fail the
    `duplicate_distance` check. It has to go with its original.
    """
    records = [make_record(f"cat-{i}") for i in range(12)]
    trimmed = run(records, sources, split_settings)
    survivors = {r.sha256 for r in trimmed.kept}
    victim = next(r for r in records if r.sha256 not in survivors)

    orphan = make_record("cat-orphan").marked_duplicate_of(victim.sha256, 3)
    result = run(records + [orphan], sources, split_settings)

    assert orphan.sha256 not in {r.sha256 for r in result.kept}
    assert any(
        "trimmed" in (rejection.detail or "") for rejection in result.rejections
    )
