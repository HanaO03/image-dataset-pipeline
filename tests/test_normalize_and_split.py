"""
Licence normalisation and train/val splitting.

Both are pure functions, and both encode a policy decision that an interviewer
will ask about — so both are tested against the awkward cases, not the happy path.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

import pytest

from pathlib import Path

from src.config import LicenseSettings, SplitSettings
from src.models import (
    DownloadedImage,
    ImageRecord,
    RawRecord,
    RejectionReason,
    SourceName,
    Split,
    ValidatedImage,
)
from src.pipeline import normalize
from src.pipeline.normalize import (
    license_elements,
    license_permits,
    normalise_license,
    requires_attribution,
)
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


@pytest.mark.parametrize(
    "raw,expected",
    [
        # CC renamed this element between generations. 2.0 and 3.0 say
        # "NoDerivs"; only 4.0 says "NoDerivatives". Matching the longer stem
        # dropped ND from every 2.0/3.0 licence and relabelled it CC-BY.
        ("Creative Commons Attribution-NoDerivs 3.0", "CC-BY-ND-3.0"),
        ("Creative Commons Attribution-NoDerivs 2.0 Generic", "CC-BY-ND-2.0"),
        ("Creative Commons Attribution-NoDerivatives 4.0", "CC-BY-ND-4.0"),
        ("Creative Commons Attribution-No Derivative Works 3.0", "CC-BY-ND-3.0"),
        # And the compound case: matching only the first whole-name pattern
        # meant NonCommercial won and ND was discarded silently.
        ("Creative Commons Attribution-NonCommercial-NoDerivs 2.0", "CC-BY-NC-ND-2.0"),
        ("Creative Commons Attribution-NonCommercial-ShareAlike 3.0", "CC-BY-NC-SA-3.0"),
    ],
)
def test_every_element_in_a_long_form_name_survives(raw, expected):
    """
    The regression that mattered most: a restricted licence silently becoming a
    permissive one. `Attribution-NoDerivs 3.0` normalised to `CC-BY-3.0`, which
    is not a relabelling the NC/ND gate downstream can catch — by the time it
    runs, the ND is gone.
    """
    assert normalise_license(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "Creative Commons Attribution-NoDerivs 3.0",
        "Creative Commons Attribution-NonCommercial-NoDerivs 2.0",
    ],
)
def test_long_form_nd_licences_are_refused_by_the_gate(raw):
    """The two halves joined up: parsed correctly, therefore rejected."""
    assert not license_permits(normalise_license(raw), ("NC", "ND"))


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
#  Licence policy
# =============================================================================
#
#  These exist because the policy was, for a while, only half enforced. The
#  Openverse query carried `license_type=commercial,modification`, which is a
#  filter on the API and on nothing else — so an NC or ND licence read off a
#  Commons file page normalised cleanly and was stored, in a dataset the
#  documentation described as usable for commercial training. The filter was
#  real. Its scope was not what the prose claimed.
#
#  The gate below is source-blind by construction: it runs after both sources
#  have converged on one SPDX identifier, so there is no path around it.


@pytest.mark.parametrize(
    "spdx,expected",
    [
        ("CC-BY-4.0", {"BY"}),
        ("CC-BY-SA-2.0", {"BY", "SA"}),
        ("CC-BY-NC-2.0", {"BY", "NC"}),
        ("CC-BY-NC-ND-2.0", {"BY", "NC", "ND"}),
        ("CC-BY-NC-SA-4.0", {"BY", "NC", "SA"}),
        # Public domain restricts nothing, and must not be read as if it did.
        ("CC0-1.0", set()),
        ("PDM-1.0", set()),
    ],
)
def test_elements_are_read_off_the_spdx_identifier(spdx, expected):
    assert license_elements(spdx) == frozenset(expected)


@pytest.mark.parametrize(
    "spdx",
    ["CC-BY-4.0", "CC-BY-SA-2.0", "CC-BY-3.0", "CC0-1.0", "PDM-1.0"],
)
def test_licences_that_permit_training_are_admitted(spdx):
    assert license_permits(spdx, ("NC", "ND"))


@pytest.mark.parametrize(
    "spdx",
    [
        "CC-BY-NC-2.0",       # no commercial use
        "CC-BY-ND-2.0",       # no derivative works — and a model is arguably one
        "CC-BY-NC-ND-2.0",
        "CC-BY-NC-SA-4.0",
    ],
)
def test_nc_and_nd_licences_are_refused(spdx):
    assert not license_permits(spdx, ("NC", "ND"))


def test_the_policy_is_configurable_for_a_dataset_that_will_never_ship():
    """Research use may legitimately admit NC. ND is a separate decision."""
    assert license_permits("CC-BY-NC-2.0", ("ND",))
    assert not license_permits("CC-BY-ND-2.0", ("ND",))
    assert license_permits("CC-BY-NC-ND-2.0", ())


def _validated(license_raw: str, sha: str) -> ValidatedImage:
    """A minimal ValidatedImage carrying one licence. No files, no network."""
    raw = RawRecord(
        source=SourceName.WIKIMEDIA_COMMONS,
        class_label="cat",
        image_url=f"https://upload.wikimedia.org/{sha[:6]}.jpg",
        source_id=sha[:6],
        landing_url="https://commons.wikimedia.org/wiki/File:X.jpg",
        license_raw=license_raw,
        attribution="A Photographer",
    )
    return ValidatedImage(
        downloaded=DownloadedImage(
            raw=raw,
            storage_path=Path(f"/data/images/{sha[:2]}/{sha}.jpg"),
            sha256=sha,
            file_size_bytes=4096,
        ),
        image_format="JPEG",
        width=800,
        height=600,
        phash="0" * 16,
    )


def test_a_scraped_nc_image_is_rejected_with_its_own_reason_code():
    """
    The regression this gate exists for, at the stage that now catches it.

    Commons is scraped, not queried, so nothing upstream of normalisation can
    filter it. An NC licence arrives as perfectly valid free text, maps to a
    perfectly valid SPDX identifier, and must still not enter the dataset.
    """
    result = normalize.run(
        [
            (_validated("CC BY-NC-SA 2.0", "a" * 64), None, None),
            (_validated("CC BY-SA 2.0", "b" * 64), None, None),
        ],
        LicenseSettings(),
    )

    assert [r.sha256 for r in result.kept] == ["b" * 64]
    assert [r.reason_code for r in result.rejections] == [
        RejectionReason.LICENSE_NOT_PERMITTED
    ]
    assert "NC" in result.rejections[0].detail
    assert result.metrics["rejected_license_not_permitted"] == 1


def test_a_refused_licence_is_distinguishable_from_an_unreadable_one():
    """
    Two different failures that must not share a reason code: one says the
    source changed its markup, the other says the source is serving licences we
    will not take. Merging them would hide the second behind the first.
    """
    result = normalize.run(
        [
            (_validated("CC BY-ND 2.0", "c" * 64), None, None),
            (_validated("All rights reserved", "d" * 64), None, None),
        ],
        LicenseSettings(),
    )

    assert result.kept == []
    assert {r.reason_code for r in result.rejections} == {
        RejectionReason.LICENSE_NOT_PERMITTED,
        RejectionReason.UNRECOGNISED_LICENSE,
    }


def test_public_domain_images_are_never_caught_by_the_gate():
    """CC0 and PDM carry no elements at all; a naive substring check fails here."""
    result = normalize.run(
        [
            (_validated("CC0", "e" * 64), None, None),
            (_validated("Public domain", "f" * 64), None, None),
        ],
        LicenseSettings(),
    )
    assert len(result.kept) == 2
    assert result.rejections == []


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
