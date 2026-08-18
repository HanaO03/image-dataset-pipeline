"""
Stage 5 — normalise two sources into one schema.

The interesting work here is licence normalisation, and it is worth more than
it looks. The same permission arrives as:

    Openverse:  license="by-sa", license_version="4.0"
    Commons:    "CC BY-SA 4.0"  /  "Creative Commons Attribution-Share Alike 4.0"
    Commons:    "Public domain" /  "CC0"  /  "PD-self"

A downstream consumer needs to answer "may I train on this, and must I credit
anyone?" — which is impossible against free text. We map everything onto SPDX
identifiers (`CC-BY-SA-4.0`, `CC0-1.0`, `PDM-1.0`) so the question becomes a
lookup rather than a judgement call.

Anything we cannot map is **rejected**, not stored with a shrug. That is the
strict licence policy carried through to its logical end: an unresolvable
licence is indistinguishable from no licence, and this dataset is intended for
training a model that will be shipped.

The other half of that policy also lives here: a licence we *can* read, which
forbids commercial use (NC) or derivative works (ND), is rejected too. It has
to happen at this stage rather than at the source, because only one of the two
sources has a licence filter to configure — the Openverse API does, a scraped
Commons file page does not. Filtering only where it is convenient produces
exactly the failure this stage now prevents: a dataset that is described as
commercially usable and contains images that are not.
"""

from __future__ import annotations

import re

from ..config import LicenseSettings
from ..logging_setup import get_logger
from ..models import (
    ImageRecord,
    Rejection,
    RejectionReason,
    Stage,
    StageResult,
    ValidatedImage,
)

log = get_logger(__name__)

#: Canonical CC element order, used to rebuild an SPDX id from parsed parts.
_CC_ELEMENTS = ("BY", "NC", "ND", "SA")

#: Licences that need no attribution and no share-alike — the easiest to use.
_PUBLIC_DOMAIN = {
    "cc0": "CC0-1.0",
    "cc-zero": "CC0-1.0",
    "zero": "CC0-1.0",
    "pdm": "PDM-1.0",
    "publicdomain": "PDM-1.0",
    "public domain": "PDM-1.0",
    "pd": "PDM-1.0",
    "pd-self": "PDM-1.0",
    "pd-us": "PDM-1.0",
    "pd-old": "PDM-1.0",
}

#: Long-form names Commons renders in prose, one pattern per element.
#:
#: Each is tested independently and every match contributes, which is the
#: correction to a version that returned the elements of the *first* whole-name
#: pattern that matched. Two failures came out of that design, both silent, both
#: relabelling a restricted licence as a permissive one:
#:
#:   "Attribution-NonCommercial-NoDerivs 2.0"  ->  CC-BY-NC-2.0   (ND dropped)
#:   "Attribution-NoDerivs 3.0"                ->  CC-BY-3.0      (ND dropped)
#:
#: The second is the worse of the two: it turns the one licence this dataset
#: must never contain into the one it most wants, and the NC/ND gate downstream
#: then has nothing to catch.
#:
#: `no[-\s]*deriv` rather than `no[-\s]*derivat` because Creative Commons
#: renamed the element between generations: 2.0 and 3.0 render "NoDerivs",
#: 4.0 renders "NoDerivatives". The shorter stem covers both, plus the prose
#: form "No Derivative Works".
_LONGFORM_ELEMENTS = (
    (re.compile(r"\battribution\b", re.I), "BY"),
    (re.compile(r"non[-\s]*commercial", re.I), "NC"),
    (re.compile(r"no[-\s]*deriv", re.I), "ND"),
    (re.compile(r"share[-\s]*alike", re.I), "SA"),
)

_VERSION_RE = re.compile(r"(\d+\.\d+)")
#: Default version when the source states a CC licence without one. 4.0 is the
#: current generation; recording an explicit default beats recording nothing,
#: and the raw string is preserved in raw_records either way.
_DEFAULT_CC_VERSION = "4.0"


def normalise_license(raw: str | None) -> str | None:
    """
    Map a free-text or coded licence onto an SPDX-style identifier.

    Returns None when the input cannot be resolved — the caller rejects those.
    Pure and total: no exceptions, no I/O, trivially unit-testable, which is
    exactly what a function encoding legal policy should be.
    """
    if not raw:
        return None

    text = raw.strip()
    lowered = text.lower()

    # Public domain / CC0 first: these have no elements or version to parse.
    for needle, spdx in _PUBLIC_DOMAIN.items():
        if lowered == needle or re.search(rf"\b{re.escape(needle)}\b", lowered):
            return spdx

    version_match = _VERSION_RE.search(text)
    version = version_match.group(1) if version_match else None

    elements: list[str] = []

    # Coded form: "by-sa-4.0", "by-nc-nd", "BY_SA"
    coded = re.findall(r"\b(by|nc|nd|sa)\b", lowered)
    if coded:
        elements = [c.upper() for c in coded]
    else:
        # Every element that appears, not the first name that matches.
        elements = [code for pattern, code in _LONGFORM_ELEMENTS if pattern.search(text)]
        # "Attribution" is implied by any CC element name in prose form, but a
        # string naming only restrictions is not a licence we can reconstruct.
        if elements and "BY" not in elements:
            elements = []

    if not elements:
        return None

    # Deduplicate and impose canonical SPDX element order.
    ordered = [e for e in _CC_ELEMENTS if e in set(elements)]
    if not ordered:
        return None

    return f"CC-{'-'.join(ordered)}-{version or _DEFAULT_CC_VERSION}"


def requires_attribution(spdx: str) -> bool:
    """True when the licence obliges us to reproduce a credit line."""
    return spdx.startswith("CC-BY")


def license_elements(spdx: str) -> frozenset[str]:
    """
    The CC elements carried by an SPDX identifier: `CC-BY-NC-SA-2.0` -> {BY,NC,SA}.

    Parsed from the identifier rather than from the source's free text on
    purpose — by this point the free text has already been normalised, and one
    representation with one parser is the whole reason normalisation exists.
    Public-domain marks (`CC0-1.0`, `PDM-1.0`) carry no elements and return
    empty, which is correct: they restrict nothing.
    """
    if not spdx.startswith("CC-") or spdx.startswith("CC0"):
        return frozenset()
    body = spdx[len("CC-"):]
    return frozenset(
        part for part in body.split("-") if part in _CC_ELEMENTS
    )


def license_permits(spdx: str, forbidden_elements: tuple[str, ...]) -> bool:
    """
    Whether this licence may enter the dataset.

    Pure and total, like `normalise_license`, and for the same reason: a
    function encoding legal policy should be testable without constructing a
    pipeline, a source or a file.
    """
    return not (license_elements(spdx) & set(forbidden_elements))


def run(
    items: list[tuple[ValidatedImage, str | None, int | None]],
    settings: LicenseSettings,
) -> StageResult:
    """
    Build ImageRecords from validated images, enforcing the licence policy.

    Input is the dedupe stage's output: (image, duplicate_of_sha256, distance).

    The policy gate lives here, at the point where both sources have converged
    on one schema, because that is the only place it can apply to all of them.
    Filtering at the API covers the API alone — the scraper reads whatever the
    file page says.
    """
    result = StageResult()

    for item, duplicate_of, distance in items:
        raw = item.raw
        spdx = normalise_license(raw.license_raw)

        if spdx is not None and not license_permits(spdx, settings.forbidden_elements):
            result.rejections.append(
                Rejection.from_raw(
                    raw,
                    Stage.NORMALIZE,
                    RejectionReason.LICENSE_NOT_PERMITTED,
                    f"{spdx} carries "
                    f"{'/'.join(sorted(license_elements(spdx) & set(settings.forbidden_elements)))}"
                    f", which this dataset does not admit",
                )
            )
            result.add_metric("rejected_license_not_permitted")
            continue

        if spdx is None:
            result.rejections.append(
                Rejection.from_raw(
                    raw,
                    Stage.NORMALIZE,
                    RejectionReason.UNRECOGNISED_LICENSE,
                    f"could not map licence: {raw.license_raw!r}",
                )
            )
            result.add_metric("rejected_unrecognised_license")
            continue

        attribution = raw.attribution
        if requires_attribution(spdx) and not attribution:
            # A BY licence with no creator string is a compliance gap, but the
            # licence itself is valid and the landing URL preserves provenance.
            # Record a placeholder and count it rather than discarding usable
            # data over missing metadata.
            attribution = f"Unknown author (see {raw.landing_url or raw.image_url})"
            result.add_metric("attribution_placeholder")

        record = ImageRecord(
            sha256=item.sha256,
            class_label=raw.class_label.strip().lower(),
            source=raw.source,
            source_id=raw.source_id,
            origin_url=raw.image_url,
            source_url=raw.landing_url,
            license=spdx,
            license_url=raw.license_url,
            attribution=attribution,
            storage_path=str(item.downloaded.storage_path),
            image_format=item.image_format,
            width=item.width,
            height=item.height,
            file_size_bytes=item.downloaded.file_size_bytes,
            phash=item.phash,
            retrieved_at=raw.fetched_at,
        )
        if duplicate_of is not None and distance is not None:
            record = record.marked_duplicate_of(duplicate_of, distance)

        result.kept.append(record)
        result.add_metric("normalised")

    log.info("normalisation complete", extra=dict(result.metrics))
    return result
