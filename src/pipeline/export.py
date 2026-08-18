"""
Stage 7 — write the ML-ready dataset to /data/output.

Produces four things, each for a different consumer:

    dataset.parquet   the primary artefact — typed, compressed, columnar
    dataset.csv       the same rows, for a human with a text editor
    manifest.json     what a training script actually loads
    images/<split>/<class>/<sha>.<ext>   the conventional folder layout

**Parquet as primary, CSV as courtesy.** CSV loses every type (dates become
strings, integers become strings, nulls become empty strings that are
indistinguishable from empty text), needs careful quoting for the unicode that
appears in attribution strings, and is several times larger. Parquet keeps the
schema, so a training script reads `width` as an integer without a cast. CSV
ships anyway because the brief allows it and because being able to eyeball the
output in any editor has real value during review.

**Paths in the manifest are relative.** An absolute `/data/output/...` is only
correct inside this container. Relative paths keep the exported folder portable
— copy it anywhere, or mount it at a different point, and it still resolves.

**Images are copied, not symlinked**, into the split/class folder layout. The
canonical store stays content-addressed; the export is a self-contained
deliverable someone can zip and hand to a data scientist. Symlinks would break
the moment it leaves this filesystem.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pandas as pd

from ..logging_setup import get_logger

log = get_logger(__name__)

#: Bump when the manifest's shape changes. A training script can then refuse a
#: manifest it does not understand instead of failing on a missing key.
MANIFEST_SCHEMA_VERSION = "1.0"


#: Pillow format name -> the extension a CV loader expects to see.
_FORMAT_EXTENSIONS = {
    "JPEG": ".jpg", "PNG": ".png", "WEBP": ".webp",
    "GIF": ".gif", "BMP": ".bmp", "TIFF": ".tif",
}


def _relative_image_path(row: dict[str, Any]) -> str:
    """
    images/<split>/<class>/<sha256><ext> — the conventional CV layout.

    The extension comes from the *decoded* format, never from the stored path.
    That path is named after the URL, and a URL with no usable extension is
    stored as `.bin`: a perfectly good JPEG would then be exported as
    `<sha>.bin` and skipped in silence by every loader that filters on
    extension, including `torchvision.datasets.ImageFolder`. The format was
    measured from the bytes at validation, so use the measurement.
    """
    extension = _FORMAT_EXTENSIONS.get(
        (row.get("format") or "").upper(), Path(row["storage_path"]).suffix or ".jpg"
    )
    return f"images/{row['split']}/{row['class_label']}/{row['sha256']}{extension}"


def _copy_images(rows: list[dict[str, Any]], output_dir: Path) -> tuple[int, list[str], int]:
    """
    Materialise the split/class folder tree. Returns (copied, missing, pruned).

    A row whose source file has vanished is reported rather than raised: the
    manifest is still correct for everything else, and a partial export with a
    known gap beats no export at all.

    **The prune is not housekeeping.** Splitting runs across the whole stored
    dataset, so an image can legitimately move from train to val as the dataset
    grows. Copying without pruning left the previous copy exactly where it was,
    and the same photograph then existed under `images/train/cat/` *and*
    `images/val/cat/` — invisible in the manifest, which is generated from the
    database and stays correct, but plainly visible to `ImageFolder` and every
    other loader that walks the directory tree. That is train/val leakage: the
    precise failure the deduplication stage exists to prevent, reintroduced at
    the last step by an export that only ever added.

    Only files under `images/` are touched. `sample_images/` is a sibling and
    is committed to the repository, not generated here.
    """
    copied = 0
    missing: list[str] = []
    expected: set[Path] = set()

    for row in rows:
        source = Path(row["storage_path"])
        if not source.is_file():
            missing.append(row["sha256"])
            continue
        destination = output_dir / _relative_image_path(row)
        expected.add(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            shutil.copy2(source, destination)
        copied += 1

    pruned = _prune_stale_images(output_dir / "images", expected)
    return copied, missing, pruned


def _prune_stale_images(images_root: Path, expected: set[Path]) -> int:
    """
    Delete every file under `images/` that the current dataset does not claim.

    Covers the image that changed split, the image trimmed at the class target,
    and the image dropped as a near-duplicate on a later run — all of which
    would otherwise linger in the exported tree and be loaded as training data
    that the manifest says nothing about.
    """
    if not images_root.is_dir():
        return 0

    pruned = 0
    for path in images_root.rglob("*"):
        if path.is_file() and path not in expected:
            path.unlink()
            pruned += 1

    # Leave no empty class or split directories behind: an empty `val/bird/`
    # reads as a class with no validation images, which is a real condition
    # this pipeline warns about and must not fake.
    for path in sorted(images_root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()

    if pruned:
        log.info("export: pruned images no longer in the dataset",
                 extra={"pruned": pruned})
    return pruned


def _dataset_fingerprint(rows: list[dict[str, Any]]) -> str:
    """
    A content hash of the dataset itself: sha256 over every member's checksum
    and split, in sorted order.

    This is the lightweight version of dataset versioning. Two runs producing
    the same fingerprint produced literally the same dataset, which is exactly
    the question DVC answers at much greater cost. Cheap, and it makes the
    reproducibility claim checkable rather than rhetorical.
    """
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda r: r["sha256"]):
        digest.update(f"{row['sha256']}:{row['split']}:{row['class_label']}".encode())
    return digest.hexdigest()[:16]


def run(
    rows: list[dict[str, Any]],
    output_dir: Path,
    run_id: UUID,
    config_snapshot: dict[str, Any] | None = None,
    copy_images: bool = True,
) -> dict[str, Any]:
    """Write every artefact. Returns a summary for logging and the run report."""
    output_dir.mkdir(parents=True, exist_ok=True)

    if not rows:
        log.warning("export: nothing to write — dataset is empty")
        return {"records": 0, "files": []}

    for row in rows:
        row["relative_path"] = _relative_image_path(row)

    copied, missing, pruned = (0, [], 0)
    if copy_images:
        copied, missing, pruned = _copy_images(rows, output_dir)
        if missing:
            log.warning("export: image files missing from store",
                        extra={"count": len(missing), "examples": missing[:5]})

    frame = pd.DataFrame(rows)
    # Stable column order: a diff between two exports should show changed data,
    # not reshuffled columns.
    columns = [
        "sha256", "class_label", "split", "relative_path", "source", "source_id",
        "source_url", "origin_url", "license", "license_url", "attribution",
        "format", "width", "height", "file_size_bytes", "retrieved_at",
    ]
    frame = frame[[c for c in columns if c in frame.columns]]

    parquet_path = output_dir / "dataset.parquet"
    csv_path = output_dir / "dataset.csv"
    frame.to_parquet(parquet_path, index=False, compression="snappy")
    frame.to_csv(csv_path, index=False, encoding="utf-8")

    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        counts[row["class_label"]][row["split"]] += 1

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "dataset_fingerprint": _dataset_fingerprint(rows),
        "run_id": str(run_id),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "n_images": len(rows),
        "classes": sorted(counts),
        "splits": sorted({row["split"] for row in rows}),
        "counts": {cls: dict(splits) for cls, splits in sorted(counts.items())},
        "licenses": _license_breakdown(rows),
        "config": config_snapshot or {},
        "images": [
            {
                "path": row["relative_path"],
                "label": row["class_label"],
                "split": row["split"],
                "source": row["source"],
                "source_url": row["source_url"],
                "license": row["license"],
                "attribution": row["attribution"],
                "checksum": {"algorithm": "sha256", "value": row["sha256"]},
                "width": row["width"],
                "height": row["height"],
            }
            for row in sorted(rows, key=lambda r: (r["class_label"], r["split"], r["sha256"]))
        ],
    }

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )

    # Attribution is a licence obligation, not a nicety: CC-BY requires the
    # credit to travel with the work. A plain text file next to the data is the
    # simplest form that survives someone copying the folder elsewhere.
    _write_attributions(rows, output_dir)

    summary = {
        "records": len(rows),
        "images_copied": copied,
        "images_missing": len(missing),
        "images_pruned": pruned,
        "fingerprint": manifest["dataset_fingerprint"],
        "counts": manifest["counts"],
        "files": [str(parquet_path), str(csv_path), str(manifest_path)],
    }
    log.info("export complete", extra=summary)
    return summary


def _license_breakdown(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["license"]] += 1
    return dict(sorted(counts.items()))


def _write_attributions(rows: list[dict[str, Any]], output_dir: Path) -> None:
    lines = [
        "ATTRIBUTIONS",
        "",
        "Images in this dataset are reused under the licences listed below.",
        "Retaining these credits is a condition of the CC-BY family of licences.",
        "",
    ]
    for row in sorted(rows, key=lambda r: (r["class_label"], r["sha256"])):
        lines.append(
            f"- {row['relative_path']}\n"
            f"    license: {row['license']}\n"
            f"    source:  {row['source_url'] or row['origin_url']}\n"
            f"    credit:  {row['attribution'] or 'n/a'}"
        )
    (output_dir / "ATTRIBUTIONS.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
