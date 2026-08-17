"""
End-to-end integration test.

Runs the *real* pipeline — real HttpClient, real downloads, real Pillow
validation, real perceptual hashing, real Postgres, real Parquet export —
against a local HTTP server serving deliberately messy files.

Nothing is mocked except the source adapters, which are replaced by ones that
point at localhost. That substitution is the whole trick: it keeps the test
hermetic and fast while still exercising every line of the pipeline that
touches bytes, the database and the filesystem.

The fixture server serves, on purpose:

    3 good photographs                  -> kept
    1 exact duplicate (same bytes)      -> collapsed by sha256
    1 resized copy                      -> caught by pHash
    1 HTML error page named .jpg        -> UNREADABLE_IMAGE
    1 truncated JPEG                    -> TRUNCATED_IMAGE
    1 PNG served at a .jpg URL          -> EXTENSION_MISMATCH
    1 16x16 tracking pixel              -> too small
    1 URL that 404s                     -> HTTP_ERROR
    1 record with an unusable licence   -> UNRECOGNISED_LICENSE

Skipped automatically when no Postgres is reachable, so `pytest` still passes
on a machine with nothing running.

    TEST_POSTGRES_HOST=127.0.0.1 TEST_POSTGRES_PORT=5433 pytest tests/test_integration.py
"""

from __future__ import annotations

import functools
import http.server
import io
import json
import os
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import psycopg
import pytest
from PIL import Image
from pydantic import SecretStr

from src.config import (
    DatabaseSettings,
    HttpSettings,
    PathSettings,
    Settings,
    SourceSettings,
)
from src.db.connection import apply_schema, connect
from src.db.repository import Repository
from src.models import RawRecord, RunStatus, SourceName
from src.sources.base import ImageSource
from tests.conftest import make_image

PG_HOST = os.getenv("TEST_POSTGRES_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("TEST_POSTGRES_PORT", "5433"))
PG_USER = os.getenv("TEST_POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("TEST_POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("TEST_POSTGRES_DB", "pipeline_itest")


def _postgres_available() -> bool:
    try:
        with socket.create_connection((PG_HOST, PG_PORT), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _postgres_available(), reason=f"no Postgres at {PG_HOST}:{PG_PORT}"
)


# =============================================================================
#  Fixture web server
# =============================================================================


def _build_fixture_files(root: Path) -> None:
    """Create the messy corpus described in the module docstring."""
    root.mkdir(parents=True, exist_ok=True)

    for index, seed in enumerate((1, 3, 5), start=1):
        make_image(root / f"cat_{index}.jpg", size=(480, 360), seed=seed)

    # Exact duplicate: byte-identical, different URL.
    (root / "cat_1_copy.jpg").write_bytes((root / "cat_1.jpg").read_bytes())

    # Near duplicate: same photograph, resized and re-compressed.
    with Image.open(root / "cat_2.jpg") as img:
        img.resize((240, 180)).save(root / "cat_2_thumb.jpg", format="JPEG", quality=70)

    # HTML error page served at an image URL.
    (root / "dead_link.jpg").write_bytes(
        b"<!DOCTYPE html><html><body><h1>404 Not Found</h1></body></html>" + b" " * 1500
    )

    # Truncated download.
    buffer = io.BytesIO()
    Image.new("RGB", (400, 400), (10, 90, 200)).save(buffer, format="JPEG", quality=95)
    data = buffer.getvalue()
    (root / "truncated.jpg").write_bytes(data[: len(data) // 2])

    # PNG bytes at a .jpg URL.
    make_image(root / "_tmp.png", fmt="PNG", seed=7)
    (root / "mislabelled.jpg").write_bytes((root / "_tmp.png").read_bytes())
    (root / "_tmp.png").unlink()

    # Tracking pixel.
    make_image(root / "pixel.jpg", size=(16, 16), seed=2)

    # A perfectly good photograph whose licence we cannot use. Must be a file
    # nothing else references, or it would be caught as an exact duplicate at
    # download and never reach the licence check we are trying to exercise.
    make_image(root / "dog_1.jpg", size=(500, 400), seed=9)


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args) -> None:  # keep pytest output readable
        pass


@pytest.fixture(scope="module")
def fixture_server(tmp_path_factory) -> Iterator[str]:
    root = tmp_path_factory.mktemp("www")
    _build_fixture_files(root)

    handler = functools.partial(_QuietHandler, directory=str(root))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


# =============================================================================
#  A source that points at the fixture server
# =============================================================================


class LocalFixtureSource(ImageSource):
    """
    Stands in for Openverse. Everything downstream of `fetch` is the real code
    path — this only changes where the URLs point.
    """

    name = SourceName.OPENVERSE

    def __init__(self, client, settings, base_url: str) -> None:
        super().__init__(client, settings)
        self.base_url = base_url

    #: (filename, licence, class)
    CORPUS = [
        ("cat_1.jpg", "by-4.0", "cat"),
        ("cat_2.jpg", "by-sa-3.0", "cat"),
        ("cat_3.jpg", "cc0", "cat"),
        ("cat_1_copy.jpg", "by-4.0", "cat"),          # exact duplicate
        ("cat_2_thumb.jpg", "by-sa-3.0", "cat"),      # near duplicate
        ("dead_link.jpg", "by-4.0", "cat"),           # html error page
        ("truncated.jpg", "by-4.0", "cat"),           # truncated
        ("mislabelled.jpg", "by-4.0", "cat"),         # png at .jpg
        ("pixel.jpg", "by-4.0", "cat"),               # too small
        ("does_not_exist.jpg", "by-4.0", "cat"),      # 404
        ("dog_1.jpg", "All rights reserved", "dog"),  # unusable licence
    ]

    def fetch(self, class_label: str, limit: int) -> Iterator[RawRecord]:
        for index, (filename, licence, label) in enumerate(self.CORPUS):
            if label != class_label:
                continue
            yield RawRecord(
                source=self.name,
                class_label=class_label,
                image_url=f"{self.base_url}/{filename}",
                source_id=f"fixture-{index}-{filename}",
                landing_url=f"{self.base_url}/page/{filename}",
                license_raw=licence,
                license_url="https://creativecommons.org/licenses/by/4.0/",
                attribution="Fixture Author",
                title=filename,
                payload={"filename": filename, "license": licence},
                fetched_at=datetime.now(timezone.utc),
            )


# =============================================================================
#  Settings / database fixtures
# =============================================================================


@pytest.fixture(scope="module")
def database() -> Iterator[DatabaseSettings]:
    admin_dsn = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/postgres"
    with psycopg.connect(admin_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{PG_DB}"')
        cur.execute(f'CREATE DATABASE "{PG_DB}"')

    yield DatabaseSettings(
        host=PG_HOST, port=PG_PORT, db=PG_DB, user=PG_USER,
        password=SecretStr(PG_PASSWORD), connect_max_retries=3,
    )


@pytest.fixture(scope="module")
def settings(database, tmp_path_factory) -> Settings:
    data_dir = tmp_path_factory.mktemp("data")
    return Settings(
        log_json=False,
        log_level="WARNING",
        download_workers=4,
        database=database,
        paths=PathSettings(data_dir=data_dir),
        # No politeness delay against our own loopback server — the delay is a
        # courtesy to third parties, and 15 downloads x 0.5s would make the
        # suite needlessly slow.
        http=HttpSettings(min_delay_seconds=0.0, max_retries=2, respect_robots_txt=False),
        sources=SourceSettings(classes=("cat", "dog"), target_per_class=20, min_per_class=1),
    )


def _run_pipeline(settings, fixture_server, monkeypatch):
    """Execute the real runner with the fixture source substituted in."""
    from src.pipeline import run as run_module

    monkeypatch.setattr(
        run_module,
        "build_sources",
        lambda client, source_settings: [
            LocalFixtureSource(client, source_settings, fixture_server)
        ],
    )
    conn = connect(settings.database)
    apply_schema(conn)
    try:
        return run_module.PipelineRunner(conn, settings).execute(), conn
    finally:
        pass


@pytest.fixture(scope="module")
def first_run(settings, fixture_server):
    from _pytest.monkeypatch import MonkeyPatch

    patcher = MonkeyPatch()
    report, conn = _run_pipeline(settings, fixture_server, patcher)
    yield report, conn, settings
    patcher.undo()
    conn.close()


# =============================================================================
#  Assertions
# =============================================================================


def test_run_completes_and_is_recorded(first_run):
    report, conn, _ = first_run
    assert report.status in {RunStatus.SUCCESS, RunStatus.PARTIAL}
    summary = Repository(conn).run_summary(report.run_id)
    assert summary["fetched"] > 0
    assert summary["kept"] > 0
    assert summary["rejection_rate_pct"] > 0, "the messy fixtures must produce rejections"


def test_every_broken_fixture_is_rejected_with_the_right_reason(first_run):
    """
    The core claim of the whole exercise: messy data is *handled*, and each
    failure is attributed to a specific, queryable cause.
    """
    report, conn, _ = first_run
    with conn.cursor() as cur:
        cur.execute(
            "SELECT reason_code, count(*) AS n FROM rejections WHERE run_id = %s GROUP BY 1",
            (report.run_id,),
        )
        reasons = {row["reason_code"]: row["n"] for row in cur.fetchall()}

    assert "UNREADABLE_IMAGE" in reasons, "HTML error page served as .jpg"
    assert "TRUNCATED_IMAGE" in reasons, "half a JPEG"
    assert "EXTENSION_MISMATCH" in reasons, "PNG bytes at a .jpg URL"
    assert "HTTP_ERROR" in reasons, "404"
    assert "UNRECOGNISED_LICENSE" in reasons, "'All rights reserved'"
    assert "EXACT_DUPLICATE" in reasons or "NEAR_DUPLICATE" in reasons
    # 16x16 pixel: caught by whichever bound fires first.
    assert {"DIMENSIONS_TOO_SMALL", "FILESIZE_TOO_SMALL"} & set(reasons)


def test_exact_duplicate_produces_a_single_row(first_run):
    """Two URLs, identical bytes, one row — enforced by the UNIQUE constraint."""
    _, conn, _ = first_run
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n, count(DISTINCT sha256) AS d FROM images")
        row = cur.fetchone()
    assert row["n"] == row["d"]


def test_near_duplicate_is_marked_not_deleted(first_run):
    _, conn, _ = first_run
    with conn.cursor() as cur:
        cur.execute(
            "SELECT sha256, duplicate_of, duplicate_distance, split FROM images "
            "WHERE duplicate_of IS NOT NULL"
        )
        duplicates = cur.fetchall()

    assert duplicates, "the resized copy must be detected by pHash"
    for row in duplicates:
        assert row["duplicate_distance"] is not None
        assert row["split"] is None, "a duplicate must never carry a split"


def test_only_licensed_images_are_stored(first_run):
    _, conn, _ = first_run
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT license FROM images")
        licences = {row["license"] for row in cur.fetchall()}
    assert licences
    assert all(lic.startswith(("CC-", "CC0", "PDM")) for lic in licences), licences


def test_export_artefacts_are_written_and_consistent(first_run):
    import pandas as pd

    report, _, settings = first_run
    output = settings.paths.output_dir

    for name in ("dataset.parquet", "dataset.csv", "manifest.json", "ATTRIBUTIONS.txt"):
        assert (output / name).is_file(), f"missing {name}"

    frame = pd.read_parquet(output / "dataset.parquet")
    manifest = json.loads((output / "manifest.json").read_text())

    assert len(frame) == manifest["n_images"] == len(manifest["images"])
    assert set(frame["split"]) <= {"train", "val"}
    # Parquet preserved the integer type; CSV would have made these strings.
    assert frame["width"].dtype.kind == "i"

    # Every manifest path must resolve to a file that is actually there.
    for entry in manifest["images"]:
        assert (output / entry["path"]).is_file(), entry["path"]
        assert entry["checksum"]["algorithm"] == "sha256"


def test_manifest_carries_provenance_for_reproducibility(first_run):
    report, _, settings = first_run
    manifest = json.loads((settings.paths.output_dir / "manifest.json").read_text())
    assert manifest["run_id"] == str(report.run_id)
    assert manifest["dataset_fingerprint"]
    assert manifest["config"], "the producing configuration travels with the dataset"
    assert "password" not in json.dumps(manifest["config"]), "secrets must not leak"


def test_images_are_content_addressed_on_disk(first_run):
    _, conn, settings = first_run
    with conn.cursor() as cur:
        cur.execute("SELECT sha256, storage_path FROM images LIMIT 5")
        rows = cur.fetchall()
    for row in rows:
        path = Path(row["storage_path"])
        assert path.is_file()
        assert path.stem == row["sha256"]
        assert path.parent.name == row["sha256"][:2]


# =============================================================================
#  The headline property: idempotency
# =============================================================================


def test_second_run_adds_no_duplicate_rows_and_downloads_nothing_new(
    first_run, fixture_server
):
    """
    `docker compose up` twice must not corrupt anything.

    Asserted three ways, because each catches a different mistake:
      * row count unchanged        -> the ON CONFLICT upsert works
      * dataset fingerprint equal  -> the exported dataset is byte-identical
      * every image already known  -> content-addressed storage skipped the writes
    """
    from _pytest.monkeypatch import MonkeyPatch

    report_1, conn, settings = first_run

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM images")
        rows_before = cur.fetchone()["n"]
    # Release this reader's implicit transaction before starting a second run.
    # An idle-in-transaction session holds a read lock on `images`, which the
    # second run's schema apply would otherwise queue behind. (Production is
    # protected by the lock_timeout in apply_schema; a test should not rely on
    # hitting that timeout to pass.)
    conn.rollback()
    fingerprint_before = json.loads(
        (settings.paths.output_dir / "manifest.json").read_text()
    )["dataset_fingerprint"]

    patcher = MonkeyPatch()
    report_2, conn_2 = _run_pipeline(settings, fixture_server, patcher)
    try:
        with conn_2.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM images")
            rows_after = cur.fetchone()["n"]

        fingerprint_after = json.loads(
            (settings.paths.output_dir / "manifest.json").read_text()
        )["dataset_fingerprint"]

        assert rows_after == rows_before, "a second run must not insert duplicate rows"
        assert fingerprint_after == fingerprint_before, "the dataset must be identical"
        assert report_2.metrics["persist"]["inserted"] == 0
        assert report_2.metrics["persist"]["already_known"] > 0
        assert report_2.run_id != report_1.run_id, "but it is a distinct run"

        with conn_2.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM pipeline_runs")
            assert cur.fetchone()["n"] == 2
    finally:
        patcher.undo()
        conn_2.close()
