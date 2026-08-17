"""
Every SQL statement in the project lives here.

No other module imports psycopg or writes SQL. The value of that rule is not
tidiness — it is that the pipeline stages become testable without a database,
and that changing the storage engine touches exactly one file. It also means a
reviewer can audit the entire data-access surface by reading one module.

Transaction policy: this class never commits. The caller (`pipeline/run.py`)
owns transaction boundaries, because only the caller knows what constitutes a
complete unit of work.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence
from uuid import UUID

import psycopg
from psycopg import Connection
from psycopg.types.json import Jsonb

from ..logging_setup import get_logger
from ..models import ImageRecord, RawRecord, Rejection, RunStatus, Split

log = get_logger(__name__)


class Repository:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    # =========================================================================
    #  Runs
    # =========================================================================

    def start_run(
        self,
        config_snapshot: dict[str, Any],
        git_commit: str | None = None,
        trigger: str = "cli",
    ) -> UUID:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (status, trigger, config_snapshot, git_commit)
                VALUES ('running', %s, %s, %s)
                RETURNING run_id
                """,
                (trigger, Jsonb(config_snapshot), git_commit),
            )
            return cur.fetchone()["run_id"]

    def finish_run(
        self, run_id: UUID, status: RunStatus, error_message: str | None = None
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                   SET status = %s, finished_at = now(), error_message = %s
                 WHERE run_id = %s
                """,
                (status.value, error_message, run_id),
            )

    def run_summary(self, run_id: UUID) -> dict[str, Any] | None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM run_summary WHERE run_id = %s", (run_id,))
            return cur.fetchone()

    def dataset_composition(self) -> list[dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM dataset_composition")
            return cur.fetchall()

    # =========================================================================
    #  Raw layer
    # =========================================================================

    def insert_raw_records(self, run_id: UUID, records: Sequence[RawRecord]) -> int:
        """
        Land upstream payloads verbatim, before any interpretation.

        Append-only by design: this is the replay source. If normalisation logic
        changes tomorrow, it can be re-applied to these rows without paying the
        rate limit (or the fragility) of fetching again.
        """
        if not records:
            return 0
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO raw_records (run_id, source, source_id, class_label, payload, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        run_id,
                        r.source.value,
                        r.source_id,
                        r.class_label,
                        Jsonb(r.payload),
                        r.fetched_at,
                    )
                    for r in records
                ],
            )
        return len(records)

    # =========================================================================
    #  Curated layer
    # =========================================================================

    _UPSERT_IMAGE = """
        INSERT INTO images (
            sha256, phash, class_label, source, source_id, source_url, origin_url,
            license, license_url, attribution,
            storage_path, format, width, height, file_size_bytes,
            split, retrieved_at, first_seen_run_id, last_seen_run_id
        ) VALUES (
            %(sha256)s, %(phash)s, %(class_label)s, %(source)s, %(source_id)s,
            %(source_url)s, %(origin_url)s,
            %(license)s, %(license_url)s, %(attribution)s,
            %(storage_path)s, %(format)s, %(width)s, %(height)s, %(file_size_bytes)s,
            %(split)s, %(retrieved_at)s, %(run_id)s, %(run_id)s
        )
        ON CONFLICT (sha256) DO UPDATE
            SET last_seen_run_id = EXCLUDED.last_seen_run_id,
                split            = COALESCE(EXCLUDED.split, images.split)
        RETURNING id, (xmax = 0) AS was_inserted
    """

    def upsert_image(self, run_id: UUID, record: ImageRecord) -> tuple[int, bool]:
        """
        Insert an image, or note that we have seen it again.

        `ON CONFLICT (sha256) DO UPDATE` is the idempotency mechanism: running
        the pipeline twice touches the same rows instead of duplicating them.
        `xmax = 0` distinguishes a genuine insert from an update, so the run can
        report "12 new, 48 already known" rather than an undifferentiated count.

        A savepoint wraps the statement because the *other* unique constraint —
        (source, source_id) — can still fire when upstream re-encodes an asset
        it had already served us. Without the savepoint that error would abort
        the surrounding transaction and take the whole run down; with it, the
        single offending record is skipped and reported.
        """
        params = {
            "sha256": record.sha256,
            "phash": record.phash,
            "class_label": record.class_label,
            "source": record.source.value,
            "source_id": record.source_id,
            "source_url": record.source_url,
            "origin_url": record.origin_url,
            "license": record.license,
            "license_url": record.license_url,
            "attribution": record.attribution,
            "storage_path": record.storage_path,
            "format": record.image_format,
            "width": record.width,
            "height": record.height,
            "file_size_bytes": record.file_size_bytes,
            "split": record.split.value if record.split else None,
            "retrieved_at": record.retrieved_at,
            "run_id": run_id,
        }
        with self.conn.transaction():  # savepoint — see docstring
            with self.conn.cursor() as cur:
                cur.execute(self._UPSERT_IMAGE, params)
                row = cur.fetchone()
                return row["id"], row["was_inserted"]

    def upsert_images(
        self, run_id: UUID, records: Iterable[ImageRecord]
    ) -> tuple[dict[str, int], list[tuple[ImageRecord, str]]]:
        """
        Bulk upsert. Returns (counters, failures).

        Failures are returned rather than raised: a single conflicting record is
        a data problem, and data problems must not abort a run.
        """
        stats = {"inserted": 0, "already_known": 0, "conflicted": 0}
        failures: list[tuple[ImageRecord, str]] = []
        for record in records:
            try:
                _, was_inserted = self.upsert_image(run_id, record)
                stats["inserted" if was_inserted else "already_known"] += 1
            except psycopg.errors.UniqueViolation as exc:
                # Same (source, source_id) already stored under different bytes:
                # upstream re-encoded or replaced the asset since our last run.
                stats["conflicted"] += 1
                failures.append((record, str(exc).splitlines()[0]))
        return stats, failures

    def mark_duplicate(self, sha256: str, duplicate_of_sha256: str, distance: int) -> bool:
        """
        Flag a near-duplicate. Returns False when either row is absent.

        Deliberately an UPDATE, never a DELETE: the row stays queryable so "why
        is this image not in the dataset?" has an answer. Clearing the split at
        the same time is what stops a duplicate leaking into train or val.

        The join — rather than a scalar sub-select — is the point. A sub-select
        that finds nothing yields NULL, which would write `duplicate_of = NULL`
        alongside a non-null distance and trip `ck_images_duplicate_distance`,
        killing the run over a row that simply is not there. Joining instead
        updates nothing and says so, and the caller decides whether that matters.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE images AS target
                   SET duplicate_of = original.id,
                       duplicate_distance = %s,
                       split = NULL
                  FROM images AS original
                 WHERE target.sha256 = %s
                   AND original.sha256 = %s
                   AND original.id <> target.id
                """,
                (distance, sha256, duplicate_of_sha256),
            )
            return cur.rowcount > 0

    def set_split(self, sha256: str, split: Split) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE images SET split = %s WHERE sha256 = %s AND duplicate_of IS NULL",
                (split.value, sha256),
            )

    # -------------------------------------------------------------------------
    #  Reads used by the pipeline itself
    # -------------------------------------------------------------------------

    def known_sha256(self) -> set[str]:
        """
        Content hashes already stored.

        Used for *reporting* how much of a run was already-known content. It
        cannot drive skipping, because a hash is only knowable after the bytes
        have been fetched — see `known_origin_urls` for the part that actually
        avoids the download.
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT sha256 FROM images")
            return {row["sha256"] for row in cur.fetchall()}

    def known_origin_urls(self) -> dict[str, tuple[str, str]]:
        """
        origin_url -> (sha256, storage_path) for everything already stored.

        This is what makes a re-run genuinely cheap. The content hash is only
        knowable *after* downloading, so hashes alone cannot prevent the
        transfer; the URL is known before it. If we have fetched this exact URL
        before and the file is still in the content-addressed store, the bytes
        are reused and nothing goes over the network.

        The assumption — that a URL still serves the bytes it served last time
        — is true for both sources here (Openverse and Commons serve immutable
        asset URLs) but is not true in general. `--refetch` forces the download
        anyway; the principled version is a conditional GET carrying the stored
        ETag, which is noted in the README as the next step.
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT origin_url, sha256, storage_path FROM images")
            return {
                row["origin_url"]: (row["sha256"], row["storage_path"])
                for row in cur.fetchall()
            }

    def phash_index(self) -> list[dict[str, Any]]:
        """
        Non-duplicate images with a perceptual hash, for near-duplicate search.

        Returns whole rows so the keep-strategy can compare resolutions without
        a second query. Linear scan is correct at this volume; see the scale
        note in schema.sql for what replaces it.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, sha256, phash, class_label, width, height, created_at
                  FROM images
                 WHERE phash IS NOT NULL AND duplicate_of IS NULL
                 ORDER BY created_at, id
                """
            )
            return cur.fetchall()

    def dataset_rows(self) -> list[dict[str, Any]]:
        """The exportable dataset: deduplicated, split-assigned, licence-bearing."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT sha256, class_label, split, source, source_id, source_url,
                       origin_url, license, license_url, attribution,
                       storage_path, format, width, height, file_size_bytes,
                       retrieved_at
                  FROM images
                 WHERE duplicate_of IS NULL
                   AND split IS NOT NULL
                 ORDER BY class_label, split, sha256
                """
            )
            return cur.fetchall()

    def split_candidates(self) -> list[dict[str, Any]]:
        """
        Every non-duplicate image, whether or not it already has a split.

        Splitting deliberately considers the whole stored dataset rather than
        just the current batch: that is what keeps the ratio exact per class as
        the dataset grows, instead of drifting once per run. Assignment is
        content-derived, so re-deciding is stable — an image keeps its split
        unless the set around it changes materially.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT sha256, class_label, split FROM images
                 WHERE duplicate_of IS NULL
                 ORDER BY class_label, sha256
                """
            )
            return cur.fetchall()

    def class_counts(self) -> dict[str, int]:
        """Kept images per class — the input to the quality gate."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT class_label, count(*) AS n FROM images
                 WHERE duplicate_of IS NULL GROUP BY class_label
                """
            )
            return {row["class_label"]: row["n"] for row in cur.fetchall()}

    # =========================================================================
    #  Audit: rejections and metrics
    # =========================================================================

    def record_rejections(self, run_id: UUID, rejections: Sequence[Rejection]) -> int:
        if not rejections:
            return 0
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO rejections
                    (run_id, stage, source, class_label, source_id, source_url,
                     reason_code, detail, occurred_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        run_id,
                        r.stage.value,
                        r.source.value,
                        r.class_label,
                        r.source_id,
                        r.source_url,
                        r.reason_code.value,
                        r.detail,
                        r.occurred_at,
                    )
                    for r in rejections
                ],
            )
        return len(rejections)

    def add_metrics(self, run_id: UUID, stage: str, metrics: dict[str, int]) -> None:
        """
        Accumulate counters. `DO UPDATE SET value = value + EXCLUDED.value`
        means a stage may report incrementally without the caller tracking
        whether it is the first write.
        """
        if not metrics:
            return
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO run_stage_metrics (run_id, stage, metric, value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, stage, metric)
                DO UPDATE SET value = run_stage_metrics.value + EXCLUDED.value
                """,
                [(run_id, stage, metric, value) for metric, value in metrics.items()],
            )

    def rejection_breakdown(self, run_id: UUID) -> list[dict[str, Any]]:
        """Counts by stage and reason — the run's diagnostic summary."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT stage, reason_code, count(*) AS n
                  FROM rejections WHERE run_id = %s
                 GROUP BY stage, reason_code
                 ORDER BY n DESC, reason_code
                """,
                (run_id,),
            )
            return cur.fetchall()
