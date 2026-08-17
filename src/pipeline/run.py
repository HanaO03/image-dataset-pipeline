"""
The orchestrator.

This is the only module that knows the order of the stages, owns the `run_id`,
and decides transaction boundaries. Every stage below it is a pure-ish function
that takes records and returns records — none of them import the database, and
none of them know what runs before or after.

That separation is the point. It means each stage is unit-testable in isolation,
and it means swapping this file for an Airflow DAG or a Prefect flow later is a
mechanical change to one module rather than a rewrite: each `_stage_*` method
below is already shaped like a task.

**Error policy, applied consistently:**

* *fail-soft on data* — a broken link, an unreadable JPEG, an unmappable
  licence: recorded as a rejection with a reason code, the run continues.
* *fail-fast on infrastructure* — no database, an unwritable volume, invalid
  config: the run stops, is marked `failed`, and says why. Limping on would
  produce a dataset nobody can trust.
* *quality gate in between* — the run completed but a class came in under the
  minimum: status `partial`. Not a crash, and emphatically not a success.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from psycopg import Connection

from ..config import Settings
from ..db.repository import Repository
from ..http.client import HttpClient
from ..logging_setup import get_logger, set_run_id, stage_context
from ..models import (
    ImageRecord,
    RawRecord,
    Rejection,
    RunStatus,
    Split,
    Stage,
)
from ..sources import build_sources
from . import dedupe, download, export, normalize, select, validate
from .split import SplitCandidate, assign

log = get_logger(__name__)


@dataclass
class RunReport:
    """What the CLI prints and what the caller inspects. No DB objects leak out."""

    run_id: UUID
    status: RunStatus
    metrics: dict[str, dict[str, int]] = field(default_factory=dict)
    rejections_by_reason: dict[str, int] = field(default_factory=dict)
    class_counts: dict[str, int] = field(default_factory=dict)
    export_summary: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class PipelineRunner:
    def __init__(self, conn: Connection, settings: Settings) -> None:
        self.conn = conn
        self.settings = settings
        self.repo = Repository(conn)
        self.run_id: UUID | None = None

    # =========================================================================
    #  Entry point
    # =========================================================================

    def execute(self) -> RunReport:
        self.run_id = self.repo.start_run(
            config_snapshot=self.settings.snapshot(),
            git_commit=Settings.git_commit(),
        )
        self.conn.commit()  # make the run visible immediately, even if we crash
        set_run_id(str(self.run_id))
        log.info("run started", extra={"classes": list(self.settings.sources.classes)})

        report = RunReport(run_id=self.run_id, status=RunStatus.RUNNING)

        try:
            with HttpClient(self.settings.http) as client:
                raw_records = self._stage_ingest(client, report)
                downloads = self._stage_download(raw_records, client, report)

            validated = self._stage_validate(downloads, report)
            deduped = self._stage_dedupe(validated, report)
            records = self._stage_normalize(deduped, report)
            selected = self._stage_select(records, report)
            self._stage_persist(selected, report)
            self._stage_split(report)
            self._stage_export(report)

            report.class_counts = self.repo.class_counts()
            report.status = self._evaluate_quality_gate(report)
            report.rejections_by_reason = self._rejection_breakdown()
            self.repo.finish_run(self.run_id, report.status)
            self.conn.commit()

        except Exception as exc:  # infrastructure fault — fail fast, fail loud
            self.conn.rollback()
            report.status = RunStatus.FAILED
            report.error = f"{type(exc).__name__}: {exc}"
            log.error("run failed", extra={"error": report.error,
                                           "traceback": traceback.format_exc()})
            try:
                self.repo.finish_run(self.run_id, RunStatus.FAILED, report.error)
                self.conn.commit()
            except Exception:
                log.error("could not record failure status — database unreachable")
            return report

        log.info("run finished", extra={"status": report.status.value,
                                        "classes": report.class_counts})
        return report

    # =========================================================================
    #  Stages
    # =========================================================================

    def _stage_ingest(self, client: HttpClient, report: RunReport) -> list[RawRecord]:
        """
        Collect candidates from every source, landing raw payloads first.

        Raw records are committed before anything else happens to them. If the
        process dies during download, the expensive part — the API calls and
        the scrape — is not lost and can be replayed.
        """
        assert self.run_id
        with stage_context(Stage.INGEST.value):
            collected: list[RawRecord] = []
            rejections: list[Rejection] = []
            metrics: dict[str, int] = {}

            # How much each class still needs. Fetching is scoped to the gap
            # between what is stored and the target, so a class that is already
            # full costs nothing: no API call, no scrape, no download, no trim.
            # Without this the pipeline re-fetches a full batch every run and
            # discards it at the selection stage — correct, but it spends a
            # source's rate limit to arrive back where it started.
            stored = self.repo.class_counts()
            target = self.settings.sources.target_per_class

            for source in build_sources(client, self.settings.sources):
                source_total = 0
                skipped_classes = 0
                for class_label in self.settings.sources.classes:
                    still_needed = max(0, target - stored.get(class_label, 0))
                    if still_needed == 0:
                        skipped_classes += 1
                        log.info(
                            "class already at target, not fetching",
                            extra={"source": source.name.value, "class": class_label,
                                   "stored": stored.get(class_label, 0), "target": target},
                        )
                        continue

                    limit = self.settings.sources.fetch_limit(still_needed)
                    records = list(source.fetch(class_label, limit))
                    collected.extend(records)
                    source_total += len(records)
                    metrics[f"fetched_{source.name.value}"] = (
                        metrics.get(f"fetched_{source.name.value}", 0) + len(records)
                    )
                    log.info("source fetched",
                             extra={"source": source.name.value, "class": class_label,
                                    "records": len(records)})
                rejections.extend(source.drain_rejections())

                # A source that contributes nothing is a defect, not a quiet
                # non-event. The first real run produced a dataset sourced
                # entirely from the API because the scraper silently returned
                # zero, and nothing in the output said so. Always say so.
                #
                # Unless we never asked it for anything: on a re-run every class
                # is already at target, so zero is the correct and expected
                # answer and shouting about it would train the reader to ignore
                # the message that matters.
                all_classes_skipped = skipped_classes == len(self.settings.sources.classes)
                if source_total == 0 and not all_classes_skipped:
                    log.error(
                        "SOURCE CONTRIBUTED ZERO RECORDS — the dataset will not "
                        "include anything from this source",
                        extra={"source": source.name.value},
                    )
                metrics["sources_empty"] = metrics.get("sources_empty", 0) + (
                    1 if source_total == 0 and not all_classes_skipped else 0
                )
                metrics["classes_at_target"] = (
                    metrics.get("classes_at_target", 0) + skipped_classes
                )

            metrics["fetched"] = len(collected)
            self.repo.insert_raw_records(self.run_id, collected)
            self.repo.record_rejections(self.run_id, rejections)
            self.repo.add_metrics(self.run_id, Stage.INGEST.value, metrics)
            self.conn.commit()

            report.metrics[Stage.INGEST.value] = metrics
            return collected

    def _stage_download(
        self, records: list[RawRecord], client: HttpClient, report: RunReport
    ) -> list[Any]:
        assert self.run_id
        with stage_context(Stage.DOWNLOAD.value):
            result = download.run(
                records,
                client,
                self.settings,
                known_sha256=self.repo.known_sha256(),
                # The URL index is what actually saves the transfer; the hash
                # set only reports what turned out to be already known.
                known_urls=self.repo.known_origin_urls(),
                refetch=self.settings.refetch,
            )
            self._record(Stage.DOWNLOAD, result, report)
            return result.kept

    def _stage_validate(self, downloads: list[Any], report: RunReport) -> list[Any]:
        with stage_context(Stage.VALIDATE.value):
            result = validate.run(downloads, self.settings.validation)
            self._record(Stage.VALIDATE, result, report)
            return result.kept

    def _stage_dedupe(self, validated: list[Any], report: RunReport) -> list[Any]:
        with stage_context(Stage.DEDUPE.value):
            existing = self.repo.phash_index()
            result = dedupe.run(validated, existing, self.settings.dedupe)
            # Stored rows that an incoming higher-resolution copy has displaced.
            # Applied during persist, once the replacement row exists — the
            # duplicate_of foreign key requires it.
            self._supersessions = result.extras.get("supersessions", [])
            self._record(Stage.DEDUPE, result, report)
            return result.kept

    def _stage_normalize(self, deduped: list[Any], report: RunReport) -> list[ImageRecord]:
        with stage_context(Stage.NORMALIZE.value):
            result = normalize.run(deduped)
            self._record(Stage.NORMALIZE, result, report)
            return result.kept

    def _stage_select(
        self, records: list[ImageRecord], report: RunReport
    ) -> list[ImageRecord]:
        """
        Hold each class to its target size.

        Runs against the stored counts, not just this batch, so a re-run adds
        nothing to a class that is already at target instead of growing the
        dataset by one over-fetch per run.
        """
        with stage_context(Stage.SELECT.value):
            result = select.run(
                records,
                stored_counts=self.repo.class_counts(),
                stored_sha256=self.repo.known_sha256(),
                sources=self.settings.sources,
                split_settings=self.settings.split,
            )
            self._record(Stage.SELECT, result, report)
            return result.kept

    def _stage_persist(self, records: list[ImageRecord], report: RunReport) -> None:
        """
        Write the curated rows, then apply near-duplicate markings.

        Order matters: `duplicate_of` is a foreign key to `images.id`, so the
        image it points at must already exist. Doing this in two passes is what
        lets a duplicate and its original arrive in the same batch.
        """
        assert self.run_id
        with stage_context("persist"):
            stats, failures = self.repo.upsert_images(self.run_id, records)

            for record, error in failures:
                log.warning("upsert conflict",
                            extra={"sha256": record.sha256[:12], "error": error})

            marked = 0
            for record in records:
                if record.is_duplicate:
                    if self.repo.mark_duplicate(
                        record.sha256, record.duplicate_of_sha256, record.duplicate_distance
                    ):
                        marked += 1

            # Demote stored rows that a better copy has replaced. Done last,
            # because the replacement must already exist for the foreign key.
            # A marking can legitimately find nothing to do — the replacement
            # may have been trimmed at the class target — so a miss is counted
            # and logged rather than assumed impossible.
            superseded = 0
            skipped = 0
            for old_sha, new_sha, distance in getattr(self, "_supersessions", []):
                if self.repo.mark_duplicate(old_sha, new_sha, distance):
                    superseded += 1
                else:
                    skipped += 1
                    log.info("supersession skipped: replacement not stored",
                             extra={"stored": old_sha[:12], "replacement": new_sha[:12]})

            stats["kept"] = stats["inserted"] + stats["already_known"]
            stats["marked_duplicate"] = marked
            stats["superseded"] = superseded
            stats["supersessions_skipped"] = skipped
            self.repo.add_metrics(self.run_id, "persist", stats)
            self.conn.commit()
            report.metrics["persist"] = stats
            log.info("persisted", extra=stats)

    def _stage_split(self, report: RunReport) -> None:
        """
        Assign splits across the whole stored dataset, not just this batch.

        Batch-local splitting would drift from the target ratio a little more on
        every run. Because assignment is content-derived, re-deciding globally
        is cheap and stable — an image only moves if the set around it changed
        materially.
        """
        assert self.run_id
        with stage_context(Stage.SPLIT.value):
            candidates = self.repo.split_candidates()
            assignment = assign(
                (SplitCandidate(row["sha256"], row["class_label"]) for row in candidates),
                self.settings.split,
            )

            changed = 0
            for row in candidates:
                target = assignment.get(row["sha256"])
                if target is not None and row["split"] != target.value:
                    self.repo.set_split(row["sha256"], target)
                    changed += 1

            metrics = {
                "eligible": len(candidates),
                "changed": changed,
                Split.TRAIN.value: sum(1 for s in assignment.values() if s is Split.TRAIN),
                Split.VAL.value: sum(1 for s in assignment.values() if s is Split.VAL),
            }
            self.repo.add_metrics(self.run_id, Stage.SPLIT.value, metrics)
            self.conn.commit()
            report.metrics[Stage.SPLIT.value] = metrics
            log.info("split complete", extra=metrics)

    def _stage_export(self, report: RunReport) -> None:
        assert self.run_id
        with stage_context(Stage.EXPORT.value):
            rows = self.repo.dataset_rows()
            summary = export.run(
                rows=rows,
                output_dir=self.settings.paths.output_dir,
                run_id=self.run_id,
                config_snapshot=self.settings.snapshot(),
            )
            self.repo.add_metrics(
                self.run_id, Stage.EXPORT.value, {"exported": summary.get("records", 0)}
            )
            self.conn.commit()
            report.export_summary = summary

    # =========================================================================
    #  Helpers
    # =========================================================================

    def _record(self, stage: Stage, result: Any, report: RunReport) -> None:
        """Persist a stage's rejections and counters, then commit."""
        assert self.run_id
        self.repo.record_rejections(self.run_id, result.rejections)
        self.repo.add_metrics(self.run_id, stage.value, result.metrics)
        self.conn.commit()
        report.metrics[stage.value] = dict(result.metrics)

    def _evaluate_quality_gate(self, report: RunReport) -> RunStatus:
        """
        A run that quietly under-delivers a class is worse than one that fails
        loudly, because the shortfall surfaces later as a model that cannot
        recognise that class. Make it visible here.
        """
        minimum = self.settings.sources.min_per_class
        counts = self.repo.class_counts()
        short = {
            cls: counts.get(cls, 0)
            for cls in self.settings.sources.classes
            if counts.get(cls, 0) < minimum
        }
        if short:
            log.warning("quality gate: classes below minimum",
                        extra={"minimum": minimum, "short": short})
            return RunStatus.PARTIAL
        return RunStatus.SUCCESS

    def _rejection_breakdown(self) -> dict[str, int]:
        assert self.run_id
        return {
            f"{row['stage']}/{row['reason_code']}": row["n"]
            for row in self.repo.rejection_breakdown(self.run_id)
        }
