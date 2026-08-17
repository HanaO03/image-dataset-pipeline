"""
Command-line entry point.

Thin by design: parse arguments, wire dependencies, hand off to PipelineRunner,
print a report. No business logic lives here — if it did, it would be
unreachable from tests and from any future orchestrator.

    python -m src.cli run                     # full pipeline
    python -m src.cli run --classes cat,dog   # override classes
    python -m src.cli run --limit 10          # small smoke run
    python -m src.cli status                  # last runs at a glance
    python -m src.cli init-db                 # apply the schema and exit

Exit codes are meaningful, because `docker compose up` and CI both read them:
    0  success
    1  failed (infrastructure fault)
    2  partial (completed, but a quality gate was not met)
"""

from __future__ import annotations

import sys
from typing import Optional

import typer

from .config import get_settings
from .db.connection import apply_schema, connect
from .db.repository import Repository
from .logging_setup import configure_logging, get_logger
from .models import RunStatus
from .pipeline.run import PipelineRunner

app = typer.Typer(add_completion=False, help="Image dataset ingestion pipeline")
log = get_logger(__name__)

EXIT_OK, EXIT_FAILED, EXIT_PARTIAL = 0, 1, 2


def _bootstrap():
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    for directory in (settings.paths.raw_dir, settings.paths.images_dir,
                      settings.paths.output_dir):
        directory.mkdir(parents=True, exist_ok=True)
    conn = connect(settings.database)
    apply_schema(conn)
    return settings, conn


@app.command()
def run(
    classes: Optional[str] = typer.Option(
        None, help="Comma-separated class list, overriding config."
    ),
    limit: Optional[int] = typer.Option(
        None, help="Target images per class. Lower this for a quick smoke run."
    ),
    skip_scrape: bool = typer.Option(
        False, help="Use only the API source. Useful when Commons is slow."
    ),
    refetch: bool = typer.Option(
        False,
        help="Re-download images already in the store instead of reusing them.",
    ),
) -> None:
    """Run the full pipeline end to end."""
    settings, conn = _bootstrap()

    # CLI overrides beat env beats defaults — the conventional precedence.
    if classes:
        parsed = tuple(c.strip().lower() for c in classes.split(",") if c.strip())
        settings.sources.classes = parsed
    if limit is not None:
        settings.sources.target_per_class = limit
        settings.sources.min_per_class = min(settings.sources.min_per_class, limit)
    if skip_scrape:
        settings.sources.commons_max_images_per_class = 0
    if refetch:
        settings.refetch = True

    try:
        report = PipelineRunner(conn, settings).execute()
    finally:
        conn.close()

    _print_report(report)

    if report.status is RunStatus.FAILED:
        raise typer.Exit(EXIT_FAILED)
    if report.status is RunStatus.PARTIAL:
        raise typer.Exit(EXIT_PARTIAL)
    raise typer.Exit(EXIT_OK)


@app.command("debug-scrape")
def debug_scrape(
    class_label: str = typer.Option("cat", help="Which class to trace."),
    limit: int = typer.Option(5, help="How many file pages to open."),
) -> None:
    """
    Trace the Commons scraper step by step, without touching the database.

    Exists because "the scraper returned nothing" is not a debuggable message.
    This prints what it saw at every stage — category page reached, files found
    directly, subcategories visited, and for each file page the image URL and
    licence it managed to extract — so a failure points at the step that broke.
    """
    from .config import get_settings
    from .http.client import HttpClient
    from .sources.wikimedia import WikimediaCommonsSource

    settings = get_settings()
    configure_logging("DEBUG", as_json=False)

    typer.echo(f"\ntracing class={class_label!r} "
               f"category={settings.sources.commons_categories.get(class_label)!r}\n")

    with HttpClient(settings.http) as client:
        source = WikimediaCommonsSource(client, settings.sources)
        records = list(source.fetch(class_label, limit))

        typer.echo(f"\n{'=' * 70}\nRECORDS PARSED: {len(records)}\n{'=' * 70}")
        for record in records:
            typer.echo(f"  {record.title[:55]:<57} {record.license_raw}")
            typer.echo(f"    -> {record.image_url[:100]}")

        rejections = source.drain_rejections()
        typer.echo(f"\nREJECTIONS: {len(rejections)}")
        for rejection in rejections[:15]:
            typer.echo(f"  {rejection.reason_code.value:<22} {rejection.detail}")

    if not records:
        typer.echo(
            "\nNo records. Check, in order: the category page was reachable, "
            "it contains media files (broad categories often do not), and the "
            "file pages expose a Creative Commons deed link.\n"
        )
        raise typer.Exit(1)
    typer.echo("")


@app.command("init-db")
def init_db() -> None:
    """Apply the schema and exit. Handy for inspecting the database alone."""
    _, conn = _bootstrap()
    conn.close()
    typer.echo("schema applied")


@app.command()
def status(limit: int = typer.Option(5, help="How many recent runs to show.")) -> None:
    """Print recent runs and the current dataset composition."""
    settings, conn = _bootstrap()
    repo = Repository(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM run_summary ORDER BY started_at DESC LIMIT %s", (limit,)
            )
            runs = cur.fetchall()

        typer.echo("\nRECENT RUNS")
        typer.echo("-" * 78)
        for row in runs:
            typer.echo(
                f"{str(row['run_id'])[:8]}  {row['status']:<8}"
                f"  fetched={row['fetched']:<4} kept={row['kept']:<4}"
                f"  rejected={row['rejected']:<4}"
                f"  reject_rate={row['rejection_rate_pct'] or 0}%"
                f"  {row['duration_seconds']}s"
            )

        typer.echo("\nDATASET COMPOSITION")
        typer.echo("-" * 78)
        for row in repo.dataset_composition():
            typer.echo(
                f"{row['class_label']:<10} {row['split']:<6} n={row['n_images']:<5}"
                f" avg={int(row['avg_width'])}x{int(row['avg_height'])}"
                f" sources={row['n_sources']}"
            )
        typer.echo("")
    finally:
        conn.close()


def _print_report(report) -> None:
    """
    Human-readable summary on stderr, so stdout stays parseable JSON logs.

    The rejection breakdown is printed unconditionally, including when the run
    succeeded. A run that quietly discarded 40% of what it fetched is not
    healthy, and the only way anyone notices is if the number is in their face.
    """
    out = sys.stderr
    print("\n" + "=" * 78, file=out)
    print(f"RUN {report.run_id}   status={report.status.value.upper()}", file=out)
    print("=" * 78, file=out)

    for stage, metrics in report.metrics.items():
        if metrics:
            rendered = "  ".join(f"{k}={v}" for k, v in sorted(metrics.items()))
            print(f"  {stage:<12} {rendered}", file=out)

    if report.class_counts:
        print("\n  kept per class:", file=out)
        for class_label, count in sorted(report.class_counts.items()):
            print(f"    {class_label:<10} {count}", file=out)

    if report.rejections_by_reason:
        print("\n  rejections:", file=out)
        for reason, count in sorted(
            report.rejections_by_reason.items(), key=lambda kv: -kv[1]
        ):
            print(f"    {reason:<45} {count}", file=out)

    if report.export_summary:
        summary = report.export_summary
        print(
            f"\n  exported {summary.get('records', 0)} records"
            f"  fingerprint={summary.get('fingerprint')}",
            file=out,
        )

    if report.error:
        print(f"\n  ERROR: {report.error}", file=out)
    print("=" * 78 + "\n", file=out)


if __name__ == "__main__":
    app()
