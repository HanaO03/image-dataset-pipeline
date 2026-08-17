"""
Database connectivity and schema bootstrap.

Two responsibilities, both about making `docker compose up` reliable:

1. **Wait for Postgres.** A compose healthcheck is necessary but not
   sufficient: Postgres accepts TCP connections during startup while still
   rejecting queries, and `pg_isready` can pass a moment before the database
   is genuinely usable. Retrying here closes the remaining gap, and a first
   run that fails intermittently reads as a broken submission.

2. **Apply the schema idempotently, from the application.**
   `docker-entrypoint-initdb.d` only executes when the PGDATA volume is empty,
   so a project relying on it alone works exactly once and then breaks on every
   subsequent `docker compose up` against the existing volume. Applying the DDL
   from the app makes both the first and the hundredth run behave identically.
   In production this would be Alembic; one idempotent DDL file is the right
   weight for this scope.
"""

from __future__ import annotations

import time
from pathlib import Path

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from ..config import DatabaseSettings
from ..logging_setup import get_logger

log = get_logger(__name__)

#: Bundled into the image at /app/sql/schema.sql; falls back to the repo layout
#: when running outside the container.
_SCHEMA_CANDIDATES = (
    Path("/app/sql/schema.sql"),
    Path(__file__).resolve().parents[2] / "sql" / "schema.sql",
)


def _schema_path() -> Path:
    for candidate in _SCHEMA_CANDIDATES:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"schema.sql not found in any of: {[str(p) for p in _SCHEMA_CANDIDATES]}"
    )


def connect(settings: DatabaseSettings) -> Connection:
    """
    Open a connection, retrying while Postgres finishes starting up.

    Fails fast and loudly once the retry budget is exhausted: an unreachable
    database is an infrastructure fault, and the run must not limp on.
    """
    last_error: Exception | None = None
    for attempt in range(1, settings.connect_max_retries + 1):
        try:
            conn = psycopg.connect(
                settings.dsn, row_factory=dict_row, connect_timeout=5
            )
            # Prove the server actually answers queries, not just that the
            # socket accepted a connection — Postgres does the latter while
            # still starting up.
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            # psycopg3 opens an implicit transaction on the first statement,
            # so the probe leaves the connection INTRANS. Roll it back to hand
            # the caller a clean, idle connection; without this the very first
            # thing the runner does is inherit a stray open transaction.
            conn.rollback()
            if attempt > 1:
                log.info("database ready", extra={"attempts": attempt})
            return conn
        except psycopg.OperationalError as exc:
            last_error = exc
            log.debug(
                "database not ready yet",
                extra={"attempt": attempt, "max": settings.connect_max_retries},
            )
            time.sleep(settings.connect_retry_delay_seconds)

    raise RuntimeError(
        f"could not connect to {settings.safe_dsn()} after "
        f"{settings.connect_max_retries} attempts: {last_error}"
    ) from last_error


def apply_schema(conn: Connection, lock_timeout: str = "10s") -> None:
    """
    Apply sql/schema.sql. Safe to call on every startup, including a hot DB.

    `lock_timeout` is the important part. DDL can require an ACCESS EXCLUSIVE
    lock, and without a timeout a blocked statement waits *forever* — a run
    that appears to have frozen, with nothing in the logs to explain it. That
    is a genuinely nasty production incident and it costs one line to prevent.
    With the timeout, the same situation fails in seconds with a message that
    names the actual problem.
    """
    path = _schema_path()
    ddl = path.read_text(encoding="utf-8")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET lock_timeout = '{lock_timeout}'")
            cur.execute(ddl)
        conn.commit()
    except psycopg.errors.LockNotAvailable as exc:
        conn.rollback()
        raise RuntimeError(
            "schema migration could not acquire a lock within "
            f"{lock_timeout}. Another session is holding a lock on the pipeline "
            "tables — a concurrent pipeline container, or an idle psql "
            "transaction. Close it and retry."
        ) from exc
    except Exception:
        conn.rollback()
        raise
    finally:
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = 0")
        conn.commit()
    log.info("schema applied", extra={"schema_file": str(path)})
