"""
Structured logging.

Every line is JSON and carries `run_id` and `stage` automatically, injected via
a contextvar rather than passed as an argument through every function. Two
reasons this matters more than it looks:

* Debugging a pipeline means asking "what happened in stage X of run Y" —
  that question is only cheap if run_id and stage are fields, not prose.
* A contextvar keeps the pipeline stages free of logging plumbing. A stage
  signature stays `(records, ctx) -> StageResult`; it does not grow a
  `logger` parameter it would only pass down.

Set LOG_JSON=false for human-readable output while developing.
"""

from __future__ import annotations

import json
import logging
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

_run_id: ContextVar[str | None] = ContextVar("run_id", default=None)
_stage: ContextVar[str | None] = ContextVar("stage", default=None)

# Keys already present on every LogRecord; anything else the caller passed via
# `extra=` is ours and belongs in the JSON output.
_RESERVED = set(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if (run_id := _run_id.get()) is not None:
            payload["run_id"] = run_id
        if (stage := _stage.get()) is not None:
            payload["stage"] = stage
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class HumanFormatter(logging.Formatter):
    """Readable local-dev output; the context still shows up as a prefix."""

    def format(self, record: logging.LogRecord) -> str:
        stage = _stage.get()
        prefix = f"[{stage}] " if stage else ""
        extras = " ".join(
            f"{k}={v}" for k, v in record.__dict__.items() if k not in _RESERVED
        )
        base = f"{self.formatTime(record, '%H:%M:%S')} {record.levelname:<7} {prefix}{record.getMessage()}"
        return f"{base}  {extras}" if extras else base


def configure_logging(level: str = "INFO", as_json: bool = True) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if as_json else HumanFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    # These two are chatty at INFO and drown out our own lines.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)


def set_run_id(run_id: str) -> None:
    _run_id.set(run_id)


@contextmanager
def stage_context(stage: str) -> Iterator[None]:
    """Tag every log line emitted inside the block with the stage name."""
    token = _stage.set(stage)
    try:
        yield
    finally:
        _stage.reset(token)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
