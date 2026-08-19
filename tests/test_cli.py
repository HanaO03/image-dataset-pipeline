"""
The CLI's help screens must render.

This file exists because of a defect nothing else could have caught. The suite
imports modules and calls functions; CI runs lint and pytest; neither ever asks
typer to *draw* anything. So an incompatibility between the pinned typer and an
unpinned transitive click sat in the shipped Docker image, and the first command
a reviewer runs after `docker compose up` — `--help` — died with a TypeError
while `docker compose up` itself kept working perfectly.

`requirements.txt` pins click and rich for that reason. These tests are what
turn the pin from a comment into something that fails loudly if it is undone.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from src.cli import app

runner = CliRunner()


def test_the_root_help_screen_renders():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0, f"--help crashed: {result.exception!r}"
    assert "Image dataset ingestion pipeline" in result.output


@pytest.mark.parametrize("command", ["run", "status", "debug-scrape", "init-db"])
def test_every_subcommand_help_screen_renders(command):
    result = runner.invoke(app, [command, "--help"])
    assert result.exit_code == 0, f"{command} --help crashed: {result.exception!r}"


def test_click_stays_below_the_version_that_breaks_typer():
    """
    typer 0.15.x calls `Parameter.make_metavar()` with click's pre-8.2
    signature. Pinning is the fix; this is the tripwire if the pin is loosened.
    """
    import click

    major, minor = (int(part) for part in click.__version__.split(".")[:2])
    assert (major, minor) < (8, 2), (
        f"click {click.__version__} removes the signature typer 0.15.x relies on"
    )
