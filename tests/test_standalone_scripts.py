"""Pytest shim that runs the standalone test scripts in this directory.

The other ``test_*.py`` files here are written as runnable programs — they call
``sys.exit(1)`` on failure rather than exposing ``test_*`` functions — so a plain
``pytest`` run collects nothing from them and silently reports success. This shim
parametrizes over those scripts, runs each as a subprocess, and asserts a clean
exit, so CI (and ``pytest``) actually exercise them.

Run:  pytest tests/            (or)   python tests/<script>.py
"""

import subprocess
import sys
from pathlib import Path

import pytest

_TESTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _TESTS_DIR.parent
_SELF = Path(__file__).name

# Every standalone test script except this shim.
_SCRIPTS = sorted(
    p.name for p in _TESTS_DIR.glob("test_*.py") if p.name != _SELF
)


@pytest.mark.parametrize("script", _SCRIPTS)
def test_standalone_script(script):
    result = subprocess.run(
        [sys.executable, str(_TESTS_DIR / script)],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, (
        f"{script} failed (exit {result.returncode})\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
