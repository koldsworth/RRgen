"""
Session‑wide fixtures shared by all table‑specific test suites.
"""

from __future__ import annotations

import pathlib
from datetime import datetime

import pandas as pd
import pytest

# ---------------------------------------------------------------------
# Locate the repository root (directory that contains data/kodifikaator.csv)
# ---------------------------------------------------------------------

def _find_repo_root(start: pathlib.Path) -> pathlib.Path:
    for path in [start, *start.parents]:
        if (path / "data" / "kodifikaator.csv").exists():
            return path
    raise FileNotFoundError(
        "Could not locate 'data/kodifikaator.csv' from any parent of "
        f"{start}. Ensure you run pytest inside the RRgen project directory."
    )


TESTS_DIR = pathlib.Path(__file__).resolve().parent
ROOT_DIR = _find_repo_root(TESTS_DIR)

DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "output"

# ---------------------------------------------------------------------
# Session‑scoped fixtures & helpers
# ---------------------------------------------------------------------

@pytest.fixture(scope="session")
def kodifikaator_df() -> pd.DataFrame:
    """Load the master code table exactly once for the whole test run."""
    return pd.read_csv(DATA_DIR / "kodifikaator.csv", encoding="ISO-8859-1")

@pytest.fixture(scope="session")
def conftest_kodifikaator_df(kodifikaator_df):
    """Backward-compat alias used by test_isikudokument.py"""
    return kodifikaator_df


@pytest.fixture(scope="session")
def now_ts() -> datetime:
    """Return a fixed now timestamp to keep tests deterministic."""
    return datetime.now()


# Convenience loader for CSVs in output

def _load_csv(name: str, *, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = OUTPUT_DIR / name
    return pd.read_csv(path, parse_dates=parse_dates, encoding="ISO-8859-1")


__all__ = [
    "kodifikaator_df",
    "now_ts",
    "DATA_DIR",
    "OUTPUT_DIR",
    "_load_csv",
]