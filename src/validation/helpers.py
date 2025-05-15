"""
Shared pytest helper utilities and generic fixtures.
These helpers centralise validations that recur across multiple table‐
specific test suites, so the individual test files can stay short and
focus on table‑specific business rules.
"""

from __future__ import annotations

import pandas as pd
import pytest


# ------------------------------------------------------------------
# FK / PK helpers
# ------------------------------------------------------------------

def assert_unique_not_null(df: pd.DataFrame, col: str, *, label: str | None = None):
    """Assert col in df has no NULLs and no duplicates."""
    label = label or col
    assert df[col].notna().all(), f"{label} contains NULL values"
    dups = df[df[col].duplicated()]
    assert dups.empty, f"{label} duplicates: {dups[[col]].head()}"


def assert_fk(
        df: pd.DataFrame,
        col: str,
        ref_df: pd.DataFrame,
        ref_col: str = "KdID",
        *,
        msg: str | None = None,
):
    """Foreign‑key check: every value in df[col] must exist in ref_df[ref_col]."""
    used = set(df[col].dropna())
    valid = set(ref_df[ref_col].dropna())
    diff = used - valid
    assert not diff, msg or f"{col} unknown codes: {diff}"


# ------------------------------------------------------------------
# Temporal helpers
# ------------------------------------------------------------------

def assert_temporal_order(
        df: pd.DataFrame,
        earlier: str,
        later: str,
        *,
        allow_equal: bool = True,
        msg: str | None = None,
):
    """Assert earlier ≤ later (if both present)."""
    op = (df[earlier] > df[later]) if allow_equal else (df[earlier] >= df[later])
    bad = df[df[later].notna() & df[earlier].notna() & op]
    assert bad.empty, msg or f"{earlier}>{later} rows: {bad[[earlier, later]].head()}"


# ------------------------------------------------------------------
# Active / overlap helpers
# ------------------------------------------------------------------

def assert_single_active(
        df: pd.DataFrame,
        id_col: str,
        *,
        status_col: str | None = None,
        active_code: int | str | None = None,
):
    """Ensure at most one active row per entity.

    If status_col is None, df is assumed to be pre‑filtered to active rows.
    """
    if status_col is not None and active_code is not None:
        active_df = df[df[status_col] == active_code]
    else:
        active_df = df
    counts = active_df.groupby(id_col).size()
    multiples = counts[counts > 1]
    assert multiples.empty, f"Multiple active rows per {id_col}: {multiples}"


def assert_no_overlap(
        df: pd.DataFrame,
        id_col: str,
        start_col: str,
        end_col: str,
):
    """Ensure no overlapping periods per entity."""
    for _id, grp in df.groupby(id_col):
        ordered = grp.sort_values(start_col)
        prev_end = None
        for _, row in ordered.iterrows():
            st = row[start_col]
            en = row[end_col]
            if prev_end is not None and pd.notna(en) and prev_end > st:
                pytest.fail(f"Overlap in {id_col}={_id}: {prev_end} > {st}")
            prev_end = en if pd.notna(en) else prev_end


__all__ = [
    "assert_unique_not_null",
    "assert_fk",
    "assert_temporal_order",
    "assert_single_active",
    "assert_no_overlap",
]
