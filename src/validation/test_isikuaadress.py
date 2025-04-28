"""Tests for 05_isikuaadress.csv – person‑address history.

This file focuses on rules that are specific to the address‑history
logic; generic FK/PK/date helpers come from 'validation.helpers'.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from datetime import datetime

from src.validation.helpers import (
    assert_unique_not_null,
    assert_temporal_order,
    assert_single_active,
    assert_no_overlap,
)
from src.generation.utils import get_kdid_for_name

OUT = Path("output")

# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------

@pytest.fixture(scope="module")
def isikuaadress_df() -> pd.DataFrame:  # noqa: D401 – simple fixture
    """Load 05_isikuaadress.csv with parsed date columns."""
    date_cols = [
        "IAdrKehtibAlatesKpv",
        "IAdrKehtibKuniKpv",
        "LoodiKpv",
        "MuudetiKpv",
        "KustutatiKpv",
    ]
    return pd.read_csv(OUT / "05_isikuaadress.csv", parse_dates=date_cols, encoding="ISO-8859-1")


@pytest.fixture(scope="module")
def aadress_df() -> pd.DataFrame:  # noqa: D401
    """Load 01_aadress.csv for AdrID foreign‑key checks."""
    return pd.read_csv(OUT / "01_aadress.csv", usecols=["AdrID"], encoding="ISO-8859-1")


# ---------------------------------------------------------------------
# PK / FK validations
# ---------------------------------------------------------------------

def test_primary_key(isikuaadress_df: pd.DataFrame):
    assert_unique_not_null(isikuaadress_df, "IAdrID", label="IAdrID (PK)")


def test_fk_adrid(isikuaadress_df: pd.DataFrame, aadress_df: pd.DataFrame):
    """Every AdrID in the link table must exist in 01_aadress.csv."""
    used = set(isikuaadress_df["AdrID"].dropna())
    valid = set(aadress_df["AdrID"].dropna())
    missing = used - valid
    assert not missing, f"AdrID values missing in aadress table: {missing}"


# ---------------------------------------------------------------------
# Temporal coherence
# ---------------------------------------------------------------------

def test_start_before_end(isikuaadress_df: pd.DataFrame):
    assert_temporal_order(isikuaadress_df, "IAdrKehtibAlatesKpv", "IAdrKehtibKuniKpv")


# ---------------------------------------------------------------------
# Business‑specific rules
# ---------------------------------------------------------------------

def test_status_matches_period_end(isikuaadress_df: pd.DataFrame, kodifikaator_df: pd.DataFrame):
    kd_kehtiv = get_kdid_for_name(kodifikaator_df, "KEHTIV")
    kd_kehtetu = get_kdid_for_name(kodifikaator_df, "KEHTETU")
    kd_elukoht = get_kdid_for_name(kodifikaator_df, "ELUKOHT")
    kd_endine = get_kdid_for_name(kodifikaator_df, "ENDINE ELUKOHT")

    # open periods → KEHTIV / ELUKOHT
    open_rows = isikuaadress_df[isikuaadress_df["IAdrKehtibKuniKpv"].isna()]
    bad_open = open_rows[(open_rows["KdIDAadressiStaatus"] != kd_kehtiv) | (open_rows["KdIDAadressiLiik"] != kd_elukoht)]
    assert bad_open.empty, "Open periods must be KEHTIV & ELUKOHT, but found: " + str(bad_open)

    # closed periods → KEHTETU / ENDINE ELUKOHT
    closed_rows = isikuaadress_df[isikuaadress_df["IAdrKehtibKuniKpv"].notna()]
    bad_closed = closed_rows[(closed_rows["KdIDAadressiStaatus"] != kd_kehtetu) | (closed_rows["KdIDAadressiLiik"] != kd_endine)]
    assert bad_closed.empty, "Closed periods must be KEHTETU & ENDINE ELUKOHT, but found: " + str(bad_closed)


def test_single_active_residence(isikuaadress_df: pd.DataFrame, kodifikaator_df: pd.DataFrame):
    kd_kehtiv = get_kdid_for_name(kodifikaator_df, "KEHTIV")
    kd_elukoht = get_kdid_for_name(kodifikaator_df, "ELUKOHT")
    active = isikuaadress_df[(isikuaadress_df["KdIDAadressiStaatus"] == kd_kehtiv) & (isikuaadress_df["KdIDAadressiLiik"] == kd_elukoht)]
    assert_single_active(active, id_col="IsID")


def test_no_overlapping_periods(isikuaadress_df: pd.DataFrame):
    assert_no_overlap(
        isikuaadress_df,
        id_col="IsID",
        start_col="IAdrKehtibAlatesKpv",
        end_col="IAdrKehtibKuniKpv",
    )

def test_kehtiv_residence_not_in_future(isikuaadress_df: pd.DataFrame, kodifikaator_df: pd.DataFrame):
    kd_kehtiv = get_kdid_for_name(kodifikaator_df, "KEHTIV")

    # Only KEHTIV rows whose start date is later than now
    future_rows = isikuaadress_df[
        (isikuaadress_df["KdIDAadressiStaatus"] == kd_kehtiv)
        & (isikuaadress_df["IAdrKehtibAlatesKpv"] > datetime.now())
    ]
    assert future_rows.empty, (
        "Found KEHTIV residence entries with a start date in the future:\n"
        f"{future_rows[['IsID', 'IAdrID', 'IAdrKehtibAlatesKpv']].head()}"
    )


# ---------------------------------------------------------------------
# Audit‑field logic
# ---------------------------------------------------------------------

def test_deleted_rows_have_kehtetu_status(isikuaadress_df: pd.DataFrame, kodifikaator_df: pd.DataFrame):
    kd_kehtetu = get_kdid_for_name(kodifikaator_df, "KEHTETU")
    deleted = isikuaadress_df[isikuaadress_df["KustutatiKpv"].notna()]
    bad = deleted[deleted["KdIDAadressiStaatus"] != kd_kehtetu]
    assert bad.empty, "Rows with KustutatiKpv must have KEHTETU status: " + str(bad)


def test_modified_before_deleted(isikuaadress_df: pd.DataFrame):
    deleted = isikuaadress_df[isikuaadress_df["KustutatiKpv"].notna()]
    bad = deleted[(deleted["MuudetiKpv"].notna()) & (deleted["MuudetiKpv"] > deleted["KustutatiKpv"])]
    assert bad.empty, "MuudetiKpv must be ≤ KustutatiKpv on deleted rows: " + str(bad)
