"""Tests for 06_isik.csv – person master table.

Only the business‑specific rules live here; all generic validation
helpers are imported from 'validation.helpers'.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from src.validation.helpers import (
    assert_fk,
    assert_unique_not_null,
    assert_temporal_order,
)
from src.generation.utils import get_kdid_for_name

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

_OUTPUT_FILE = Path(__file__).resolve().parents[2] / "output" / "06_isik.csv"
_DATE_COLS = [
    "IsSurmaaeg",
    "LoodiKpv",
    "MuudetiKpv",
    "KustutatiKpv",
    "isSynniaeg",
]
_FK_COLS = [
    "KdIDIsikuStaatus",
    "KdIDKirjeStaatus",
    "KdIDHaridus",
    "KdIDEmakeel",
    "KdIDRahvus",
    "KdIDKodakondsus",
    "KdIDPerekonnaseis",
    "KdIDPohjus",
]

# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------

@pytest.fixture(scope="module")
def isik_df() -> pd.DataFrame:
    """Load 06_isik.csv once for the module."""
    return pd.read_csv(_OUTPUT_FILE, parse_dates=_DATE_COLS, low_memory=False)

# ---------------------------------------------------------------------
# Generic constraints (PK, FK, dates)
# ---------------------------------------------------------------------

def test_pk_isik(isik_df: pd.DataFrame) -> None:
    """Primary key IsID must be unique and NOT NULL."""
    assert_unique_not_null(isik_df, "IsID", label="IsID (PK)")


def test_kodifikaator_fks(isik_df: pd.DataFrame, kodifikaator_df: pd.DataFrame) -> None:
    """All KdID‑typed columns reference valid codes in kodifikaator."""
    for col in _FK_COLS:
        if col in isik_df.columns:
            assert_fk(
                isik_df,
                col,
                kodifikaator_df,
                msg=f"{col} contains unknown KdIDs",
            )


def test_temporal_coherence(isik_df: pd.DataFrame) -> None:
    """If MuudetiKpv exists it must not precede LoodiKpv."""
    assert_temporal_order(isik_df, "LoodiKpv", "MuudetiKpv")

# ---------------------------------------------------------------------
# Business‑specific rules
# ---------------------------------------------------------------------

def test_surmaaeg_vs_status(isik_df: pd.DataFrame, kodifikaator_df: pd.DataFrame) -> None:
    """Status fields follow the SURNUD/ARHIIVIS vs ELUS/REGISTRIS rule."""
    kd = lambda name: get_kdid_for_name(kodifikaator_df, name)

    kd_surnud = kd("SURNUD")
    kd_elus = kd("ELUS")
    kd_arhiivis = kd("ARHIIVIS")
    kd_registris = kd("REGISTRIS")

    if None in (kd_surnud, kd_elus, kd_arhiivis, kd_registris):
        pytest.skip("Required status codes missing in kodifikaator.")

    deceased = isik_df[isik_df["IsSurmaaeg"].notna()]
    living = isik_df[isik_df["IsSurmaaeg"].isna()]

    assert (
        (deceased["KdIDIsikuStaatus"] == kd_surnud)
    ).all() and (
        (deceased["KdIDKirjeStaatus"] == kd_arhiivis).all()
    ), "Deceased persons must be SURNUD/ARHIIVIS"

    assert (
        (living["KdIDIsikuStaatus"] == kd_elus).all()
    ) and (
        (living["KdIDKirjeStaatus"] == kd_registris).all()
    ), "Living persons must be ELUS/REGISTRIS"


def test_isikukood_format(isik_df: pd.DataFrame) -> None:
    """IsIsikukood is required and must consist of 11–13 digits."""
    pattern = re.compile(r"^[0-9]{11,13}$")

    assert isik_df["IsIsikukood"].notna().all(), "IsIsikukood NULL values found"

    bad = isik_df.loc[~isik_df["IsIsikukood"].astype(str).str.match(pattern)]
    assert bad.empty, f"Invalid IsIsikukood format: {bad[['IsID','IsIsikukood']]}"


def test_kov_fk_when_arrival(isik_df: pd.DataFrame) -> None:
    """If IsSaabusEesti is filled then AKpID must also be present."""
    if not {"IsSaabusEesti", "AKpID"}.issubset(isik_df.columns):
        pytest.skip("IsSaabusEesti/AKpID columns not found – rule not applicable.")

    problem = isik_df[isik_df["IsSaabusEesti"].notna() & isik_df["AKpID"].isna()]
    assert problem.empty, "IsSaabusEesti filled but AKpID missing for some rows"
