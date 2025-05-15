"""Tests for 01_aadress.csv and 02_aadresskomponent.csv.

Only table‑specific business rules live here – generic FK/PK/date helpers
come from 'validation.helpers'.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.validation.helpers import (
    assert_fk,
    assert_temporal_order,
)
from src.generation.utils import get_kdid_for_name

# ---------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------
OUTPUT = Path("output")
DATA = Path("data")

DATE_COLS_ADR = ["LoodiKpv", "MuudetiKpv", "KustutatiKpv"]
DATE_COLS_AKP = [
    "AKpKehtivAlatesKpv",
    "AKpKehtivKuniKpv",
    "LoodiKpv",
    "MuudetiKpv",
    "KustutatiKpv",
]


# ---------------------------------------------------------------------
# Fixtures (module scope)
# ---------------------------------------------------------------------

@pytest.fixture(scope="module")
def aadress_df() -> pd.DataFrame:  # 01
    return pd.read_csv(
        OUTPUT / "01_aadress.csv",
        parse_dates=DATE_COLS_ADR,
        encoding="ISO-8859-1",
    )


@pytest.fixture(scope="module")
def aadress_komp_df() -> pd.DataFrame:  # 02
    return pd.read_csv(
        OUTPUT / "02_aadresskomponent.csv",
        parse_dates=DATE_COLS_AKP,
        encoding="ISO-8859-1",
        low_memory=False,
    )


# ---------------------------------------------------------------------
# 01_aadress.csv tests
# ---------------------------------------------------------------------

def test_aadress_fk_status(aadress_df, kodifikaator_df):
    """'KdIDAdressiStaatus' must reference a valid KdID."""
    assert_fk(
        aadress_df,
        "KdIDAdressiStaatus",
        kodifikaator_df,
        msg="Unknown KdIDAdressiStaatus values:",
    )


def test_aadress_zipcode_range(aadress_df):
    """'AdrSihtnumber' must be within 10000‑99999 (inclusive)."""
    if "AdrSihtnumber" not in aadress_df:
        pytest.skip("AdrSihtnumber column missing")

    bad = aadress_df[(aadress_df["AdrSihtnumber"] < 10000) | (aadress_df["AdrSihtnumber"] > 99999)]
    assert bad.empty, f"Out‑of‑range zipcodes:\n{bad[['AdrID', 'AdrSihtnumber']]}"


def test_aadress_date_chain(aadress_df):
    """Temporal coherence: LoodiKpv <= MuudetiKpv <= KustutatiKpv (if cols present)."""
    if "MuudetiKpv" in aadress_df:
        assert_temporal_order(aadress_df, "LoodiKpv", "MuudetiKpv")
    if "KustutatiKpv" in aadress_df:
        assert_temporal_order(aadress_df, "MuudetiKpv", "KustutatiKpv")


@pytest.mark.parametrize("status_code,expected", [("KEHTETU", True)])
def test_aadress_kustutatud_on_kehtetu(aadress_df, kodifikaator_df, status_code, expected):
    """If KustutatiKpv is filled the row must have status KEHTETU."""
    kd_kehtetu = get_kdid_for_name(kodifikaator_df, status_code)
    if kd_kehtetu is None or "KustutatiKpv" not in aadress_df:
        pytest.skip("Missing code or column")

    with_kust = aadress_df[aadress_df["KustutatiKpv"].notna()]
    problem = with_kust[with_kust["KdIDAdressiStaatus"] != kd_kehtetu]
    assert problem.empty, (
        "Rows logically deleted must carry KEHTETU status, but found:"
        f"\n{problem[['AdrID', 'KdIDAdressiStaatus', 'KustutatiKpv']]}"
    )


# ---------------------------------------------------------------------
# 02_aadresskomponent.csv tests
# ---------------------------------------------------------------------

def test_aadresskomp_status_fk(aadress_komp_df, kodifikaator_df):
    assert_fk(
        aadress_komp_df,
        "KdIDStaatus",
        kodifikaator_df,
        msg="Unknown KdIDStaatus in aadresskomponent:",
    )


def test_aadresskomp_temporal_ranges(aadress_komp_df):
    """Ensure date ranges make sense and creation precedes validity start."""
    assert_temporal_order(aadress_komp_df, "AKpKehtivAlatesKpv", "AKpKehtivKuniKpv")
    assert_temporal_order(aadress_komp_df, "LoodiKpv", "AKpKehtivAlatesKpv")


def test_aadresskomp_kehtiv_vs_kehtetu(aadress_komp_df, kodifikaator_df):
    """Closed components (KuniKpv filled) must be KEHTETU."""
    kd_kehtetu = get_kdid_for_name(kodifikaator_df, "KEHTETU")
    if kd_kehtetu is None:
        pytest.skip("KEHTETU code not in kodifikaator")

    closed = aadress_komp_df[aadress_komp_df["AKpKehtivKuniKpv"].notna()]
    bad = closed[closed["KdIDStaatus"] != kd_kehtetu]
    assert bad.empty, f"Closed components must be KEHTETU, but:\n{bad[['AKpID', 'KdIDStaatus']]}"


def test_aadresskomp_required_columns(aadress_komp_df):
    """Spot‑check a handful of columns that generator promises."""
    expected = [
        "IAsIDLooja",
        "IAsIDMuutja",
        "MuudetiKpv",
        "KdIDAkpLiik",
    ]
    missing = [c for c in expected if c not in aadress_komp_df.columns]
    assert not missing, f"Missing expected columns: {missing}"
