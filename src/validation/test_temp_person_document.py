"""Tests for 09_dokument.csv produced by the temporary‑generation
'generate_person_document' helper.

The file may or may not contain an 'IsID' column (depending on whether it
was dropped later).  FK tests are therefore conditional.

Generic checks (PK, temporal order, etc.) reuse helpers from
'src.validation.helpers'; this module keeps only the document‑specific rules.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.validation.helpers import (
    assert_unique_not_null,
    assert_temporal_order,
)
from src.generation.utils import get_kdid_for_name

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path("output")
DATA_DIR = Path("data")

DATE_COLS = [
    "DokValjaantudKpv",
    "DokKehtivKuniKpv",
    "DokKehtetuAlatesKpv",
    "DokKehtivAlates",
    "LoodiKpv",
    "MuudetiKpv",
    "KustutatiKpv",
]


@pytest.fixture(scope="module")
def dokument_df() -> pd.DataFrame:
    csv_path = OUTPUT_DIR / "09_dokument.csv"
    return pd.read_csv(csv_path, parse_dates=DATE_COLS, encoding="ISO-8859-1")


@pytest.fixture(scope="module")
def kodifikaator_df() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "kodifikaator.csv", encoding="ISO-8859-1")


@pytest.fixture(scope="module")
def isik_df() -> pd.DataFrame | None:
    csv_path = OUTPUT_DIR / "06_isik.csv"
    if not csv_path.exists():
        pytest.skip("06_isik.csv missing – skipping IsID FK checks.")
    return pd.read_csv(csv_path, encoding="ISO-8859-1")


# ---------------------------------------------------------------------------
# Column presence & PK
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = [
    "DokID",
    "DokNumber",
    "DokSeeria",
    "DokValjaantudKpv",
    "DokKehtivAlates",
    "KdIDDokumendiStaatus",
    "KdIDRiik",
    "LoodiKpv",
    "MuudetiKpv",
    "KustutatiKpv",
]


def test_required_columns_present(dokument_df: pd.DataFrame):
    missing = [c for c in REQUIRED_COLUMNS if c not in dokument_df.columns]
    assert not missing, f"Missing expected columns: {missing}"


def test_dokid_unique(dokument_df: pd.DataFrame):
    """Primary‑key constraint: DokID unique & not‑null."""
    assert_unique_not_null(dokument_df, "DokID", label="DokID")

# ---------------------------------------------------------------------------
# Status‑vs‑dates business rules
# ---------------------------------------------------------------------------

def test_status_and_expiry_logic(dokument_df: pd.DataFrame, kodifikaator_df: pd.DataFrame):
    kd_kehtiv = get_kdid_for_name(kodifikaator_df, "KEHTIV")
    kd_kehtetu = get_kdid_for_name(kodifikaator_df, "KEHTETU")

    # KEHTIV docs must not have DokKehtivKuniKpv
    invalid_kehtiv = dokument_df[
        (dokument_df["KdIDDokumendiStaatus"] == kd_kehtiv)
        & dokument_df["DokKehtivKuniKpv"].notna()
    ]
    assert invalid_kehtiv.empty, (
        "Documents with status=KEHTIV must not have DokKehtivKuniKpv populated: "
        f"{invalid_kehtiv[['DokID', 'DokKehtivKuniKpv']].head()}"
    )

    # If DokKehtivKuniKpv populated ⇒ status must be KEHTETU
    invalid_kuni = dokument_df[
        dokument_df["DokKehtivKuniKpv"].notna()
        & (dokument_df["KdIDDokumendiStaatus"] != kd_kehtetu)
    ]
    assert invalid_kuni.empty, (
        "DokKehtivKuniKpv present but status ≠ KEHTETU for: "
        f"{invalid_kuni[['DokID', 'KdIDDokumendiStaatus']].head()}"
    )


# ---------------------------------------------------------------------------
# Deleted rows: MuudetiKpv must equal KustutatiKpv when deleted
# ---------------------------------------------------------------------------

def test_deleted_rows_coherence(dokument_df: pd.DataFrame):
    deleted = dokument_df[dokument_df["KustutatiKpv"].notna()]
    mismatch = deleted[
    deleted["MuudetiKpv"].notna() &  # require present
    (deleted["MuudetiKpv"] != deleted["KustutatiKpv"])
    ]
    assert mismatch.empty, (
        "For deleted documents MuudetiKpv must equal KustutatiKpv: "
        f"{mismatch[['DokID', 'MuudetiKpv', 'KustutatiKpv']].head()}"
    )


# ---------------------------------------------------------------------------
# Temporal order
# ---------------------------------------------------------------------------

def test_loodi_before_muudeti_and_kustutati(dokument_df: pd.DataFrame):
    assert_temporal_order(dokument_df, "LoodiKpv", "MuudetiKpv")
    assert_temporal_order(dokument_df, "LoodiKpv", "KustutatiKpv")


def test_muudeti_before_kustutati(dokument_df: pd.DataFrame):
    # Only rows where both dates are present
    both = dokument_df[dokument_df["MuudetiKpv"].notna() & dokument_df["KustutatiKpv"].notna()]
    bad = both[both["MuudetiKpv"] > both["KustutatiKpv"]]
    assert bad.empty, (
        "MuudetiKpv must not be after KustutatiKpv: "
        f"{bad[['DokID', 'MuudetiKpv', 'KustutatiKpv']].head()}"
    )
