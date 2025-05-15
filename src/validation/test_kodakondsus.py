"""Tests for 07_kodakondsus.csv – citizenship history table.

Only business‑specific rules live here; common validations reuse
'src.validation.helpers' utilities.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.validation.helpers import (
    assert_unique_not_null,
    assert_fk,
    assert_temporal_order,
)
from src.generation.utils import get_kdid_for_name

# ----------------------------------------------------------------------------
# Fixtures – just lightweight loaders; heavy shared things sit in conftest.py
# ----------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]  # project root assumed two levels up
OUTPUT = ROOT / "output"

DATE_COLS = [
    "KodKehtibAlates",
    "KodKehtibKuni",
    "LoodiKpv",
    "MuudetiKpv",
    "KustutatiKpv",
]


@pytest.fixture(scope="module")
def kodakondsus_df() -> pd.DataFrame:
    return pd.read_csv(
        OUTPUT / "07_kodakondsus.csv",
        parse_dates=DATE_COLS,
        encoding="ISO-8859-1",
    )


@pytest.fixture(scope="module")
def dokument_df() -> pd.DataFrame:
    return pd.read_csv(
        OUTPUT / "09_dokument.csv",
        parse_dates=["DokKehtivAlates", "DokKehtivKuniKpv",
                     "DokKehtetuAlatesKpv", "LoodiKpv",
                     "MuudetiKpv", "KustutatiKpv"],
        encoding="ISO-8859-1",
        low_memory=False
    )


@pytest.fixture(scope="module")
def isik_df() -> pd.DataFrame:
    return pd.read_csv(
        OUTPUT / "06_isik.csv",
        usecols=["IsID"],
        encoding="ISO-8859-1",
    )


# ----------------------------------------------------------------------------
# Generic validations (PK, FK)
# ----------------------------------------------------------------------------

def test_pk_kodid(kodakondsus_df):
    """'KodID' is the primary key."""
    assert_unique_not_null(kodakondsus_df, "KodID")


def test_fk_isid(kodakondsus_df, isik_df):
    """'IsID' values reference the person table."""
    assert_fk(kodakondsus_df, "IsID", isik_df, ref_col="IsID")


# ----------------------------------------------------------------------------
# Business‑specific rules
# ----------------------------------------------------------------------------

def test_status_matches_end_or_delete(kodakondsus_df, kodifikaator_df):
    """If a citizenship row has an end date or is deleted, status must be KEHTETU."""
    kd_kehtetu = get_kdid_for_name(kodifikaator_df, "KEHTETU")
    if kd_kehtetu is None:
        pytest.skip("No KEHTETU code in kodifikaator.")

    ended_or_deleted = kodakondsus_df[
        kodakondsus_df["KodKehtibKuni"].notna() | kodakondsus_df["KustutatiKpv"].notna()
        ]
    mismatch = ended_or_deleted[ended_or_deleted["KdIDStaatus"] != kd_kehtetu]
    assert mismatch.empty, (
        "Rows with an end date or deletion must have status=KEHTETU, found mismatches:\n"
        f"{mismatch}"
    )


def test_temporal_consistency(kodakondsus_df):
    """'LoodiKpv ≤ KodKehtibAlates' and 'Muudeti,Kustutati ≥ Loodi'."""
    assert_temporal_order(kodakondsus_df, "LoodiKpv", "KodKehtibAlates")
    # Muudeti vs Loodi
    assert_temporal_order(kodakondsus_df, "LoodiKpv", "MuudetiKpv")
    # Kustutati vs Muudeti (allow equal)
    assert_temporal_order(kodakondsus_df, "MuudetiKpv", "KustutatiKpv")


def test_document_reference_order(kodakondsus_df):
    """If both document columns are filled, 'DokIDLopuAlus ≥ DokIDAlguseAlus'."""
    both = kodakondsus_df[kodakondsus_df["DokIDLopuAlus"].notna()]
    bad = both[both["DokIDLopuAlus"] < both["DokIDAlguseAlus"]]
    assert bad.empty, (
        "Expected DokIDLopuAlus to be ≥ DokIDAlguseAlus, found:\n" 
        f"{bad[['KodID', 'DokIDAlguseAlus', 'DokIDLopuAlus']]}"
    )


def test_no_parallel_unknown_and_country_citizenship(
        kodakondsus_df, dokument_df, kodifikaator_df):
    """
    A person must NOT have, at the same time,

      • an active citizenship backed by an ordinary document and
      • an active citizenship backed by a document of type
        "MÄÄRATLEMATA KODAKONDSUS".

    "Active" KdIDStaatus == KEHTIV and KodKehtibKuni IS NULL
    """

    kd_kehtiv = get_kdid_for_name(kodifikaator_df, "KEHTIV")

    # The code-generation script may register the doc type under either of
    # these two short names – try the longer one first.
    kd_id_maaratlemata_doc = get_kdid_for_name(kodifikaator_df, "MÄÄRATLEMATA")

    # keep only ACTIVE citizenship records
    active_cit = kodakondsus_df[
        (kodakondsus_df["KdIDStaatus"] == kd_kehtiv)
        & (kodakondsus_df["KodKehtibKuni"].isnull())
        ].copy()

    # bring in the document type that opened the period
    active_cit = active_cit.merge(
        dokument_df[["DokID", "KdIDDokumendiLiik"]],
        left_on="DokIDAlguseAlus",
        right_on="DokID",
        how="left",
    )

    # If the opening document is missing in 09_dokument.csv,
    # treat it as an unknown (MÄÄRATLEMATA) document:
    active_cit["KdIDDokumendiLiik"].fillna(kd_id_maaratlemata_doc, inplace=True)

    # scan person-by-person
    offenders = []
    for is_id, grp in active_cit.groupby("IsID"):
        has_unknown = (grp["KdIDDokumendiLiik"] == kd_id_maaratlemata_doc).any()
        has_regular = (grp["KdIDDokumendiLiik"] != kd_id_maaratlemata_doc).any()
        if has_unknown and has_regular:
            offenders.append(is_id)

    assert not offenders, (
        "Found person with both unknown and known active citizenships: "
        f"{sorted(offenders)}"
    )
