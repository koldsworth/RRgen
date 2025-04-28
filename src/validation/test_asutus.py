"""Tests for 03_asutus.csv (institutions) and 04_isik_asutus.csv (person‑institution links).

Shared FK/PK/date helpers are imported from 'validation.helpers'.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.validation.helpers import (
    assert_fk,
    assert_unique_not_null,
    assert_temporal_order,
)
from src.generation.utils import get_kdid_for_name

# ----------------------------------------------------------------------
# Local fixtures – data lives under the session‑scope OUTPUT folder
# ----------------------------------------------------------------------

OUTPUT = Path(__file__).resolve().parents[2] / "output"


@pytest.fixture(scope="module")
def asutus_df() -> pd.DataFrame:
    path = OUTPUT / "03_asutus.csv"
    if not path.exists():
        pytest.skip("03_asutus.csv missing – generation step not run")
    date_cols = [
        "AsAlguseKpv",
        "AsLopuKpv",
        "LoodiKpv",
        "MuudetiKpv",
        "KustutatiKpv",
    ]
    return pd.read_csv(path, parse_dates=date_cols, low_memory=False)


@pytest.fixture(scope="module")
def isik_asutus_df() -> pd.DataFrame:
    path = OUTPUT / "04_isik_asutus.csv"
    if not path.exists():
        pytest.skip("04_isik_asutus.csv missing – generation step not run")
    date_cols = [
        "IAsAlgusKpv",
        "IAsKinniKpv",
        "LoodiKpv",
        "MuudetiKpv",
        "KustutatiKpv",
    ]
    return pd.read_csv(path, parse_dates=date_cols, low_memory=False)


# ----------------------------------------------------------------------
# 03_asutus.csv – institution table rules
# ----------------------------------------------------------------------

def test_asutus_pk_unique(asutus_df: pd.DataFrame):
    assert_unique_not_null(asutus_df, "AsID", label="Asutus.AsID")


def test_asutus_fk_status(asutus_df: pd.DataFrame, kodifikaator_df: pd.DataFrame):
    if "KdIDStaatus" in asutus_df.columns:
        assert_fk(
            asutus_df,
            "KdIDStaatus",
            kodifikaator_df,
            msg="Asutus.KdIDStaatus contains unknown codes",
        )
    else:
        pytest.skip("KdIDStaatus column missing in 03_asutus.csv")


def test_asutus_dates(asutus_df: pd.DataFrame):
    # AsAlguseKpv <= AsLopuKpv (if LopuKpv present)
    assert_temporal_order(asutus_df, "AsAlguseKpv", "AsLopuKpv")
    # LoodiKpv <= AsAlguseKpv
    assert_temporal_order(asutus_df, "LoodiKpv", "AsAlguseKpv", allow_equal=True)

    # If both KustutatiKpv and AsLopuKpv exist they must be equal
    subset = asutus_df.dropna(subset=["KustutatiKpv", "AsLopuKpv"])
    mismatch = subset[subset["KustutatiKpv"] != subset["AsLopuKpv"]]
    assert mismatch.empty, (
        "Where Asutus is deleted we expect KustutatiKpv == AsLopuKpv; mismatches: "
        f"{mismatch[['AsID', 'AsLopuKpv', 'KustutatiKpv']].to_dict('records')}"
    )


# ----------------------------------------------------------------------
# 04_isik_asutus.csv – link table rules
# ----------------------------------------------------------------------

def test_isik_asutus_fk_asutus(
    isik_asutus_df: pd.DataFrame, asutus_df: pd.DataFrame
):
    if "AsID" not in isik_asutus_df.columns:
        pytest.skip("AsID column missing in 04_isik_asutus.csv")
    assert_fk(
        isik_asutus_df,
        "AsID",
        asutus_df,
        ref_col="AsID",
        msg="IsikAsutus.AsID references unknown AsID values",
    )


def test_isik_asutus_dates(isik_asutus_df: pd.DataFrame):
    # IAsAlgusKpv <= IAsKinniKpv (if closed)
    assert_temporal_order(isik_asutus_df, "IAsAlgusKpv", "IAsKinniKpv")
    # LoodiKpv <= IAsAlgusKpv
    assert_temporal_order(isik_asutus_df, "LoodiKpv", "IAsAlgusKpv", allow_equal=True)



def test_isik_asutus_deleted_kehtetu(
    isik_asutus_df: pd.DataFrame, kodifikaator_df: pd.DataFrame
):
    if "KdIDStaatus" not in isik_asutus_df.columns:
        pytest.skip("KdIDStaatus column missing in 04_isik_asutus.csv")

    kd_kehtetu = get_kdid_for_name(kodifikaator_df, "KEHTETU")
    deleted_rows = isik_asutus_df[isik_asutus_df["KustutatiKpv"].notna()]
    mismatch = deleted_rows[deleted_rows["KdIDStaatus"] != kd_kehtetu]
    assert mismatch.empty, (
        "Rows with KustutatiKpv must have KdIDStaatus=KEHTETU; mismatch IDs: "
        f"{mismatch['IAsID'].tolist()}"
    )
