"""Tests for 08_isikudokument.csv (person document bridge) and its
referenced 09_dokument.csv records.

Generic validations leverage helpers; business‑specific logic lives here.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime

import pandas as pd
import pytest

from src.validation.helpers import (
    assert_unique_not_null,
    assert_temporal_order,
)
from src.generation.utils import get_kdid_for_name

# ---------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------

OUTPUT = Path(__file__).resolve().parents[2] / "output"
DATA = Path(__file__).resolve().parents[2] / "data"


@pytest.fixture(scope="module")
def isikudokument_df() -> pd.DataFrame:  # noqa: D401
    """Load 08_isikudokument.csv with parsed audit‑date columns."""
    path = OUTPUT / "08_isikudokument.csv"
    return pd.read_csv(
        path,
        parse_dates=["LoodiKpv", "MuudetiKpv", "KustutatiKpv"],
        low_memory=False,
        encoding="ISO-8859-1",
    )


@pytest.fixture(scope="module")
def dokument_df() -> pd.DataFrame | None:  # noqa: D401
    """Load 09_dokument.csv if present (some tests depend on it)."""
    path = OUTPUT / "09_dokument.csv"
    if not path.exists():
        pytest.skip("09_dokument.csv missing – skipping DokID FK tests")
    return pd.read_csv(
        path,
        parse_dates=[
            "DokValjaantudKpv",
            "DokKehtivKuniKpv",
            "DokKehtetuAlatesKpv",
            "LoodiKpv",
            "MuudetiKpv",
            "KustutatiKpv",
        ],
        low_memory=False,
        encoding="ISO-8859-1",
    )


@pytest.fixture(scope="module")
def isik_df() -> pd.DataFrame | None:
    """Load 06_isik.csv for IsID FK checks."""
    path = OUTPUT / "06_isik.csv"
    if not path.exists():
        pytest.skip("06_isik.csv missing – skipping IsID FK tests")
    return pd.read_csv(path, encoding="ISO-8859-1")


@pytest.fixture(scope="session")
def kodifikaator_df(conftest_kodifikaator_df):  # type: ignore[name-defined]
    """Alias session‑wide kodifikaator fixture from conftest.py."""
    return conftest_kodifikaator_df  # provided by session fixture


# ---------------------------------------------------------------------
# Column presence and primary‑key uniqueness
# ---------------------------------------------------------------------

REQUIRED_COLS = [
    "IDokID",
    "IsID",
    "DokID",
    "IDokIsikukood",
    "IDokEesnimi",
    "IDokPerenimi",
    "IDokIsanimi",
    "IDokVanaEesnimi",
    "IDokVanaPerenimi",
    "IDokVanaIsikukood",
    "KdIDIsikuRoll",
    "AsID",
    "LoodiKpv",
    "MuudetiKpv",
    "KustutatiKpv",
]


def test_columns_present(isikudokument_df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLS if c not in isikudokument_df.columns]
    assert not missing, f"Missing columns: {missing}"


def test_pk_idokid(isikudokument_df: pd.DataFrame) -> None:
    assert_unique_not_null(isikudokument_df, "IDokID")


# ---------------------------------------------------------------------
# Foreign‑key relationships
# ---------------------------------------------------------------------


def test_isid_fk(isikudokument_df: pd.DataFrame, isik_df: pd.DataFrame) -> None:  # noqa: D401
    valid = set(isik_df["IsID"].unique())
    used = set(isikudokument_df["IsID"].unique())
    missing = used - valid
    assert not missing, f"Isikudokument.IsID refers to unknown IsID: {missing}"


def test_dokid_fk(isikudokument_df: pd.DataFrame, dokument_df: pd.DataFrame) -> None:  # noqa: D401
    valid = set(dokument_df["DokID"].unique())
    used = set(isikudokument_df["DokID"].unique())
    missing = used - valid
    assert not missing, f"Isikudokument.DokID refers to unknown DokID: {missing}"


# ---------------------------------------------------------------------
# Business‑specific rules
# ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def kd_codes(kodifikaator_df):  # noqa: D401
    """Map of needed KdID values (KEHTIV / KEHTETU)."""
    return {
        name: get_kdid_for_name(kodifikaator_df, name)  # type: ignore[arg-type]
        for name in ("KEHTIV", "KEHTETU")
    }


def test_kehtiv_document_no_expiry(dokument_df: pd.DataFrame, kd_codes) -> None:  # noqa: D401
    kehtiv = dokument_df[dokument_df["KdIDDokumendiStaatus"] == kd_codes["KEHTIV"]]
    bad = kehtiv[kehtiv["DokKehtivKuniKpv"].notna()]
    assert bad.empty, "KEHTIV documents must not have DokKehtivKuniKpv filled"


def test_bridge_kehtiv_refs_no_expiry(
    isikudokument_df: pd.DataFrame,
    dokument_df: pd.DataFrame,
    kd_codes,
) -> None:
    merged = isikudokument_df.merge(
        dokument_df[["DokID", "KdIDDokumendiStaatus", "DokKehtivKuniKpv"]],
        on="DokID",
        how="left",
    )
    invalid = merged[
        (merged["KdIDDokumendiStaatus"] == kd_codes["KEHTIV"]) &
        (merged["DokKehtivKuniKpv"].notna())
    ]
    assert invalid.empty, "Bridge row references a KEHTIV doc that has expiry date"


def test_document_start_not_future(dokument_df: pd.DataFrame, kd_codes) -> None:  # noqa: D401
    """DokKehtivAlates must not be in the future for KEHTIV docs."""
    if "DokKehtivAlates" not in dokument_df.columns:
        pytest.skip("DokKehtivAlates column missing")
    now = datetime.now()
    dokument_df["DokKehtivAlates"] = pd.to_datetime(dokument_df["DokKehtivAlates"], errors="coerce")
    bad = dokument_df[
        (dokument_df["KdIDDokumendiStaatus"] == kd_codes["KEHTIV"]) &
        (dokument_df["DokKehtivAlates"] > now)
    ]
    assert bad.empty, "Found KEHTIV docs with start date in the future"


# ---- “Old fields” prefix rules --------------------------------------


@pytest.mark.parametrize(
    "col,prefix",
    [
        ("IDokVanaEesnimi", "Vana-"),
        ("IDokVanaPerenimi", "Vana-"),
        ("IDokVanaIsikukood", "OLD-"),
    ],
)
def test_old_field_prefixes(isikudokument_df: pd.DataFrame, col: str, prefix: str) -> None:
    filled = isikudokument_df[col].dropna()
    bad = [v for v in filled if not str(v).startswith(prefix)]
    assert not bad, f"Values in {col} should start with '{prefix}', but found: {bad}"


# ---- Audit‑date coherence ------------------------------------------


def test_muudetikpv_not_before_loodi(isikudokument_df: pd.DataFrame) -> None:
    assert_temporal_order(isikudokument_df, "LoodiKpv", "MuudetiKpv")


def test_muudetikpv_equals_kustutati_when_deleted(isikudokument_df: pd.DataFrame) -> None:
    deleted = isikudokument_df[isikudokument_df["KustutatiKpv"].notna()]
    mismatch = deleted[deleted["MuudetiKpv"] != deleted["KustutatiKpv"]]
    assert mismatch.empty, "Deleted rows must have MuudetiKpv == KustutatiKpv"
