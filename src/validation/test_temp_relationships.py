"""Tests for the in‑memory DataFrame returned by
'generate_temp_relatsionships'.

These tests run without touching CSVs, exercising the random‑generation
logic directly.  Shared helpers are still useful for uniqueness and date
sanity.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from src.generation.temp_relationships import generate_temp_relatsionships
from src.validation.helpers import assert_unique_not_null, assert_temporal_order


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def temp_rel_df(kodifikaator_df):
    """Generate 20 synthetic persons for quick unit tests."""
    return generate_temp_relatsionships(
        n_people=20,
        seed=123,
        df_possible_aadresses=None,
        df_kd=kodifikaator_df,
    )


# ---------------------------------------------------------------------------
# Simple structural checks
# ---------------------------------------------------------------------------


def test_row_count(temp_rel_df):
    assert len(temp_rel_df) == 20, "Expected exactly 20 generated persons"


def test_pk_isid(temp_rel_df):
    assert_unique_not_null(temp_rel_df, "IsID")


# ---------------------------------------------------------------------------
# Relationship‑specific checks
# ---------------------------------------------------------------------------


def test_family_id_coherence(temp_rel_df):
    """
    Partners and their children must share the same family ID.
    We first build a quick lookup dict so we don’t keep slicing the frame.
    """
    fam_by_id = temp_rel_df.set_index("IsID")["Perekonna ID"].to_dict()

    for _, row in temp_rel_df.iterrows():
        fam = row["Perekonna ID"]
        partner = row.get("Partneri ID")
        if pd.notna(partner) and partner in fam_by_id:          # guard!
            assert fam_by_id[partner] == fam, (
                f"Partner {partner} is in family {fam_by_id[partner]}, "
                f"but {row.IsID} is in {fam}"
            )
        for parent_id in (row.get("Vanem(ad)") or []):
            if parent_id in fam_by_id:                          # guard!
                assert fam_by_id[parent_id] == fam, (
                    f"Parent {parent_id} is in family {fam_by_id[parent_id]}, "
                    f"but child {row.IsID} is in {fam}"
                )


def test_child_birth_after_parents(temp_rel_df):
    """Each child must be born after its parents (by at least 18 years)."""

    for _, row in temp_rel_df[temp_rel_df["Vanuse staatus"] == "Laps"].iterrows():
        child_bd = row["Sünniaeg"]
        for par in row["Vanem(ad)"] or []:
            par_bd = temp_rel_df.loc[temp_rel_df.IsID == par, "Sünniaeg"].iloc[0]
            # allow edge‑cases where birthdates are None (unlikely)
            if pd.notna(child_bd) and pd.notna(par_bd):
                diff_years = (child_bd - par_bd).days / 365.25
                assert diff_years >= 18, (
                    f"Child {row.IsID} born {diff_years:.1f}y after parent {par}"
                )


def test_marital_rules(temp_rel_df):
    """Logical marital‑status rules for adults vs children."""

    for _, row in temp_rel_df.iterrows():
        status = row["Suhteseis"]
        if row["Vanuse staatus"] == "Laps":
            assert status == "Vallaline", "Children must be vallaline"
        else:
            assert status in {"Vallaline", "Abielus", "Lahutatud"}
            if status == "Abielus":
                assert row["Partneri ID"] is not None, "Married adult must have a partner"


def test_citizenship_values(temp_rel_df):
    """Citizenship must be EE or MUU only."""

    assert set(temp_rel_df["Kodakondsus"].unique()) <= {"EE", "MUU"}
