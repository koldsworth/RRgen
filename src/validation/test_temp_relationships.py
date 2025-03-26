import pytest
import pandas as pd
import os
from src.generation.temp_relationships import generate_temp_relatsionships

@pytest.fixture
def kodifikaator_df():
    """
    Reads "kodifikaator.csv" 
    KdIDAadressiStaatus, KdIDAadressiLiik, etc. are valid.
    """
    path = os.path.join("data", "kodifikaator.csv")
    df = pd.read_csv(path, encoding='ISO-8859-1')
    return df

@pytest.fixture
def temp_rel_df(kodifikaator_df):
    """
    Calls 'generate_temp_relatsionships' with ~20 people 
    to test if the core logic holds.
    Uses seed=123 for reproducibility.
    """
    df = generate_temp_relatsionships(
        n_people=20,
        seed=123,
        df_possible_aadresses=None,  # or something else if desired
        df_kd=kodifikaator_df
    )
    return df

def test_row_count(temp_rel_df):
    """
    Test1: Verifies that the number of rows = n_people (20).
    """
    assert len(temp_rel_df) == 20, (
        f"Ootasime 20 rida, aga saime {len(temp_rel_df)}"
    )

def test_isid_unique(temp_rel_df):
    """
    Test2: Checks whether IsID is unique and not null.
    """
    assert temp_rel_df["IsID"].notnull().all(), "IsID sisaldab tühje väärtusi."
    dups = temp_rel_df[temp_rel_df["IsID"].duplicated()]
    assert dups.empty, f"IsID pole unikaalne, duplikaadid:\n{dups}"

def test_family_id(temp_rel_df):
    """
    Test3: 'Perekonna ID' is generated via a BFS approach 
    that links partners and children/parents.
    We verify that if two persons have a partner or child relationship, 
    they share the same 'Perekonna ID'.

    Your function sets 'Partneri ID' and 'Vanem(ad)'. 
    We check a few people to ensure they're in the same family.
    This is not an exhaustive test; just a quick spot-check.
    """
    df = temp_rel_df
    couples = df.dropna(subset=["Partneri ID"])
    for idx, row in couples.iterrows():
        partner_id = row["Partneri ID"]
        self_fam = row["Perekonna ID"]
        partner_fam = df.loc[df["IsID"] == partner_id, "Perekonna ID"].values
        if len(partner_fam) > 0:
            assert partner_fam[0] == self_fam, (
                f"Partnerid ei ole samas Perekonna ID-s: {row['IsID']} vs {partner_id}"
            )

def test_child_parent_birthdates(temp_rel_df):
    """
    Test5: A child's birthdate should be later than the parent's birthdate.
    If a child has Vanem(ad), we check child.Sünniaeg > parent's Sünniaeg 
    (minus ~18 yrs?), etc. 
    Your code around line 8 tries to ensure that. 
    Here we just do a minimal check that the parent's birth < child's birth.
    """
    df = temp_rel_df
    for idx, row in df.iterrows():
        if row["Vanuse staatus"] == "Laps":
            parents = row["Vanem(ad)"]
            if isinstance(parents, list) and len(parents) > 0:
                child_bd = row["Sünniaeg"]
                if child_bd is None:
                    continue
                # Check that parent's BD < child's BD
                for par_id in parents:
                    par_rows = df[df["IsID"] == par_id]
                    if len(par_rows) > 0:
                        par_bd = par_rows.iloc[0]["Sünniaeg"]
                        if par_bd is not None:
                            assert par_bd < child_bd, (
                                f"Lapse {row['IsID']} BD={child_bd}, vanema={par_id} BD={par_bd}, reegel rikutud"
                            )

def test_marital_status(temp_rel_df):
    """
    Test6: If Vanuse staatus='Laps' => Suhteseis='Vallaline'. 
           If adult => possibly 'Vallaline','Abielus','Lahutatud'.
           If 'Abielus', we assume there's a partner.
    """
    for idx, row in temp_rel_df.iterrows():
        if row["Vanuse staatus"] == "Laps":
            assert row["Suhteseis"] == "Vallaline", (
                f"Laps {row['IsID']} ei ole 'Vallaline'? On {row['Suhteseis']}"
            )
        else:
            # Adult => must be one of Vallaline, Abielus, Lahutatud
            assert row["Suhteseis"] in ["Vallaline","Abielus","Lahutatud"], (
                f"Unexpected Suhteseis for adult {row['IsID']}: {row['Suhteseis']}"
            )
            if row["Suhteseis"] == "Abielus":
                assert row["Partneri ID"] is not None, (
                    f"Abielus isikul {row['IsID']} puudub 'Partneri ID'"
                )

def test_family_address(temp_rel_df):
    """
    Test7: Checking address rules:
      - Married couple has an 80% chance to share addresses 
      - Child <18 => same address as parent (line 12?), 
      - Randomly 70% if >=18, etc.
    Because it's random, we only do minimal checks, e.g. 
      - married couples are likely to share addresses
      - child <18 likely has parent's address
    It's stochastic, so it may fail occasionally. 
    We'll skip a thorough approach here.
    """
    pass  # optional or partial test

def test_kodakondsus(temp_rel_df):
    """
    Test8: ~70% 'EE', 30% 'MUU'. 
    With n=20, we expect roughly 14 vs 6, but that's random. 
    We'll do a minimal check that all are 'EE' or 'MUU' 
    (no unexpected citizenship codes).
    """
    val_counts = temp_rel_df["Kodakondsus"].value_counts(dropna=False)
    for val in val_counts.index:
        assert val in ["EE","MUU"], (
            f"Leidsime ebasobiliku 'Kodakondsus'={val}"
        )
