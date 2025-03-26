import pytest
import os
import pandas as pd
from src.generation.utils import get_kdid_for_name

@pytest.fixture
def dokument_df():
    """
    Reads the file '09_dokument.csv' (or another name),
    parsing date columns to test time intervals.
    """
    path = os.path.join("output","09_dokument.csv")
    df = pd.read_csv(
        path,
        parse_dates=[
            "DokValjaantudKpv","DokKehtivKuniKpv","DokKehtetuAlatesKpv",
            "LoodiKpv","MuudetiKpv","KustutatiKpv"
        ],
        encoding='ISO-8859-1'
    )
    return df

@pytest.fixture
def kodifikaator_df():
    """
    Reads 'output/kodifikaator.csv' if it exists; if not, we skip 
    because we can't test status logic without it.
    """
    path = os.path.join("output","kodifikaator.csv")
    if not os.path.exists(path):
        pytest.skip("kodifikaator.csv puudub, ei saa staatusloogikat testida.")
    return pd.read_csv(path, encoding='ISO-8859-1')

@pytest.fixture
def isik_df():
    """
    If '06_isik.csv' exists, we read it. 
    If 'IsID' is dropped from the dokument table, we skip the FK test anyway.
    """
    path = os.path.join("output","06_isik.csv")
    if not os.path.exists(path):
        pytest.skip("06_isik.csv puudub, ei saa IsID FK-d testida.")
    return pd.read_csv(path, encoding='ISO-8859-1')

def test_dokument_columns(dokument_df):
    """
    Check some essential columns that should remain even after IsID is dropped.
    """
    required_cols = [
        "DokID","DokNumber","DokSeeria","DokValjaantudKpv",
        "DokKehtivKuniKpv","DokKehtetuAlatesKpv","LoodiKpv",
        "MuudetiKpv","KustutatiKpv","KdIDDokumendiStaatus",
        "DokKehtivAlates","KdIDRiik"
    ]
    missing = [c for c in required_cols if c not in dokument_df.columns]
    assert not missing, f"Puuduvad veerud: {missing}"

def test_dokument_dokid_unique(dokument_df):
    """
    DokID must be unique and not null.
    """
    assert dokument_df["DokID"].notnull().all(), "DokID tühje väärtusi."
    duplicates = dokument_df[dokument_df["DokID"].duplicated()]
    assert duplicates.empty, f"Leidsime DokID dublikaate:\n{duplicates}"

def test_dokument_isid_fk_if_exists(dokument_df, isik_df):
    """
    If 'IsID' still exists in the dokument table (not dropped), test the FK.
    If the column is missing, we skip this test.
    """
    if "IsID" not in dokument_df.columns:
        pytest.skip("Dokumenditabelis puudub IsID veerg, drop'iti. Ei testi isikute-FK.")

    used_isid = set(dokument_df["IsID"].dropna().unique())
    valid_isid = set(isik_df["IsID"].unique())
    missing = [i for i in used_isid if i not in valid_isid]
    assert not missing, (
        "Dokumendis on IsID väärtusi, mida isik-tabelis pole:\n"
        f"{missing}"
    )

def test_dokument_status_logics(dokument_df, kodifikaator_df):
    """
    Example: 
      - If status=KEHTIV => DokKehtivKuniKpv should be None
      - If DokKehtivKuniKpv != None => we expect KEHTETU
      etc.
    """
    kd_kehtiv = get_kdid_for_name(kodifikaator_df, "KEHTIV")
    kd_kehtetu = get_kdid_for_name(kodifikaator_df, "KEHTETU")
    if not kd_kehtiv or not kd_kehtetu:
        pytest.skip("Puudub KEHTIV/KEHTETU kood, ei saa testida.")

    # 1) If KdIDDokumendiStaatus= KEHTIV => we expect DokKehtivKuniKpv is None
    kehtiv = dokument_df[dokument_df["KdIDDokumendiStaatus"]==kd_kehtiv]
    invalid_kehtiv = kehtiv[kehtiv["DokKehtivKuniKpv"].notnull()]
    assert invalid_kehtiv.empty, (
        "Staatus=KEHTIV, kuid DokKehtivKuniKpv != null:\n"
        f"{invalid_kehtiv}"
    )

    # 2) If DokKehtivKuniKpv != None => we expect KEHTETU
    ended = dokument_df[dokument_df["DokKehtivKuniKpv"].notnull()]
    mismatch = ended[ended["KdIDDokumendiStaatus"]!=kd_kehtetu]
    assert mismatch.empty, (
        "DokKehtivKuniKpv !=null => eeldame KEHTETU, kuid:\n"
        f"{mismatch}"
    )

def test_dokument_deleted(dokument_df):
    """
    Example: If KustutatiKpv is set, we expect MuudetiKpv=KustutatiKpv to satisfy test requirements.
    """
    with_deleted = dokument_df[dokument_df["KustutatiKpv"].notnull()]
    mismatch = with_deleted[with_deleted["MuudetiKpv"]!=with_deleted["KustutatiKpv"]]
    assert mismatch.empty, (
        "Kustutatud dokumendi kirjel eeldame MuudetiKpv=KustutatiKpv:\n"
        f"{mismatch}"
    )

def test_dokument_modified_after_loodi(dokument_df):
    """
    If MuudetiKpv != null, we assume it's >= LoodiKpv.
    """
    invalid = dokument_df[
        dokument_df["MuudetiKpv"].notnull() &
        (dokument_df["MuudetiKpv"] < dokument_df["LoodiKpv"])
    ]
    assert invalid.empty, (
        "MuudetiKpv < LoodiKpv:\n{invalid}"
    )
