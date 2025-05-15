"""
error_generation.main
---------------------

Low-level helpers + a single public function
'run(selected_tests: list[int] | None)' that mutates the CSVs in-place.

To add a new test later:
1.  Write a function 'case_N(df_bundle)' that does the mutation.
2.  Add it to 'CASE' (one line).
That's it – generate_errors.py will pick it up automatically.
"""
from __future__ import annotations
from pathlib import Path
from typing import Callable
import pandas as pd

DATA_DIR = Path("output")
CODEBOOK = Path("data/kodifikaator.csv")


# Helpers

def load_csv(name: str, **read_kw) -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / name, encoding="ISO-8859-1", low_memory=False, **read_kw)


def save_csv(df: pd.DataFrame, name: str) -> None:
    df.to_csv(DATA_DIR / name, index=False, encoding="ISO-8859-1")


# Each function must accept / return nothing – they do I/O internally.
# They can of course call common helpers or other modules.

def case_01():
    """Duplicate KEHTIV residence + ELUKOHATEADE for 5 random people."""
    from .test_case_01 import add_invalid_residences
    dfs = dict(
        df_isikuaadress=load_csv("05_isikuaadress.csv"),
        df_aadress=load_csv("01_aadress.csv"),
        df_dokumendid=load_csv("09_dokument.csv"),
        df_isikudokument=load_csv("08_isikudokument.csv"),
        df_isik=load_csv("06_isik.csv"),
        df_kodifikaator=pd.read_csv(CODEBOOK, encoding="ISO-8859-1"),
    )
    out = add_invalid_residences(**dfs, person_count=5, seed=42)
    # unpack & save
    save_csv(out[0], "05_isikuaadress.csv"),
    save_csv(out[1], "09_dokument.csv"),
    save_csv(out[2], "08_isikudokument.csv")


def case_02():
    """Make a few JUHILUBA docs KEHTIV but already expired."""
    from .test_case_02 import make_driver_licenses_invalid_by_date
    df_docs = load_csv("09_dokument.csv")
    df_kd = pd.read_csv(CODEBOOK, encoding="ISO-8859-1")
    df_new = make_driver_licenses_invalid_by_date(df_docs, df_kd, num_changes=3, seed=42)

    save_csv(df_new, "09_dokument.csv")


def case_03():
    """Shift KEHTIV residence periods of immigrants to the future."""
    from .test_case_03 import shift_valid_residences_of_immigrants_to_future as shift
    dfs = dict(
        df_isik=load_csv("06_isik.csv"),
        df_isikuaadress=load_csv("05_isikuaadress.csv"),
        df_isikudokument=load_csv("08_isikudokument.csv"),
        df_dokumendid=load_csv("09_dokument.csv"),
        df_kodifikaator=pd.read_csv(CODEBOOK, encoding="ISO-8859-1"),
    )
    out = shift(**dfs, future_days=730, seed=42)
    save_csv(out[0], "06_isik.csv")
    save_csv(out[1], "05_isikuaadress.csv")
    save_csv(out[2], "09_dokument.csv")


def case_04():
    """Add expired citizenship + MÄÄRATLEMATA KODAKONDSUS doc."""
    from .test_case_04 import add_invalid_citizenship
    dfs = dict(
        df_citizenship=load_csv("07_kodakondsus.csv"),
        df_documents=load_csv("09_dokument.csv"),
        df_person_document=load_csv("08_isikudokument.csv"),
        df_person=load_csv("06_isik.csv"),
        df_codebook=pd.read_csv(CODEBOOK, encoding="ISO-8859-1"),
    )
    out = add_invalid_citizenship(**dfs, person_count=5, seed=42)
    save_csv(out[0], "07_kodakondsus.csv")
    save_csv(out[1], "09_dokument.csv")
    save_csv(out[2], "08_isikudokument.csv")


# Map “test number” to callable
_CASES: dict[int, Callable[[], None]] = {
    1: case_01,
    2: case_02,
    3: case_03,
    4: case_04,
}


def run(selected_tests: list[int] | None = None) -> None:
    """
    Apply the requested mutation cases in numerical order.

    Parameters
    ----------
    selected_tests
        • None or empty to run all cases  
        • otherwise -> list of ints (e.g. [1, 3, 4])
    """
    if not selected_tests:
        selected_tests = sorted(_CASES)
    invalid = [t for t in selected_tests if t not in _CASES]
    if invalid:
        raise ValueError(f"Unknown test id(s): {invalid}. Available: {sorted(_CASES)}")
    for t in sorted(selected_tests):
        print(f"[error_generation] running case {t}")
        _CASES[t]()
    print("[error_generation] done.")
