import os
import pandas as pd
from functools import lru_cache
from datetime import datetime
from .aadress import generate_aadress, generate_aadress_komponent
from .asutus import generate_asutus, build_isik_asutus
from .isik import generate_isik
from .isikuaadress import generate_isikuaadress
from .isikudokument import generate_isikudokument
from .kodakondsus import generate_kodakondsus
from .temp_person_document import generate_person_document
from .temp_relationships import generate_temp_relatsionships

# Default output folder (can be overridden via args)
OUTPUT_FOLDER = "output"

INPUT_FILES = {
    "kodifikaator": "data/kodifikaator.csv",
    "aadress": "data/aadress.csv",
    "aadresskomponent": "data/aadresskomponent.csv",
    "asutus": "data/asutus.xlsx"
}


# Helper function to print progress with timestamps
def log_progress(step_name):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {step_name}...")


# Load all input data files into Pandas DataFrames
def load_data():
    log_progress("Loading data files")
    try:
        return {
            "kodifikaator": pd.read_csv(INPUT_FILES["kodifikaator"], delimiter=",", encoding="ISO-8859-1"),
            "aadress": pd.read_csv(INPUT_FILES["aadress"], delimiter=";", encoding="ISO-8859-1", low_memory=False),
            "aadresskomponent": pd.read_csv(INPUT_FILES["aadresskomponent"], delimiter=";", encoding="ISO-8859-1",
                                            low_memory=False),
            "asutus": pd.read_excel(INPUT_FILES["asutus"])
        }
    except Exception as e:
        print(f"Error loading files: {e}")
        exit(1)


# Generate all required tables based on input data
def generate_tables(data, num_records=10000, seed=42):
    log_progress("Generating address data")
    gen_df_adr = generate_aadress(df=data["aadress"], df_kodifikaator=data["kodifikaator"], num_records=num_records,
                                  seed=seed)
    gen_df_ak = generate_aadress_komponent(df=data["aadresskomponent"][:num_records],
                                           df_kodifikaator=data["kodifikaator"])

    log_progress("Generating temporary relationships")
    gen_temp_rel = generate_temp_relatsionships(
        n_people=num_records,
        seed=seed,
        df_possible_aadresses=gen_df_adr,
        df_kd=data["kodifikaator"]
    )

    log_progress("Building institution-person relationships")
    gen_df_ias = build_isik_asutus(gen_temp_rel, df_kodifikaator=data["kodifikaator"])

    log_progress("Generating institution data")
    gen_df_as = generate_asutus(df=data["asutus"], df_kodifikaator=data["kodifikaator"], seed=seed)

    possible_addresses = gen_df_adr['AdrID'].tolist()
    final_address_map = dict(zip(gen_temp_rel["IsID"], gen_temp_rel["Aadress"]))

    log_progress("Generating person addresses")
    gen_df_iadr = generate_isikuaadress(
        df_relationships=gen_temp_rel,
        possible_addresses=possible_addresses,
        final_address_map=final_address_map,
        df_kodifikaator=data["kodifikaator"],
        min_address_count=1,
        max_address_count=3,
        earliest_date=datetime(1990, 1, 1),
        latest_date=datetime(2020, 12, 31),
        ensure_new_address=True
    )

    log_progress("Generating person data")
    gen_df_is = generate_isik(gen_temp_rel, isikuaadress=gen_df_iadr, kodifikaator=data["kodifikaator"])

    log_progress("Generating citizenship data")
    new_dokid_counter = gen_df_iadr['DokIDAlus'].max()
    gen_df_kk = generate_kodakondsus(df_isik=gen_df_is, kodifikaator=data["kodifikaator"],
                                     start_doc_id=new_dokid_counter)

    doc_definitions = [
        ("ELUKOHATEADE", gen_df_iadr, {
            'is_id_col': 'IsID',
            'doc_id_col': 'DokIDAlus',
            'start_date_col': 'IAdrKehtibAlatesKpv',
            'end_date_col': 'IAdrKehtibKuniKpv',
            'loodi_date_col': 'LoodiKpv'
        }),
        ("KODAKONDSUSE TÜHISTAMINE", gen_df_kk, {
            'is_id_col': 'IsID',
            'doc_id_col': 'DokIDLopuAlus',
            'start_date_col': 'KodKehtibAlates',
            'loodi_date_col': 'LoodiKpv'
        }),
        ("KODAKONDSUS", gen_df_kk, {
            'is_id_col': 'IsID',
            'doc_id_col': 'DokIDAlguseAlus',
            'start_date_col': 'KodKehtibAlates',
            'end_date_col': 'KodKehtibKuni',
            'loodi_date_col': 'LoodiKpv'
        }),
        ("JUHILUBA", gen_df_is, None)
    ]

    log_progress("Generating person documents")
    doc_id_counter = 1
    doc_dfs = []

    for doc_type, source, cols in doc_definitions:
        if cols:
            doc_df = generate_person_document(
                df_source=source,
                kodifikaator=data["kodifikaator"],
                doc_type=doc_type,
                source_cols=cols,
                mode="source",
                start_doc_id=doc_id_counter
            )
        else:
            doc_df = generate_person_document(
                df_source=source,
                kodifikaator=data["kodifikaator"],
                doc_type=doc_type,
                mode="random",
                start_doc_id=doc_id_counter
            )
        doc_dfs.append(doc_df)
        doc_id_counter = doc_df["DokID"].max() + 1

    df_docs_isid12 = pd.concat(doc_dfs, ignore_index=True)

    log_progress("Generating final document dataset")
    df_isdok = generate_isikudokument(df_docs=df_docs_isid12, df_isik=gen_df_is, doc_isik_merge_on="IsID")
    gen_df_docs = df_docs_isid12.drop(columns=['IsID'])

    return {
        "01_aadress.csv": gen_df_adr,
        "02_aadresskomponent.csv": gen_df_ak,
        "03_asutus.csv": gen_df_as,
        "04_isik_asutus.csv": gen_df_ias,
        "05_isikuaadress.csv": gen_df_iadr,
        "06_isik.csv": gen_df_is,
        "07_kodakondsus.csv": gen_df_kk,
        "08_isikudokument.csv": df_isdok,
        "09_dokument.csv": gen_df_docs
    }


def save_to_csv(dataframes):
    log_progress("Saving generated data to CSV")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    for filename, df in dataframes.items():
        try:
            df.to_csv(os.path.join(OUTPUT_FOLDER, filename), index=False)
            print(f"Saved: {filename}")
        except Exception as e:
            print(f"Error saving {filename}: {e}")

    log_progress("Data generation complete. CSV files are in the 'output' folder.")


def inject_ias_metadata_to_csv_folder(df_isik_asutus: pd.DataFrame) -> None:
    """
    Populate IAsIDLooja, IAsIDMuutja and AsID columns in every CSV found in
    OUTPUT_FOLDER.  Keeps the original pick one random matching row logic,
    but does it only once per distinct timestamp, not once per table row.
    """

    # Pre-process the lookup table only once
    df_isik_asutus = df_isik_asutus.copy()
    date_col = "IAsKinniKpv"
    df_isik_asutus[date_col] = df_isik_asutus[date_col].apply(
        pd.to_datetime, errors="coerce", format="%Y.%m.%d %H:%M:%S"
    )

    @lru_cache(maxsize=None)  # unlimited cache – every distinct ts cached once
    def choose_random_id(ts: pd.Timestamp):
        """Return a random (IAsID, AsID) that was active at ts."""
        if pd.isna(ts):
            return None, None

        mask = (
            (df_isik_asutus["IAsKinniKpv"] <= ts)
        )
        if not mask.any():
            return None, None

        chosen = df_isik_asutus.loc[mask].sample(1).iloc[0]
        return chosen["IAsID"], chosen["AsID"]

    # Go through the CSVs
    for filename in os.listdir(OUTPUT_FOLDER):
        if not filename.endswith(".csv"):
            continue

        path = os.path.join(OUTPUT_FOLDER, filename)
        df = pd.read_csv(path, low_memory=False)

        # Relevant date columns present in this file
        ts_cols = [c for c in ["LoodiKpv", "MuudetiKpv", "KustutatiKpv"] if c in df]
        if not ts_cols:  # nothing to do
            continue

        df[ts_cols] = df[ts_cols].apply(pd.to_datetime, errors="coerce")

        # Ensure the targets exist
        for c in ["IAsIDLooja", "IAsIDMuutja", "AsID"]:
            if c not in df:
                df[c] = pd.NA

        # Vectorised fill
        if "LoodiKpv" in df:
            ids = df["LoodiKpv"].map(choose_random_id)
            df[["IAsIDLooja_tmp", "AsID_tmp"]] = pd.DataFrame(ids.tolist(), index=df.index)
            df["IAsIDLooja"].fillna(df["IAsIDLooja_tmp"], inplace=True)
            df["AsID"].fillna(df["AsID_tmp"], inplace=True)
            df.drop(columns=["IAsIDLooja_tmp", "AsID_tmp"], inplace=True)

        if "MuudetiKpv" in df:
            ids = df["MuudetiKpv"].map(choose_random_id)
            df[["IAsIDMuutja_tmp", "AsID_tmp"]] = pd.DataFrame(ids.tolist(), index=df.index)
            df["IAsIDMuutja"].fillna(df["IAsIDMuutja_tmp"], inplace=True)
            df["AsID"].fillna(df["AsID_tmp"], inplace=True)
            df.drop(columns=["IAsIDMuutja_tmp", "AsID_tmp"], inplace=True)

        df.to_csv(path, index=False)
        print(f"Updated: {filename}")


def main(num_records=10000, output_folder="output", seed=42):
    global OUTPUT_FOLDER
    OUTPUT_FOLDER = output_folder

    log_progress("Starting data generation pipeline")
    data = load_data()
    generated_data = generate_tables(data, num_records=num_records, seed=seed)
    save_to_csv(generated_data)

    log_progress("Starting metadata injection")
    df_ias = generated_data["04_isik_asutus.csv"]
    inject_ias_metadata_to_csv_folder(df_ias)

    log_progress("Process completed successfully")
