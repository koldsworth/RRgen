import random
import pandas as pd
from datetime import datetime, timedelta
from src.generation.utils import get_kdid_for_name, random_date


def generate_aadress(
    df: pd.DataFrame,
    df_kodifikaator: pd.DataFrame,
    num_records: int = 10,
    seed: int = None
) -> pd.DataFrame:
    """
    Generate a specified number of random address records from the input DataFrame.

    This function randomly selects address records from the provided dataset and 
    augments them with additional attributes such as status codes and random postal codes.

    Logic:
      1) Randomly sample 'num_records' rows from the input DataFrame.
      2) Assign a status code (`KdID`) based on the address status (`AADR_OLEK`).
      3) Generate additional fields, such as postal codes and country IDs.
      4) Format the data into a structured DataFrame.

    :param df: The source DataFrame containing address data (e.g., loaded from 'aadress.csv').
                Expected columns: 'ADR_ID', 'AADR_OLEK', 'TASE1_KOOD', 'TAISAADRESS', etc.
    :param df_kodifikaator: A lookup table for status codes (KdID) and country codes.
    :param num_records: default=10, the number of address records to generate.
    :param seed: Seed for reproducibility in random sampling.
    :return: A new DataFrame containing the generated address records.
    """
    if seed is not None:
        random.seed(seed)

    # Precompute status mapping to avoid redundant function calls
    status_mapping = {
        'K': get_kdid_for_name(df_kodifikaator, 'KEHTIV'),
        'O': get_kdid_for_name(df_kodifikaator, 'OOTEL'),
        'V': get_kdid_for_name(df_kodifikaator, 'VIGANE'),
        None: get_kdid_for_name(df_kodifikaator, 'KEHTETU')  # Default case
    }

    # Select `num_records` random addresses efficiently
    sampled_df = df.sample(n=num_records, replace=True).copy()

    # Ensure necessary columns exist before mapping
    if 'AADR_OLEK' in sampled_df:
        sampled_df["KdIDAdressiStaatus"] = sampled_df["AADR_OLEK"].map(status_mapping).fillna(status_mapping[None])

    # Generate random postal codes efficiently
    sampled_df["AdrSihtnumber"] = [random.randint(10000, 99999) for _ in range(len(sampled_df))]

    # Define column mapping with optional values
    output_columns = {
        "AdrID": "ADR_ID",
        "AkpIDTase0": None,  # Will be filled later
        "AkpIDTase1": "TASE1_KOOD",
        "AkpIDTase2": "TASE2_KOOD",
        "AkpIDTase3": "TASE3_KOOD",
        "AkpIDTase4": "TASE4_KOOD",
        "AkpIDTase5": "TASE5_KOOD",
        "AkpIDTase6": "TASE6_KOOD",
        "AkpIDTase7": "TASE7_KOOD",
        "AkpIDTase8": "TASE8_KOOD",
        "AdrAadress": "TAISAADRESS",
        "KdIDAdressiStaatus": "KdIDAdressiStaatus",
        "AdrOige": None,
        "AdrSihtnumber": "AdrSihtnumber",
        "AdrMajaNr": "TASE7_NIMETUS",
        "ADS_ADR_ID": None,
        "ADS_ADR_TEKST": None,
        "ADS_ADS_OID": "ADS_OID",
        "ADS_ADR_MUUDETI": None,
        "ADS_ORIG_TUNNUS": None,
        "ADS_MUUD_OBJEKTID": None,
        "ADS_KOODAADRESS": None,
        "ADS_ADOB_ID": "ADOB_ID",
        "ADSILID": None,
        "IAsIDLooja": None,
        "LoodiKpv": "ADS_KEHTIV",
        "IAsIDMuutja": None,
        "MuudetiKpv": None,
        "KustutatiKpv": None
    }

    # Ensure missing columns exist before renaming
    for col in output_columns.values():
        if col is not None and col not in sampled_df.columns:
            sampled_df[col] = None  # Add missing columns

    # Create a new DataFrame with mapped columns
    result_df = sampled_df.rename(columns={v: k for k, v in output_columns.items() if v})

    # Ensure all required columns exist before selecting them
    for col in output_columns:
        if col not in result_df.columns:
            result_df[col] = None  # Add missing columns dynamically

    # Select only the required columns
    result_df = result_df[list(output_columns.keys())]

    # Assign static values (country ID, etc.)
    result_df["AkpIDTase0"] = get_kdid_for_name(df_kodifikaator, 'EE')

    return result_df


def generate_aadress_komponent(
    df: pd.DataFrame,
    df_kodifikaator: pd.DataFrame,
    seed: int = None
) -> pd.DataFrame:
    """
    Efficiently generate address-component records (a hierarchy of address parts).

    Logic:
      1) Convert validity period ('KEHTIV' → 'KEHTETU') to datetime.
      2) Assign a creation date randomly before 'KEHTIV'.
      3) Assign status codes based on presence of 'KEHTETU'.
      4) Ensure all required columns exist before renaming.
      5) Return a structured DataFrame.


    :param df: The source DataFrame of address components (e.g., 'aadresskomponent.csv').
    :param df_kodifikaator:  Lookup table for status codes (KdID).
    :param seed: Seed for reproducibility in random sampling.
    :return: A new DataFrame containing the generated address-component records.
    """

    if seed is not None:
        random.seed(seed)

    # Make a copy to avoid `SettingWithCopyWarning`
    df = df.copy()

    # Convert 'KEHTIV' and 'KEHTETU' to datetime (vectorized)
    df["KEHTIV"] = pd.to_datetime(df["KEHTIV"], format='%d.%m.%Y %H:%M:%S', errors='coerce')
    df["KEHTETU"] = pd.to_datetime(df["KEHTETU"], format='%d.%m.%Y %H:%M:%S', errors='coerce')

    # Precompute status mapping
    kd_kehtiv = get_kdid_for_name(df_kodifikaator, 'KEHTIV')
    kd_kehtetu = get_kdid_for_name(df_kodifikaator, 'KEHTETU')

    # Generate creation dates efficiently
    df["LoodiKpv"] = df["KEHTIV"].apply(lambda x: random_date(x - timedelta(days=365 * 10), x) if pd.notnull(x) else None)

    # Assign status IDs efficiently
    df["KdIDStaatus"] = df["KEHTETU"].apply(lambda x: kd_kehtetu if pd.notnull(x) else kd_kehtiv)

    # Define expected column mappings
    output_columns = {
        "AKpID": None,  # Will be assigned separately
        "AKpKood": "KOOD",
        "AKpIDVanem": "YLEMKOMP_KOOD",
        "AKpLyhikeNimetus": "NIMETUS",
        "AKpPikkNimetus": "NIMETUS_LIIGIGA",
        "AKpTaseNR": "TASE",
        "AKpKehtivAlatesKpv": "KEHTIV",
        "AKpKehtivKuniKpv": "KEHTETU",
        "LoodiKpv": "LoodiKpv",
        "KdIDStaatus": "KdIDStaatus"
    }

    # Ensure missing columns exist before renaming
    for col in output_columns.values():
        if col is not None and col not in df.columns:
            df[col] = None  # Add missing columns dynamically

    # Create a new DataFrame with mapped columns
    result_df = df.rename(columns={v: k for k, v in output_columns.items() if v}).copy()

    # Assign AKpID (unique ID)
    result_df["AKpID"] = result_df.index

    # Ensure missing columns exist before selecting them
    missing_columns = [
        "IAsIDLooja", "IAsIDMuutja", "MuudetiKpv", "KustutatiKpv",
        "KdIDAkpLiik", "AkpUnikaalsus", "AKpIDAds", "TsID",
        "AKpTase7NumberOsa", "AKpRoopNimi", "ADS_KOMP_ID",
        "ADS_TASE", "ADS_KOOD", "ADS_NIMETUS", "ADS_NIMETUS_LIIGIGA",
        "ADS YLEKOMP_TASE", "ADS_YLEKOMP_KOOD", "ADS_KETHIV",
        "ADS_KEHTETU", "ADS_MUUDETI", "ADS_RR_NIMED", "ADSILID"
    ]

    for col in missing_columns:
        result_df[col] = None  # Add missing columns dynamically

    # Select only the required columns
    result_df = result_df[list(output_columns.keys()) + missing_columns]

    return result_df