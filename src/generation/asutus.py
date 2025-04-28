import random
import pandas as pd
from datetime import datetime, timedelta
from src.generation.utils import random_date, get_kdid_for_name


def generate_isik_asutus(
    ias_id: int,
    is_id: int,
    as_id: int,
    df_kodifikaator: pd.DataFrame,
    earliest_date: datetime = datetime(1900, 1, 1),
    seed: int = None
) -> dict:
    """
    Create a single record (dictionary) representing the IsikAsutus relationship.
    This ties a person (IsID) to an institution (AsID) with random dates
    and randomly determined 'KEHTIV'/'KEHTETU' status in Estonian.

    Logic:
      1) Generate a random 'IAsAlgusKpv' between earliest_date and 'now'.
      2) With 99% probability, assign 'IAsKinniKpv' to a date between 'IAsAlgusKpv' and up to 7 days later (but not beyond 'now').
      3) Randomly determine the user’s status:
         - 80% chance => 'KEHTIV'
         - 20% chance => 'KEHTETU'
      4) Set 'IAsKasutajaNimi' and 'IAsKasutajaSalasona' (simple placeholders).
      5) 50% chance to have 'MuudetiKpv' in [LoodiKpv..now].
      6) 5% chance of 'KustutatiKpv' in [IAsAlgusKpv..now], which overrides status to 'KEHTETU'.

    :param ias_id: Unique integer to assign as 'IAsID'.
    :param is_id: Identifier of the person (IsID).
    :param as_id: Identifier of the institution (AsID).
    :param earliest_date: The earliest possible date for any assigned date field.
    :param seed: Random seed for reproducibility.
    :return: A dictionary of columns for the IsikAsutus record.
    """

    if seed is not None:
        random.seed(seed)

    now = datetime.now()

    # 1) IAsAlgusKpv = random date in [earliest_date..now]
    ias_algus_kpv = random_date(earliest_date, now)

    # 2) 99% chance to set IAsKinniKpv up to 7 days after ias_algus_kpv (but not beyond now)
    chance_kinni = random.random()
    if chance_kinni < 0.99:
        max_7_days = ias_algus_kpv + timedelta(days=7)
        max_possible = min(max_7_days, now)
        ias_kinni_kpv = random_date(ias_algus_kpv, max_possible)
    else:
        ias_kinni_kpv = None

    # 3) 80% => KEHTIV; 20% => KEHTETU
    chance_status = random.random()
    if chance_status < 0.8:
        kd_id_staatus = get_kdid_for_name(df_kodifikaator, "KEHTIV")
    else:
        kd_id_staatus = get_kdid_for_name(df_kodifikaator, "KEHTETU")

    # 4) Basic login info
    ias_kasutaja_nimi = f"user_{is_id}_{as_id}"
    ias_kasutaja_salasona = f"pwd{random.randint(1000, 9999)}"
    ias_lisainfo = f"Lisainfo isikule {is_id}"
    ias_kirjete_arv = random.randint(0, 200)
    ias_kontakt_teavitamiseks = f"{ias_kasutaja_nimi}@domain.ee"

    # LoodiKpv is a date in [earliest_date..ias_algus_kpv]
    loodi_kpv = random_date(earliest_date, ias_algus_kpv)

    # 5) 50% chance for MuudetiKpv in [loodi_kpv..now]
    if random.random() < 0.5:
        muudeti_kpv = random_date(loodi_kpv, now)
    else:
        muudeti_kpv = None

    # 6) 5% chance for KustutatiKpv => sets kd_id_staatus to 'KEHTETU'
    if random.random() < 0.05:
        kustutati_kpv = random_date(ias_algus_kpv, now)
        kd_id_staatus = get_kdid_for_name(df_kodifikaator, "KEHTETU")  # Overridden
    else:
        kustutati_kpv = None

    record = {
        "IAsID": ias_id,
        "IsID": is_id,
        "AsID": as_id,
        "IAsAlgusKpv": ias_algus_kpv,
        "IAsKinniKpv": ias_kinni_kpv,
        "IAsKasutajaNimi": ias_kasutaja_nimi,
        "IAsKasutajaSalasona": ias_kasutaja_salasona,
        "KdIDStaatus": kd_id_staatus,
        "IAsLisainfo": ias_lisainfo,
        "IAsKirjeteArv": ias_kirjete_arv,
        "AKpIDPadevusAla": None,
        "IAsMuudatused": None,
        "IAsAndmevaljad": None,
        "IAsDokumendiLiigid": None,
        "IAsKontaktTeavitamiseks": ias_kontakt_teavitamiseks,
        "IAsIDLooja": None,
        "LoodiKpv": loodi_kpv,
        "IAsIDMuutja": None,
        "MuudetiKpv": muudeti_kpv,
        "KustutatiKpv": kustutati_kpv
    }

    return record


def generate_asutus(
    df: pd.DataFrame,
    df_kodifikaator: pd.DataFrame,
    seed: int = None,
    earliest_date: datetime = datetime(1980, 1, 1)
) -> pd.DataFrame:
    """
    Generate a DataFrame of institution ('Asutus') records from an existing DataFrame 'df'
    which might contain columns like ['Nimi', 'Täiendav nimi', 'Staatus', 'Registrikood', 'Aadress'].
    We randomize certain date fields and status in Estonian (e.g. 'KEHTIV', 'KEHTETU').

    Logic in full:
      1) For each row in 'df', generate:
         - AsAlguseKpv (random in [earliest_date..now])
         - AsLopuKpv (~20% chance), random in [AsAlguseKpv..now]
         - LoodiKpv (random in [earliest_date..AsAlguseKpv])
         - MuudetiKpv (~50% chance), random in [LoodiKpv..now]
         - KustutatiKpv (~5% chance), but only if there's an AsLopuKpv,
           in which case we set KustutatiKpv = AsLopuKpv
      2) If 'df.loc[i, "Staatus"]' is 'Registrisse kantud', we mark 'KdIDStaatus' as 'KEHTIV',
         otherwise 'KEHTETU'.
      3) Fill out other columns, e.g. AsKontakt, AsRegNumber, AsLyhiNimi, AsPikkNimi, etc.
      4) Return the resulting DataFrame.

    :param df: Input DataFrame with institution data, typically read from an Excel file.
               It should include columns such as:
               ['Nimi', 'Täiendav nimi', 'Staatus', 'Registrikood', 'Aadress'].
    :param seed: Random seed for reproducibility.
    :param earliest_date: The earliest possible date for any date fields assigned in this function.
    :return: A DataFrame with columns for the 'Asutus' table.
    """

    if seed is not None:
        random.seed(seed)

    now = datetime.now()
    data = []

    for i in range(len(df)):
        row = df.iloc[i]

        # 1) AsAlguseKpv
        as_alguse_kpv = random_date(earliest_date, now)

        # ~20% chance of AsLopuKpv
        as_lopu_kpv = None
        if random.random() < 0.2:
            as_lopu_kpv = random_date(as_alguse_kpv, now)

        # LoodiKpv in [earliest_date..as_alguse_kpv]
        loodi_kpv = random_date(earliest_date, as_alguse_kpv)

        # ~50% chance of MuudetiKpv
        if random.random() < 0.5:
            muudeti_kpv = random_date(loodi_kpv, now)
        else:
            muudeti_kpv = None

        # ~5% chance of KustutatiKpv, but only if we have AsLopuKpv
        if as_lopu_kpv is not None and random.random() < 0.05:
            kustutati_kpv = as_lopu_kpv
        else:
            kustutati_kpv = None

        # Basic contact info
        kontakt = f"info@asutus{i}.ee"

        # Random institution ID from kodifikaator
        kd_id_asutuse_liik = random.choice(df_kodifikaator.loc[df_kodifikaator['KdKodifikaatoriKood'] == 10, 'KdID'].tolist())

        # Example code for 'AsAsutuseKood' as a random integer
        as_asutuse_kood = f"AS-{random.randint(1000, 9999)}"

        # Example random AkpID (address component ID)
        akp_id = random.randint(1, 500)

        # Derive short/pikk name
        if pd.isnull(row.get('Täiendav nimi')):
            as_pikk_nimi = row['Nimi']
        else:
            as_pikk_nimi = row['Täiendav nimi']

        # 2) If 'Staatus' == 'Registrisse kantud', we set 'KEHTIV', else 'KEHTETU'
        if row['Staatus'] == 'Registrisse kantud':
            kd_id_staatus = get_kdid_for_name(df_kodifikaator, "KEHTIV")
        else:
            kd_id_staatus = get_kdid_for_name(df_kodifikaator, "KEHTETU")

        kd_id_riik = get_kdid_for_name(df_kodifikaator, 'EE')

        # Build the record
        record = {
            "AsID": i + 1,                      # sequential ID
            "AsAlguseKpv": as_alguse_kpv,       # institution start date
            "AsLopuKpv": as_lopu_kpv,           # optional end date
            "AsKontakt": kontakt,               # contact info
            "AsRegNumber": row['Registrikood'], # registration code
            "KdIDAsutuseLiik": kd_id_asutuse_liik,
            "AsLyhiNimi": row['Nimi'],
            "AsPikkNimi": as_pikk_nimi,
            "IAsIDLooja": None,
            "LoodiKpv": loodi_kpv,
            "IAsIDMuutja": None,
            "MuudetiKpv": muudeti_kpv,
            "KustutatiKpv": kustutati_kpv,
            "AsMuuKeelesNimi": "Muu keeles nimi",
            "KdIDStaatus": kd_id_staatus,
            "AsIDYlemus": None,
            "KdIDRiik": kd_id_riik,
            "AsAsutuseAadress": row['Aadress'] if 'Aadress' in row else None,
            "AsInglise": "Inglise keelne nimetus",
            "AsSaksa": "Saksa keelne nimetus",
            "AsPrantsuse": "Prantsuse keelne nimetus",
            "AsAsutuseKood": as_asutuse_kood,
            "AkpID": akp_id
        }

        data.append(record)

    return pd.DataFrame(data)

def build_isik_asutus(df, df_kodifikaator):
    """
    Build the 'IsikAsutus' (person-institution) DataFrame.

    :param df: A DataFrame of people from `generate_temp_relatsionships`,
    :param df_kodifikaator: A DataFrame containing kodifikaator
    :return: A DataFrame for the IsikAsutus
    """
    records = []
    ias_id_counter = 1
    for idx, row in df.iterrows():
        as_id = row["AsID"]
        # Check that as_id is not  None
        if pd.notnull(as_id):
            is_id = row["IsID"]

            # Create 1 isik_asutus row
            record_ia = generate_isik_asutus(
                ias_id=ias_id_counter,
                is_id=is_id,
                as_id=int(as_id),
                df_kodifikaator=df_kodifikaator
            )
            ias_id_counter += 1
            records.append(record_ia)

    df_isik_asutus = pd.DataFrame(records)
    return df_isik_asutus
