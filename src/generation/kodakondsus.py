import random
import pandas as pd
from datetime import datetime

from src.generation.utils import get_kdid_for_name, random_date


def generate_kodakondsus(
        df_isik: pd.DataFrame,
        kodifikaator: pd.DataFrame,
        start_doc_id: int = 1000,
        seed: int = None,
        earliest_date: datetime = datetime(1950, 1, 1)
) -> pd.DataFrame:
    """
    Generate a 'Kodakondsus' (citizenship) table for individuals.

    For each person in 'df_isik', we create one (or more) citizenship record(s).
    Fields include:
      - KodID (unique primary key)
      - IsID (link to the person)
      - KdIDRiik (the country code from 'kodifikaator')
      - KodKehtibAlates, KodKehtibKuni (valid-from and valid-until dates)
      - DokIDAlguseAlus, DokIDLopuAlus (references to related documents)
      - KdIDStaatus (citizenship status: 'KEHTIV' or 'KEHTETU')
      - IAsIDLooja, IAsIDMuutja, LoodiKpv, MuudetiKpv, KustutatiKpv (audit fields)

    Logic:
      1) If seed is provided, set the random seed for reproducibility.
      2) Look up KdIDs for statuses 'KEHTIV' and 'KEHTETU' from the 'kodifikaator'.
      3) Iterate over each person:
         - Determine citizenship base date.
         - KodKehtibAlates is between base date and now.
         - LoodiKpv is between earliest_date and KodKehtibAlates - 1 day.
         - 20% chance for ended citizenship (KEHTETU).
         - If ended, 5% chance to mark as deleted (KustutatiKpv).
         - MuudetiKpv is generated after LoodiKpv, except if KustutatiKpv is set (must be equal).
         - Document IDs are properly incremented.
      4) Return the resulting DataFrame.
    """
    if seed is not None:
        random.seed(seed)

    chance_ended_all = [random.random() < 0.2 for _ in range(len(df_isik))]
    chance_kustutati_all = [random.random() < 0.05 for _ in range(len(df_isik))]
    chance_muudeti_all = [random.random() < 0.5 for _ in range(len(df_isik))]

    kd_id_kehtiv = get_kdid_for_name(kodifikaator, 'KEHTIV')
    kd_id_kehtetu = get_kdid_for_name(kodifikaator, 'KEHTETU')

    now = datetime.now()
    doc_id_counter = start_doc_id
    kod_id_counter = 1
    records = []

    for idx, row in df_isik.iterrows():
        is_id = row["IsID"]
        syn = row.get("Sünniaeg", None)
        saab = row.get("IsSaabusEesti", None)
        kd_riik = row.get("KdIDKodakondsus", None)

        kod_start_candidate = saab if pd.notnull(saab) else (syn if pd.notnull(syn) else earliest_date)
        kod_kehtib_alates = random_date(max(earliest_date, kod_start_candidate), now)

        # Generate loodi strictly earlier than alates
        if kod_kehtib_alates > earliest_date:
            loodi = random_date(earliest_date, kod_kehtib_alates - pd.Timedelta(days=1))
        else:
            loodi = earliest_date

        dok_id_algus = doc_id_counter
        doc_id_counter += 1

        if chance_ended_all[idx]:
            # Citizenship ended
            kod_kehtib_kuni = random_date(kod_kehtib_alates, now)
            kd_id_status = kd_id_kehtetu

            dok_id_lopp = doc_id_counter
            doc_id_counter += 1

            if chance_kustutati_all[idx]:
                kustutati = kod_kehtib_kuni
                muudeti = kustutati
            else:
                kustutati = None
                muudeti = kod_kehtib_kuni

        else:
            # Citizenship still valid
            kod_kehtib_kuni = None
            kd_id_status = kd_id_kehtiv
            dok_id_lopp = None
            kustutati = None
            muudeti = random_date(loodi, now) if chance_muudeti_all[idx] else None

        record = {
            "KodID": kod_id_counter,
            "IsID": is_id,
            "KdIDRiik": kd_riik,
            "KodKehtibAlates": kod_kehtib_alates,
            "KodKehtibKuni": kod_kehtib_kuni,
            "DokIDAlguseAlus": dok_id_algus,
            "DokIDLopuAlus": dok_id_lopp,
            "KdIDStaatus": kd_id_status,
            "IAsIDLooja": None,
            "IAsIDMuutja": None,
            "LoodiKpv": loodi,
            "MuudetiKpv": muudeti,
            "KustutatiKpv": kustutati
        }

        records.append(record)
        kod_id_counter += 1

    return pd.DataFrame(records)
