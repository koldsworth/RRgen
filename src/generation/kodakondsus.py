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

    For each person in 'df_isik', we create one (or more, if desired) citizenship record.
    Fields include:
      - KodID (unique primary key)
      - IsID (link to the person)
      - KdIDRiik (the country code from 'kodifikaator')
      - KodKehtibAlates, KodKehtibKuni (valid-from and valid-until)
      - DokIDAlguseAlus, DokIDLopuAlus (references to documents in a given ID range)
      - KdIDStaatus (citizenship status, e.g. 'KEHTIV'/'KEHTETU')
      - IAsIDLooja, IAsIDMuutja, LoodiKpv, MuudetiKpv, KustutatiKpv

    Logic:
      1) If seed is given, set the random seed.
      2) Look up KdIDs for 'KEHTIV' and 'KEHTETU'.
      3) We iterate over each person in 'df_isik'. We take the person's 'Kodakondsus' field 
         ('EE' or 'MUU'), plus their birth date (if any).
      4) For each person, we generate at least one citizenship record:
         - KdIDRiik is set if 'Kodakondsus' is 'EE' by default. If 'MUU', it can be None or vary.
         - KodKehtibAlates is a random date in [max(birthDate, earliest_date) .. now].
         - With 20% probability, set KodKehtibKuni, which implies a 'KEHTETU' status; otherwise 'KEHTIV'.
         - We assign references to documents (DokIDAlguseAlus, DokIDLopuAlus) from a 
           doc ID counter starting at 'start_doc_id'.
         - Set LoodiKpv <= KodKehtibAlates, plus random changes for MuudetiKpv, KustutatiKpv, etc.
      5) Return a DataFrame with all 'Kodakondsus' records.

    :param df_isik: Person table with at least columns ['IsID', 'Kodakondsus'='EE'/'MUU', 'Sünniaeg'].
    :param kodifikaator: Code table, from which we look up 'KEHTIV'/'KEHTETU'.
    :param start_doc_id: Starting point for document IDs used as references in DokIDAlguseAlus, etc.
    :param seed: Random seed for reproducibility.
    :param earliest_date: Earliest possible date for assigning KodKehtibAlates.
    :return: A Pandas DataFrame named 'Kodakondsus'.
    """


    # Set random seed if provided
    if seed is not None:
        random.seed(seed)

    kd_id_kehtiv = get_kdid_for_name(kodifikaator, 'KEHTIV')
    kd_id_kehtetu = get_kdid_for_name(kodifikaator, 'KEHTETU')

    now = datetime.now()
    doc_id_counter = start_doc_id
    records = []
    kod_id_counter = 1

    for idx, row in df_isik.iterrows():
        is_id = row["IsID"]
        person_kod = row.get("Kodakondsus", "EE")  # 'EE' or 'MUU'
        syn = row.get("Sünniaeg", None)

        # Decide how many citizenship records for each person (here, always 1 for simplicity)
        count_citizenships = 1

        for cidx in range(count_citizenships):
            # If person_kod == 'EE', we look up 'EE' in kodifikaator; else None or random
            if person_kod == "EE":
                kd_riik = get_kdid_for_name(kodifikaator, "EE")
            else:
                kd_riik = None

            # KodKehtibAlates: random in [max(syn, earliest_date)..now]
            if syn and not pd.isnull(syn):
                kod_start_candidate = syn
            else:
                kod_start_candidate = earliest_date

            kod_kehtib_alates = random_date(kod_start_candidate, now)

            # 20% chance we set KodKehtibKuni => KEHTETU
            if random.random() < 0.2:
                kod_kehtib_kuni = random_date(kod_kehtib_alates, now)
                kd_id_status = kd_id_kehtetu
            else:
                kod_kehtib_kuni = None
                kd_id_status = kd_id_kehtiv

            # DokIDAlguseAlus is always some doc, from our doc ID counter
            dok_id_algus = doc_id_counter + 1

            # If there's an end date, we might set DokIDLopuAlus as well
            if kod_kehtib_kuni:
                if random.random() < 0.5:
                    dok_id_lopp = dok_id_algus + 1
                    doc_id_counter += 1
                else:
                    dok_id_lopp = None
            else:
                dok_id_lopp = None

            # LoodiKpv <= kod_kehtib_alates
            loodi = random_date(earliest_date, kod_kehtib_alates)
            kustutati = None
            muudeti = None

            # 50% chance for MuudetiKpv
            if random.random() < 0.5:
                if kod_kehtib_kuni:
                    muudeti = random_date(loodi, now)
            else:
                muudeti = None

            # 5% chance KustutatiKpv
            if random.random() < 0.05:
                kustutati = random_date(loodi, now)
                kd_id_status = kd_id_kehtetu
            else:
                kustutati = None

            if kustutati:
                muudeti = kustutati

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
            doc_id_counter += 1

    return pd.DataFrame(records)
