import random
import pandas as pd
from datetime import datetime
from faker import Faker
from src.generation.utils import random_date, get_kdid_for_name, generate_isikukood

faker = Faker('et_EE')  # For Estonian-style names.


def generate_isik(
        temp_relationships: pd.DataFrame,
        isikuaadress: pd.DataFrame,
        kodifikaator: pd.DataFrame,
        seed: int = None
) -> pd.DataFrame:
    """
    Build the 'Isik' table from an existing relationships DataFrame ('temp_relationships'),
    optionally merging with address data from 'isikuaadress' and code references from
    'kodifikaator'. The result is a DataFrame where each row represents one person.

    Logic:
      If seed is provided, set the random seed for reproducibility.
      1) Iterate over each row in 'temp_relationships'. Create basic fields from 'temp_relationships'
         - Extract or generate fields like gender ('Sugu'), birthdate ('Sünniaeg'),
           death date ('Surmaaeg'), and education ('Haridus').
         - Retrieve KOV or address data from 'isikuaadress'.
         - Set a status code ('ELUS'/'SURNUD') + record status ('REGISTRIS'/'ARHIIVIS')
           via get_kdid_for_name, referencing 'kodifikaator'.
         - Generate a fake name (using Faker), plus a simplistic isikukood (Estonian personal code).
         - Possibly set arrival and departure dates (with a 20% chance each).
      2) Collect all these fields into a dictionary record and append to 'all_records'.
      Return a DataFrame containing all the new 'Isik' records.

    :param temp_relationships: DataFrame of people with columns like 
                               ['IsID','Sugu','Sünniaeg','Surmaaeg','Haridus','Aadress','KOV', etc.].
    :param isikuaadress: DataFrame linking people to address periods. 
                         Should have columns like ['AdrID','IsID','IAdrKehtibAlatesKpv', etc.].
    :param kodifikaator: DataFrame with code references. 
                         E.g., used by get_kdid_for_name for 'ELUS','SURNUD','REGISTRIS','ARHIIVIS'.
    :param seed: Random seed (optional).
    :return: A DataFrame of 'Isik' records, one row per person.
    """

    if seed is not None:
        random.seed(seed)

    now = datetime.now()

    chance_muudeti_all = [random.random() < 0.5 for _ in range(len(temp_relationships))]

    all_records = []

    for idx, row in temp_relationships.iterrows():
        # -------------------------------------------------------------------
        # 1) Basic fields from temp_relationships
        # -------------------------------------------------------------------
        person_id = row["IsID"]
        sugu = row["Sugu"] if pd.notnull(row["Sugu"]) else random.choice(["MEES", "NAINE"])
        synniaeg = row["Sünniaeg"] if not pd.isnull(row["Sünniaeg"]) else None
        surmaaeg = row["Surmaaeg"] if not pd.isnull(row["Surmaaeg"]) else None
        haridus = row["Haridus"] if pd.notnull(row["Haridus"]) else None
        saabus_eesti = row['IsSaabusEesti'] if pd.notnull(row['IsSaabusEesti']) else None
        lahkus_eestist = row['IsLahkusEestist'] if pd.notnull(row['IsLahkusEestist']) else None

        # If 'KOV' was provided, we interpret it as 'AKpID' (some local ID). 
        # Also, we check if the user has an address => find arrival date in isikuaadress if any.
        akp_id = row["KOV"] if pd.notnull(row["KOV"]) else None
        aadress_id = row["Aadress"] if pd.notnull(row["Aadress"]) else None

        # Attempt to find arrival date to KOV by cross-referencing isikuaadress
        # E.g. pick the earliest IAdrKehtibAlatesKpv for that person & address
        df_saabumised = isikuaadress.loc[
            (isikuaadress["AdrID"] == aadress_id) &
            (isikuaadress["IsID"] == person_id),
            "IAdrKehtibAlatesKpv"
        ]
        saabus_kov = df_saabumised.values[0] if len(df_saabumised) > 0 else None

        # Person status: if alive => "ELUS"/"REGISTRIS", if deceased => "SURNUD"/"ARHIIVIS"
        if surmaaeg is None:
            kd_id_isiku_staatus = get_kdid_for_name(kodifikaator, "ELUS")
            kd_id_kirje_staatus = get_kdid_for_name(kodifikaator, "REGISTRIS")
        else:
            kd_id_isiku_staatus = get_kdid_for_name(kodifikaator, "SURNUD")
            kd_id_kirje_staatus = get_kdid_for_name(kodifikaator, "ARHIIVIS")

        # Fake names
        eesnimi = faker.first_name_male() if sugu == "MEES" else faker.first_name_female()
        perenimi = faker.last_name()

        # Generate isikukood
        isikukood = generate_isikukood(sugu, synniaeg)

        # For demonstration, let's say "KdElemendiKood" == "EST" => this is Estonian
        # If we want a random foreign code, we do so with some probability
        # Here we just pick 'EST' for rahvus, or do a fallback if we want variety
        est_rows = kodifikaator.loc[
            (kodifikaator["KdElemendiKood"] == "EST") &
            (kodifikaator["KdKodifikaatoriKood"] == 2),
            "KdID"
        ]
        if len(est_rows) > 0:
            kd_id_rahvus_est = est_rows.iloc[0]
        else:
            kd_id_rahvus_est = None

        # We assign that by default
        # Here we also simplify and put one value for mother tongue, nationality and citizenship
        kd_id_rahvus = kd_id_rahvus_est
        kd_id_emakeel = kd_id_rahvus_est
        kd_id_kodakondsus = kd_id_rahvus_est

        # Different nationality, citizenship and mother tongue for people coming to Estonia
        if saabus_eesti:
            kd_id_rahvus = kodifikaator.loc[
                (kodifikaator["KdElemendiKood"] != "EST") & (kodifikaator["KdKodifikaatoriKood"] == 2), 'KdID'].iloc[0]

            # For the sake of example, pick a random non-EST code
            non_est = kodifikaator.loc[
                (kodifikaator["KdElemendiKood"] != "EST") &
                (kodifikaator["KdKodifikaatoriKood"] == 2),
                "KdID"
            ]
            if len(non_est) > 0:
                kd_id_rahvus = random.choice(non_est.values.tolist())
                kd_id_emakeel = kd_id_rahvus
                kd_id_kodakondsus = kd_id_rahvus

        if synniaeg:
            loodi_kpv = synniaeg
        else:
            loodi_kpv = now

        if chance_muudeti_all[idx]:
            muudeti_kpv = random_date(loodi_kpv, now)
        else:
            muudeti_kpv = None

        # -------------------------------------------------------------------
        # 2) Build the final record
        # -------------------------------------------------------------------
        record = {
            "IsID": person_id,  # Unique key
            "IsIsikukood": isikukood,  # Personal code
            "IsEesnimi": eesnimi,
            "IsPerenimi": perenimi,
            "IsSurmaaeg": surmaaeg,
            "KdIDHaridus": haridus,  # Education code
            "KdIDEmakeel": kd_id_emakeel,  # Language code
            "KdIDRahvus": kd_id_rahvus,  # Ethnicity
            "KdIDPerekonnaseis": None,  # Not implemented here
            "KdIDKodakondsus": kd_id_kodakondsus,  # Citizenship
            "IsIsanimi": faker.first_name_male(),  # Father name as a placeholder
            "IsSynnijargneNimi": eesnimi,  # Person's original birth name
            "KdIDIsikuStaatus": kd_id_isiku_staatus,  # ELUS/SURNUD
            "KdIDKirjeStaatus": kd_id_kirje_staatus,  # REGISTRIS/ARHIIVIS
            "KdIDPohjus": None,  # Reason for archival, if any
            "KdIDSugu": sugu,  # "MEES"/"NAINE"
            "isSynniaeg": synniaeg,  # Actual birthdate
            "IsSaabusEesti": saabus_eesti,
            "IsLahkusEestist": lahkus_eestist,
            "IsSaabusKOViKpv": saabus_kov,  # Date of arrival in KOV if known
            "AKpID": akp_id,  # KOV aadress component id
            "IsEesnimiEri": eesnimi,
            "IsPerenimiEri": perenimi,
            "IdKiht": None,
            "IAsIDLooja": None,
            "LoodiKpv": loodi_kpv,  # Record creation time
            "IAsIDMuutja": None,
            "MuudetiKpv": muudeti_kpv,  # A random modification date in the last 5 years
            "KustutatiKpv": None  # Marked as deleted. Not used here
        }

        all_records.append(record)

    return pd.DataFrame(all_records)
