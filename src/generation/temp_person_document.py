import random
import pandas as pd
from datetime import datetime, timedelta
from src.generation.utils import random_date, get_kdid_for_name

def generate_person_document(
    df_source: pd.DataFrame,
    kodifikaator: pd.DataFrame,
    doc_type: str = "MUU",
    mode: str = "source",
    source_cols: dict = None,
    start_doc_id: int = 1,
    earliest_date: datetime = datetime(1980, 1, 1),
    seed: int = None
) -> pd.DataFrame:
    """
    Generate a DataFrame where each row represents a Document (Dokument) plus IsID linkage.
    (Document table does not have IsID, so it is imporant to drop that column for the actual table).
    Works in two modes:

    1) mode = 'source' (e.g., for something like 'ELUKOHATEADE'):
       - df_source is expected to have:
         * a column for the person ID (by default "IsID"),
         * a 'doc_id_col' (if present, e.g. "DokIDAlus"),
         * 'start_date_col' (e.g. "IAdrKehtibAlatesKpv"),
         * 'end_date_col' (e.g. "IAdrKehtibKuniKpv").
       - Each row in df_source is turned into a document row:
         * If the end_date_col is None => status = KEHTIV, else KEHTETU.
         * doc_type='ELUKOHATEADE' (or any other) is used to find the 
           KdIDDokumendiLiik in 'kodifikaator' (fallback is 999).
         * We add standard columns such as DokID, DokNumber, DokSeeria, LoodiKpv, etc.

    2) mode = 'random' (for arbitrary random documents):
       - df_source can have an IsID column or not.
       - We generate random status, number, series, validity periods, etc.
       - doc_type='MUU' or 'PASS', 'ID-KAART', etc. is used for KdIDDokumendiLiik lookup.

    :param df_source: The source DataFrame.
                     In 'source' mode, we expect it to contain columns for is_id_col, 
                     start_date_col, end_date_col, etc., as guided by 'source_cols'.
                     In 'random' mode, it can be any structure, with or without an IsID.
    :param kodifikaator: A code lookup DataFrame, used to find KdIDDokumendiLiik
                         and KEHTIV/KEHTETU statuses.
    :param doc_type: e.g. 'ELUKOHATEADE', 'MUU', 'PASS', 'ID-KAART', etc.
    :param mode: 'source' or 'random'.
    :param source_cols: A dictionary specifying the column mappings, e.g.:
                        {
                          'is_id_col': 'IsID',
                          'doc_id_col': 'DokIDAlus',
                          'start_date_col': 'IAdrKehtibAlatesKpv',
                          'end_date_col': 'IAdrKehtibKuniKpv',
                          'loodi_date_col': 'LoodiKpv'
                        }
    :param start_doc_id: The initial starting value for DokID counters.
    :param earliest_date: The earliest date to use for random date generation.
    :param seed: Random seed for reproducibility.
    :return: A DataFrame representing the Dokument table (plus an IsID field).
    """

    if seed is not None:
        random.seed(seed)

    # If no source_cols given, define defaults
    if source_cols is None:
        source_cols = {
            'is_id_col': 'IsID',
            'doc_id_col': None,
            'start_date_col': None,
            'end_date_col': None,
            'loodi_date_col': None
        }

    is_id_col = source_cols.get("is_id_col", "IsID")
    doc_id_col = source_cols.get("doc_id_col", None)
    start_date_col = source_cols.get("start_date_col", None)
    end_date_col = source_cols.get("end_date_col", None)
    loodi_date_col = source_cols.get("loodi_date_col", None)

    # Find the KdID for doc_type, fallback = 999 if not found
    kd_id_dokumendi_liik = get_kdid_for_name(kodifikaator, doc_type)

    rows = []
    now = datetime.now()
    doc_id_counter = start_doc_id

    # Iterate over df_source rows
    for i in range(len(df_source)):
        # Decide the DokID
        if mode == "source":
            # Possibly read from doc_id_col if present
            if doc_id_col and doc_id_col in df_source.columns:
                candidate_did = df_source.loc[i, doc_id_col]
                if pd.isnull(candidate_did):
                    dok_id = doc_id_counter
                    doc_id_counter += 1
                else:
                    dok_id = candidate_did
            else:
                # Just assign sequential
                dok_id = doc_id_counter
                doc_id_counter += 1
        else:
            # mode='random'
            dok_id = doc_id_counter
            doc_id_counter += 1

        # If there's an IsID column, get the person's ID
        if is_id_col in df_source.columns:
            is_id_value = df_source.loc[i, is_id_col]
        else:
            is_id_value = None

        # Determine LoodiKpv
        if mode == "source":
            # Possibly read from loodi_date_col
            if loodi_date_col and loodi_date_col in df_source.columns:
                candidate_loodi = df_source.loc[i, loodi_date_col]
                if pd.isnull(candidate_loodi):
                    loodi_kpv = random_date(earliest_date, now)
                else:
                    loodi_kpv = candidate_loodi
            else:
                loodi_kpv = random_date(earliest_date, now)
        else:
            # random
            loodi_kpv = random_date(earliest_date, now)

        # Basic doc number and series
        dok_number = f"{doc_type}-{i:07d}"
        dok_seeria = f"S-{random.randint(100, 999)}"

        # Random issuing institutions
        as_id_valjaandja_asutus = random.randint(1, 50)
        as_id_haldaja = random.randint(1, 50)

        # Initialize placeholders for various fields
        ka_id_kanne = None
        ias_id_looja = None
        ias_id_muutja = None
        kustutati_kpv = None
        dok_id_vanem = None
        dok_asutuse_tekst = None
        dok_markus = None
        dok_valjastaja_isikukood = None
        dok_valjastaja_nimi = None
        mk_aid = None
        dok_skaneeritud = None
        as_id_asutus_tellija = None
        lr_protseduur_kirjeldus = None
        dok_salastatud = None
        kd_id_dok_alguse_alus = None

        # Compute document validity fields
        if doc_type.upper() == "ELUKOHATEADE" and mode == "source":
            # Typically we read from start_date_col/end_date_col
            start_ = None
            end_ = None
            if start_date_col and start_date_col in df_source.columns:
                start_ = df_source.loc[i, start_date_col]
            else:
                start_ = random_date(loodi_kpv, now)

            if end_date_col and end_date_col in df_source.columns:
                end_ = df_source.loc[i, end_date_col]

            # Usually DokValjaantudKpv = start_ (90% chance) or else random
            if random.random() < 0.9 and not pd.isnull(start_):
                dok_valjaantud_kpv = start_
            else:
                dok_valjaantud_kpv = random_date(loodi_kpv, now)

            dok_kehtiv_alates = start_

            # If end_ is null => KEHTIV
            if pd.isnull(end_):
                kd_id_staatus = get_kdid_for_name(kodifikaator, "KEHTIV")
                dok_kehtiv_kuni_kpv = None
                dok_kehtetu_alates_kpv = None
                muudeti_kpv = random_date(loodi_kpv, now)
            else:
                kd_id_staatus = get_kdid_for_name(kodifikaator, "KEHTETU")
                dok_kehtiv_kuni_kpv = end_
                # 90% => kehtetu_alates = end_
                if random.random() < 0.9:
                    dok_kehtetu_alates_kpv = end_
                else:
                    dok_kehtetu_alates_kpv = random_date(end_, now)
                muudeti_kpv = dok_kehtetu_alates_kpv

                # 5% => KustutatiKpv
                if random.random() < 0.05:
                    kustutati_kpv = random_date(loodi_kpv, now)
                else:
                    kustutati_kpv = None

        else:
            # Generic random logic
            dok_valjaantud_kpv = random_date(loodi_kpv, now)
            dok_kehtiv_alates = dok_valjaantud_kpv

            # 30% => KEHTETU
            if random.random() < 0.3:
                kd_id_staatus = get_kdid_for_name(kodifikaator, "KEHTETU")
                dok_kehtiv_kuni_kpv = random_date(dok_valjaantud_kpv, now)

                # 90% => kehtetu_alates = dok_kehtiv_kuni_kpv
                if random.random() < 0.9:
                    dok_kehtetu_alates_kpv = dok_kehtiv_kuni_kpv
                else:
                    dok_kehtetu_alates_kpv = random_date(dok_kehtiv_kuni_kpv, now)

                muudeti_kpv = dok_kehtetu_alates_kpv

                # 5% => we also set KustutatiKpv
                if random.random() < 0.05:
                    kustutati_kpv = random_date(loodi_kpv, now)
            else:
                # KEHTIV
                kd_id_staatus = get_kdid_for_name(kodifikaator, "KEHTIV")
                dok_kehtiv_kuni_kpv = None
                dok_kehtetu_alates_kpv = None
                muudeti_kpv = random_date(loodi_kpv, now)

        # Hardcode the doc's country code
        kd_id_riik = get_kdid_for_name(kodifikaator, 'EE')

        # Make sure MuudetiKpv is equal to KustutatiKpv if the latter exists
        if kustutati_kpv is not None:
            muudeti_kpv = kustutati_kpv

        row = {
            "IsID": is_id_value,  # Link to the person if applicable
            "DokID": dok_id,
            "KdIDDokumendiLiik": kd_id_dokumendi_liik,
            "DokNumber": dok_number,
            "DokSeeria": dok_seeria,
            "DokValjaantudKpv": dok_valjaantud_kpv,
            "DokKehtivKuniKpv": dok_kehtiv_kuni_kpv,
            "DokKehtetuAlatesKpv": dok_kehtetu_alates_kpv,
            "AsIDValjaandjaAsutus": as_id_valjaandja_asutus,
            "KaIDKanne": ka_id_kanne,
            "IAsIDLooja": ias_id_looja,
            "LoodiKpv": loodi_kpv,
            "IAsIDMuutja": ias_id_muutja,
            "MuudetiKpv": muudeti_kpv,
            "KustutatiKpv": kustutati_kpv,
            "KdIDDokumendiStaatus": kd_id_staatus,
            "DokIDVanem": dok_id_vanem,
            "DokKehtivAlates": dok_kehtiv_alates,
            "KdIDRiik": kd_id_riik,
            "DokAsutuseTekst": dok_asutuse_tekst,
            "AsIDHaldaja": as_id_haldaja,
            "DokMarkus": dok_markus,
            "DokValjastajaIsikukood": dok_valjastaja_isikukood,
            "DokValjastajaNimi": dok_valjastaja_nimi,
            "MKaID": mk_aid,
            "DokSkaneeritud": dok_skaneeritud,
            "AsIDAsutusTellija": as_id_asutus_tellija,
            "LRProtseduurKirjeldus": lr_protseduur_kirjeldus,
            "DokSalastatud": dok_salastatud,
            "KdIDDokAlguseAlus": kd_id_dok_alguse_alus
        }

        rows.append(row)

    return pd.DataFrame(rows)
