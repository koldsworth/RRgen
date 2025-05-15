import random
import pandas as pd
from datetime import datetime
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
    Generate a 'Dokument' table for individuals.

    For each row in 'df_source', we create one document record.
    Fields include:
      - DokID, IsID, KdIDDokumendiLiik
      - DokNumber, DokSeeria, DokValjaantudKpv
      - DokKehtivKuniKpv, DokKehtetuAlatesKpv, LoodiKpv, MuudetiKpv, KustutatiKpv
      - Plus codifier fields, free text fields, flags

    Logic:
      1) Set random seed if provided.
      2) Look up KdID values for document type, KEHTIV, KEHTETU statuses.
      3) For each row:
         - Assign unique DokID.
         - Read IsID, validity dates, loodi_kpv if available.
         - If end date is present → KEHTETU, else KEHTIV.
         - 5% chance to mark as deleted (kustutati_kpv).
         - If deleted, immediately set muudeti_kpv = kustutati_kpv.
      4) Assemble all fields into a dictionary.
      5) Return a DataFrame with all documents.

    :param df_source: Source table to build documents from.
    :param kodifikaator: Codifier lookup table.
    :param doc_type: Logical document type ('PASS', 'ID-KAART', 'MUU', etc.).
    :param mode: 'source' or 'random'.
    :param source_cols: Dict specifying column mappings (is_id_col, start_date_col, end_date_col, etc.)
    :param start_doc_id: Starting DokID value.
    :param earliest_date: Earliest LoodiKpv.
    :param seed: Optional random seed.
    :return: DataFrame with documents.
    """

    if seed is not None:
        random.seed(seed)

    if source_cols is None:
        source_cols = {
            'is_id_col': 'IsID',
            'doc_id_col': None,
            'start_date_col': None,
            'end_date_col': None,
            'loodi_date_col': None
        }

    is_id_col = source_cols.get("is_id_col", "IsID")
    start_date_col = source_cols.get("start_date_col", None)
    end_date_col = source_cols.get("end_date_col", None)
    loodi_date_col = source_cols.get("loodi_date_col", None)

    kd_id_dokumendi_liik = get_kdid_for_name(kodifikaator, doc_type)
    kd_id_kehtiv = get_kdid_for_name(kodifikaator, "KEHTIV")
    kd_id_kehtetu = get_kdid_for_name(kodifikaator, "KEHTETU")
    kd_id_riik = get_kdid_for_name(kodifikaator, "EE")

    n_rows = len(df_source)
    chance_issued_as_start_all = [random.random() < 0.9 for _ in range(n_rows)]
    chance_kehtetu_is_end_all = [random.random() < 0.9 for _ in range(n_rows)]
    chance_kustutati_all = [random.random() < 0.05 for _ in range(n_rows)]
    chance_muudeti_kehtiv_all = [random.random() < 1.0 for _ in range(n_rows)]
    # Only relevant in random mode
    chance_ended_random_all = [random.random() <  0.3 for _ in range(n_rows)]

    now = datetime.now()
    rows = []
    doc_id_counter = start_doc_id

    for idx in range(len(df_source)):
        dok_id = doc_id_counter
        doc_id_counter += 1

        is_id = df_source.loc[idx, is_id_col] if is_id_col in df_source.columns else None

        # LoodiKpv
        if loodi_date_col and loodi_date_col in df_source.columns:
            loodi_kpv = df_source.loc[idx, loodi_date_col]
            if pd.isnull(loodi_kpv):
                loodi_kpv = random_date(earliest_date, now)
        else:
            loodi_kpv = random_date(earliest_date, now)

        # Default
        kustutati_kpv = None
        muudeti_kpv = None

        # Validity dates
        if mode == "source":
            start_ = df_source.loc[idx, start_date_col] if start_date_col and start_date_col in df_source.columns else random_date(loodi_kpv, now)
            end_ = df_source.loc[idx, end_date_col] if end_date_col and end_date_col in df_source.columns else None

            dok_valjaantud_kpv = start_ if chance_issued_as_start_all[idx] and pd.notnull(start_) else random_date(loodi_kpv, now)
            dok_kehtiv_alates = start_

            if pd.isnull(end_):
                # KEHTIV
                kd_id_staatus = kd_id_kehtiv
                dok_kehtiv_kuni_kpv = None
                dok_kehtetu_alates_kpv = None
                if chance_muudeti_kehtiv_all[idx]:
                    muudeti_kpv = random_date(loodi_kpv, now)
            else:
                # KEHTETU
                kd_id_staatus = kd_id_kehtetu
                dok_kehtiv_kuni_kpv = end_
                if chance_kehtetu_is_end_all[idx]:
                    dok_kehtetu_alates_kpv = end_
                else:
                    dok_kehtetu_alates_kpv = random_date(end_, now)
                muudeti_kpv = dok_kehtetu_alates_kpv

        else:  # mode == "random"
            dok_valjaantud_kpv = random_date(loodi_kpv, now)
            dok_kehtiv_alates = dok_valjaantud_kpv

            if chance_ended_random_all[idx]:
                # Ended → KEHTETU
                kd_id_staatus = kd_id_kehtetu
                dok_kehtiv_kuni_kpv = random_date(dok_valjaantud_kpv, now)
                dok_kehtetu_alates_kpv = (
                    dok_kehtiv_kuni_kpv if chance_kehtetu_is_end_all[idx] else random_date(dok_kehtiv_kuni_kpv, now)
                )
                muudeti_kpv = dok_kehtetu_alates_kpv
            else:
                # Ongoing → KEHTIV
                kd_id_staatus = kd_id_kehtiv
                dok_kehtiv_kuni_kpv = None
                dok_kehtetu_alates_kpv = None
                if chance_muudeti_kehtiv_all[idx]:
                    muudeti_kpv = random_date(loodi_kpv, now)

        if chance_kustutati_all[idx]:
            kustutati_kpv = random_date(loodi_kpv, now)
            muudeti_kpv = kustutati_kpv  # Always equal when deleted

        row = {
            "IsID": is_id,
            "DokID": dok_id,
            "KdIDDokumendiLiik": kd_id_dokumendi_liik,
            "DokNumber": f"{doc_type}-{idx:07d}",
            "DokSeeria": f"S-{random.randint(100, 999)}",
            "DokValjaantudKpv": dok_valjaantud_kpv,
            "DokKehtivKuniKpv": dok_kehtiv_kuni_kpv,
            "DokKehtetuAlatesKpv": dok_kehtetu_alates_kpv,
            "AsIDValjaandjaAsutus": random.randint(1, 50),
            "KaIDKanne": None,
            "IAsIDLooja": None,
            "LoodiKpv": loodi_kpv,
            "IAsIDMuutja": None,
            "MuudetiKpv": muudeti_kpv,
            "KustutatiKpv": kustutati_kpv,
            "KdIDDokumendiStaatus": kd_id_staatus,
            "DokIDVanem": None,
            "DokKehtivAlates": dok_kehtiv_alates,
            "KdIDRiik": kd_id_riik,
            "DokAsutuseTekst": None,
            "AsIDHaldaja": random.randint(1, 50),
            "DokMarkus": None,
            "DokValjastajaIsikukood": None,
            "DokValjastajaNimi": None,
            "MKaID": None,
            "DokSkaneeritud": None,
            "AsIDAsutusTellija": None,
            "LRProtseduurKirjeldus": None,
            "DokSalastatud": None,
            "KdIDDokAlguseAlus": None
        }
        rows.append(row)

    return pd.DataFrame(rows)
