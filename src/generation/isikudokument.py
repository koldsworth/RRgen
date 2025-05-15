import random
import pandas as pd
from datetime import datetime
from src.generation.utils import random_date


def generate_isikudokument(
        df_docs: pd.DataFrame,
        df_isik: pd.DataFrame,
        doc_isik_merge_on: str = "IsID",
        seed: int = None
) -> pd.DataFrame:
    """
    Build the bridging table "IsikuDokument":
      - IDokID (unique PK)
      - IsID
      - DokID
      - IDokIsikukood, IDokEesnimi, IDokPerenimi, IDokIsanimi
      - IDokVanaEesnimi, IDokVanaPerenimi, IDokVanaIsikukood
      - KdIDIsikuRoll
      - AsID
      - IAsIDLooja
      - LoodiKpv
      - IAsIDMuutja
      - MuudetiKpv
      - KustutatiKpv

    Logic:
      1) Merge df_docs and df_isik on the specified 'doc_isik_merge_on' (default 'IsID'),
         retrieving both document and person fields in one row.
      2) For each merged row, create a bridging record:
         - IDokID: incremental counter
         - DokID: df_docs["DokID"]
         - IsID: df_isik["IsID"]
         - IDokIsikukood = df_isik["IsIsikukood"] or None
         - IDokEesnimi, IDokPerenimi, etc. from the person
         - 5% chance of "IDokVana..." fields
         - AsID: doc's "AsIDValjaandjaAsutus" or random
         - IAsIDLooja, LoodiKpv, IAsIDMuutja, MuudetiKpv, KustutatiKpv:
           assigned from document or random logic

    :param df_docs: The "Dokumendid" DataFrame, containing at least:
                    ["DokID", "KdIDDokumendiLiik", "AsIDValjaandjaAsutus",
                     "LoodiKpv", "MuudetiKpv", "KustutatiKpv", "IsID"].
                    (We expect "IsID" if the doc is linked to a person.)
    :param df_isik: The "Isikud" DataFrame, containing at least:
                    ["IsID", "IsIsikukood", "IsEesnimi", "IsPerenimi", "IsIsanimi", ...].
    :param doc_isik_merge_on: The column on which to merge (default "IsID").
    :param seed: Random seed (optional).
    :return: A new DataFrame representing the bridging table, "IsikuDokument".
    """

    if seed is not None:
        random.seed(seed)

    now = datetime.now()

    # Merge (left = df_docs, right = df_isik) on "IsID" (or doc_isik_merge_on)
    merged = pd.merge(df_docs, df_isik, left_on=doc_isik_merge_on, right_on="IsID", how="inner")

    records = []
    i_dok_id_counter = 1

    for idx, row in merged.iterrows():
        chance_vana_fields = random.random() < 0.05
        chance_bridging_update = random.random() < 0.5
        chance_bridging_deleted = random.random() < 0.05
        
        dok_id = row["DokID"]
        is_id = row["IsID"]

        # Possibly "IsIsikukood", "IsEesnimi", "IsPerenimi", "IsIsanimi"
        isikukood = row.get("IsIsikukood", None)
        eesnimi = row.get("IsEesnimi", None)
        perenimi = row.get("IsPerenimi", None)
        isanimi = row.get("IsIsanimi", None)

        # A set chance we set "old" fields
        if chance_vana_fields:
            vana_eesnimi = "Vana-" + (eesnimi if eesnimi else "")
            vana_perenimi = "Vana-" + (perenimi if perenimi else "")
            vana_isikukood = f"OLD-{random.randint(1000, 9999)}"
        else:
            vana_eesnimi = None
            vana_perenimi = None
            vana_isikukood = None

        # AsID -> from doc's "AsIDValjaandjaAsutus" or random
        as_id = row.get("AsIDValjaandjaAsutus", None)

        # For bridging creation date, pick a random date between doc's LoodiKpv and now
        doc_loodi = row.get("LoodiKpv", datetime(2000, 1, 1))
        bridging_loodi = random_date(doc_loodi, now)

        # Possibly set MuudetiKpv, KustutatiKpv
        bridging_muudeti = random_date(bridging_loodi, now) if chance_bridging_update else None
        bridging_kustutati = None

        # A set chance bridging is "deleted"
        if chance_bridging_deleted:
            bridging_kustutati = random_date(bridging_loodi, now)
            bridging_muudeti = bridging_kustutati

        record = {
            "IDokID": i_dok_id_counter,
            "IsID": is_id,
            "DokID": dok_id,
            "IDokIsikukood": isikukood,
            "IDokEesnimi": eesnimi,
            "IDokPerenimi": perenimi,
            "IDokVanaEesnimi": vana_eesnimi,
            "IDokVanaPerenimi": vana_perenimi,
            "IDokVanaIsikukood": vana_isikukood,
            "IDokIsanimi": isanimi,
            "KdIDIsikuRoll": "Mingi roll",
            "AsID": as_id,
            "IAsIDLooja": None,
            "LoodiKpv": bridging_loodi,
            "IAsIDMuutja": None,
            "MuudetiKpv": bridging_muudeti,
            "KustutatiKpv": bridging_kustutati
        }
        records.append(record)
        i_dok_id_counter += 1

    df_isik_dok = pd.DataFrame(records)
    return df_isik_dok
