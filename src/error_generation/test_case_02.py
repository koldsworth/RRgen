import pandas as pd
import random
from datetime import datetime, timedelta

def get_kdid_for_name(df_kod, short_name: str):
    rows = df_kod.loc[df_kod['KdLyhikeNimi'] == short_name, 'KdID']
    return rows.iloc[0] if len(rows) else None

def make_driver_licenses_invalid_by_date(
    df_dokumendid: pd.DataFrame,
    df_kodifikaator: pd.DataFrame,
    dok_liik_nimi: str = "JUHILUBA",
    dok_staatus_nimi: str = "KEHTIV",
    num_changes: int = 3,
    seed: int = None
) -> pd.DataFrame:
    """
    Finds driver's licenses with status 'KEHTIV' and sets their 'DokKehtivKuniKpv'
    to a past date, creating a case where the document appears valid but has expired.

    :param df_dokumendid: 'pd.DataFrame'
        Document table. Expected to contain columns:
          - 'DokID'
          - 'KdIDDokumendiLiik'
          - 'KdIDDokumendiStaatus'
          - 'DokKehtivKuniKpv' (date or None)
          - 'MuudetiKpv' (date or None)
    :param df_kodifikaator: 'pd.DataFrame'
        Codebook table.
    :param dok_liik_nimi: 'str', default "JUHILUBA"
        Short name in the codebook that defines driver's licenses.
    :param dok_staatus_nimi: 'str', default "KEHTIV"
        Status name used to identify documents to be changed.
    :param num_changes: 'int'
        How many driver licenses to randomly update. If fewer are available, updates as many as possible.
    :param seed: 'int', optional
        Seed for reproducibility.
    :return: 'pd.DataFrame'
        Updated document DataFrame.
    """

    if seed is not None:
        random.seed(seed)

    # Get the KdID for JUHILUBA and KEHTIV
    kd_id_juhiluba = get_kdid_for_name(df_kodifikaator, dok_liik_nimi)
    kd_id_kehtiv = get_kdid_for_name(df_kodifikaator, dok_staatus_nimi)

    # Filter documents that are driver's licenses and have status KEHTIV
    mask = (
        (df_dokumendid["KdIDDokumendiLiik"] == kd_id_juhiluba) &
        (df_dokumendid["KdIDDokumendiStaatus"] == kd_id_kehtiv)
    )
    df_candidates = df_dokumendid[mask]

    if len(df_candidates) == 0:
        print("No valid driver's licenses found to update.")
        return df_dokumendid

    # Randomly sample documents to update
    df_to_update = df_candidates.sample(n=min(num_changes, len(df_candidates)), replace=False)

    # Set 'DokKehtivKuniKpv' to a past date while keeping status KEHTIV
    now = datetime.now()
    past_date = now - timedelta(days=30)  # e.g., 30 days ago
    for idx in df_to_update.index:
        df_dokumendid.at[idx, "DokKehtivKuniKpv"] = past_date
        df_dokumendid.at[idx, "MuudetiKpv"] = now

    print(f"Updated {len(df_to_update)} driver's licenses to have an expired 'DokKehtivKuniKpv' while keeping status KEHTIV.")
    return df_dokumendid
