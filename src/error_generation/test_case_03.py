import random
import pandas as pd
from datetime import datetime, timedelta

def get_kdid_for_name(df_kod, short_name: str):
    """Retrieve KdID for a given short name from the codebook DataFrame."""
    rows = df_kod.loc[df_kod['KdLyhikeNimi'] == short_name, 'KdID']
    return rows.iloc[0] if len(rows) else None

def shift_valid_residences_of_immigrants_to_future(
    df_isik: pd.DataFrame,
    df_isikudokument: pd.DataFrame,
    df_isikuaadress: pd.DataFrame,
    df_dokumendid: pd.DataFrame,
    df_kodifikaator: pd.DataFrame,
    future_days: int = 365,
    dok_liik_nimi: str = "ELUKOHATEADE",
    seed: int = None
) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    For persons who have 'IsSaabusEesti' filled (immigrants),
    this function updates:
    - Their valid residence address entries by setting the start date to a future date.
    - Their valid residence documents (of type dok_liik_nimi) by setting 'DokKehtibAlatesKpv' to the same future date.

    :param df_isik: DataFrame with people. Must contain 'IsID' and 'IsSaabusEesti'.
    :param df_isikuaadress: Address table. Must contain 'IsID', 'IAdrKehtibAlatesKpv', 'KdIDAadressiStaatus'.
    :param df_dokumendid: Document table. Must contain 'IsID', 'DokKehtibAlatesKpv', 'KdIDDokumendiLiik', 'KdIDStaatus'.
    :param df_kodifikaator: Codebook table.
    :param future_days: How many days into the future the date should be shifted.
    :param dok_liik_nimi: Document type to be updated (e.g. "ELUKOHATEADE").
    :param seed: Optional fixed seed for reproducibility.
    :return: Updated (df_isik, df_isikuaadress, df_dokumendid)
    """

    if seed is not None:
        random.seed(seed)

    now = datetime.now()
    future_date = now + timedelta(days=future_days)

    kd_id_kehtiv = get_kdid_for_name(df_kodifikaator, "KEHTIV")
    kd_id_elukohateade = get_kdid_for_name(df_kodifikaator, dok_liik_nimi)

    # Select only persons who have arrived in Estonia
    arrived_mask = df_isik["IsSaabusEesti"].notnull()
    arrived_persons = df_isik.loc[arrived_mask].copy()

    if "IsID" not in df_dokumendid.columns:
        # bring IsID back from the bridge table
        df_dokumendid = df_dokumendid.merge(
            df_isikudokument[["DokID", "IsID"]], on="DokID", how="left"
        )

    for _, person in arrived_persons.iterrows():
        is_id = person["IsID"]

        # Update residence address start dates for KEHTIV entries
        mask_iadr = (
            (df_isikuaadress["IsID"] == is_id) &
            (df_isikuaadress["KdIDAadressiStaatus"] == kd_id_kehtiv)
        )
        df_isikuaadress.loc[mask_iadr, "IAdrKehtibAlatesKpv"] = future_date

        # Update residence documents start dates for KEHTIV ELUKOHATEADE
        mask_dok = (
            (df_dokumendid["IsID"] == is_id) &
            (df_dokumendid["KdIDDokumendiLiik"] == kd_id_elukohateade) &
            (df_dokumendid["KdIDDokumendiStaatus"] == kd_id_kehtiv)
        )
        df_dokumendid.loc[mask_dok, "DokKehtibAlatesKpv"] = future_date

    return df_isik, df_isikuaadress, df_dokumendid