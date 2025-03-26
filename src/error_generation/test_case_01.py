import random
import pandas as pd
from datetime import datetime, timedelta

def get_kdid_for_name(df_kod, short_name: str):
    """Retrieve KdID for a given short name from the codebook DataFrame."""
    rows = df_kod.loc[df_kod['KdLyhikeNimi'] == short_name, 'KdID']
    return rows.iloc[0] if len(rows) else None

def random_date(start: datetime, end: datetime) -> datetime:
    """Generate a random date between start and end."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

def add_invalid_residences(
    df_isikuaadress: pd.DataFrame,
    df_aadress: pd.DataFrame,
    df_dokumendid: pd.DataFrame,
    df_isikudokument: pd.DataFrame,
    df_isik: pd.DataFrame,
    df_kodifikaator: pd.DataFrame,
    dok_liik_nimi: str = "ELUKOHATEADE",
    person_count: int = 5,
    seed: int = None
) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    Creates a second valid residence address for selected persons (in df_isikuaadress),
    generates a new ELUKOHATEADE document with additional fields (in df_dokumendid),
    and adds rows to the bridging table (df_isikudokument) linking the person and the document.
    
    The document record includes extra fields such as DokSeeria, DokValjaantudKpv,
    DokKehtivKuniKpv, and others. After concatenation the 'IsID' column is dropped from df_dokumendid.
    
    :param df_isikuaadress: DataFrame for person addresses.
    :param df_aadress: DataFrame for addresses.
    :param df_dokumendid: DataFrame for documents.
    :param df_isikudokument: DataFrame for the person-document bridging table.
    :param df_isik: DataFrame for person records.
    :param df_kodifikaator: DataFrame for codebook lookup.
    :param dok_liik_nimi: Document type (e.g., "ELUKOHATEADE").
    :param person_count: Number of persons to randomly select.
    :param seed: Fixed seed for reproducibility.
    :return: Tuple of updated (df_isikuaadress, df_dokumendid, df_isikudokument).
    """
    if seed is not None:
        random.seed(seed)
    
    kd_id_kehtiv = get_kdid_for_name(df_kodifikaator, "KEHTIV")
    kd_id_elukoht = get_kdid_for_name(df_kodifikaator, "ELUKOHT")
    kd_id_elukohateade = get_kdid_for_name(df_kodifikaator, dok_liik_nimi)
    
    selected_persons = df_isik.sample(n=person_count, replace=False)
    
    start_dok_id = df_dokumendid["DokID"].max() + 1 if len(df_dokumendid) > 0 else 1
    start_iadr_id = df_isikuaadress["IAdrID"].max() + 1 if len(df_isikuaadress) > 0 else 1
    start_idok_id = df_isikudokument["IDokID"].max() + 1 if len(df_isikudokument) > 0 else 1
    
    dok_id_counter = start_dok_id
    iadr_id_counter = start_iadr_id
    idok_id_counter = start_idok_id
    
    now = datetime.now()
    
    new_isikuaadress_rows = []
    new_dokumendid_rows = []
    new_isikudokument_rows = []
    
    for _, person_row in selected_persons.iterrows():
        is_id = person_row["IsID"]
        random_adr_id = df_aadress["AdrID"].dropna().sample(1).iloc[0]
        
        # Create a new document record with additional fields
        doc_record = {
            "DokID": dok_id_counter,
            "KdIDDokumendiLiik": kd_id_elukohateade,
            "DokNumber": f"TEST-{dok_id_counter}",
            "DokSeeria": f"S-{random.randint(100, 999)}",
            "DokValjaantudKpv": random_date(datetime(2000, 1, 1), now),
            "DokKehtivKuniKpv": random_date(datetime(2000, 1, 1), now),
            "DokKehtetuAlatesKpv": None,
            "AsIDValjaandjaAsutus": random.randint(1, 50),
            "KaIDKanne": None,
            "IAsIDLooja": None,
            "LoodiKpv": now,
            "IAsIDMuutja": None,
            "MuudetiKpv": None,
            "KustutatiKpv": None,
            "KdIDDokumendiStaatus": kd_id_kehtiv,
            "DokIDVanem": None,
            "DokKehtivAlates": now,
            "KdIDRiik": get_kdid_for_name(df_kodifikaator, "EE"),
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
            "KdIDDokAlguseAlus": None,
            "IsID": is_id
        }
        new_dokumendid_rows.append(doc_record)
        
        # Generate a random start date for the new residence record
        start_date = datetime(2000, 1, 1)
        end_date = datetime(2023, 12, 31)
        delta = (end_date - start_date).days
        random_offset = random.randint(0, delta)
        iadr_kehtib_alates = start_date + timedelta(days=random_offset)
        
        # Create a new residence address record with basic fields
        iadr_record = {
            "IAdrID": iadr_id_counter,
            "AdrID": random_adr_id,
            "IsID": is_id,
            "IAdrKehtibAlatesKpv": iadr_kehtib_alates,
            "IAdrKehtibKuniKpv": None,
            "KdIDAadressiLiik": kd_id_elukoht,
            "KdIDAadressiStaatus": kd_id_kehtiv,
            "DokIDAlus": dok_id_counter,
            "IAdrIDJargmine": None,
            "DokIDLopuAlus": None,
            "ADSILID": None,
            "IAdrPiirang": None,
            "IAsIDLooja": None,
            "LoodiKpv": iadr_kehtib_alates,
            "IAsIDMuutja": None,
            "MuudetiKpv": None,
            "KustutatiKpv": None
        }
        new_isikuaadress_rows.append(iadr_record)
        
        # Create a new person-document bridging record with extra fields from person data
        idok_record = {
            "IDokID": idok_id_counter,
            "IsID": is_id,
            "DokID": dok_id_counter,
            "IDokIsikukood": person_row.get("IsIsikukood", None),
            "IDokEesnimi": person_row.get("IsEesnimi", None),
            "IDokPerenimi": person_row.get("IsPerenimi", None),
            "IDokIsanimi": person_row.get("IsIsanimi", None),
            "IDokVanaEesnimi": None,
            "IDokVanaPerenimi": None,
            "IDokVanaIsikukood": None,
            "KdIDIsikuRoll": None,
            "AsID": None,
            "IAsIDLooja": None,
            "LoodiKpv": now,
            "IAsIDMuutja": None,
            "MuudetiKpv": None,
            "KustutatiKpv": None
        }
        new_isikudokument_rows.append(idok_record)
        
        dok_id_counter += 1
        iadr_id_counter += 1
        idok_id_counter += 1
    
    if new_dokumendid_rows:
        new_docs_df = pd.DataFrame(new_dokumendid_rows).dropna(axis=1, how='all')
        df_dokumendid = pd.concat([df_dokumendid, new_docs_df], ignore_index=True)
        # Drop the 'IsID' column from df_dokumendid
        if 'IsID' in df_dokumendid.columns:
            df_dokumendid = df_dokumendid.drop(columns=['IsID'])
    if new_isikuaadress_rows:
        new_address_df = pd.DataFrame(new_isikuaadress_rows).dropna(axis=1, how='all')
        df_isikuaadress = pd.concat([df_isikuaadress, new_address_df], ignore_index=True)
    if new_isikudokument_rows:
        new_person_doc_df = pd.DataFrame(new_isikudokument_rows).dropna(axis=1, how='all')
        df_isikudokument = pd.concat([df_isikudokument, new_person_doc_df], ignore_index=True)
    
    return df_isikuaadress, df_dokumendid, df_isikudokument

if __name__ == "__main__":
    input_files = {
        "df_isikuaadress": "output/05_isikuaadress.csv",
        "df_aadress": "output/01_aadress.csv",
        "df_dokumendid": "output/09_dokument.csv",
        "df_isikudokument": "output/08_isikudokument.csv",
        "df_isik": "output/06_isik.csv",
        "df_kodifikaator": "data/kodifikaator.csv"
    }
    
    dataframes = {
        name: pd.read_csv(path, delimiter=",", encoding="ISO-8859-1", low_memory=False)
        for name, path in input_files.items()
    }
    
    df_isikuaadress = dataframes["df_isikuaadress"]
    df_aadress = dataframes["df_aadress"]
    df_dokumendid = dataframes["df_dokumendid"]
    df_isikudokument = dataframes["df_isikudokument"]
    df_isik = dataframes["df_isik"]
    df_kodifikaator = dataframes["df_kodifikaator"]
    
    df_isikuaadress, df_dokumendid, df_isikudokument = add_invalid_residences(
        df_isikuaadress=df_isikuaadress,
        df_aadress=df_aadress,
        df_dokumendid=df_dokumendid,
        df_isikudokument=df_isikudokument,
        df_isik=df_isik,
        df_kodifikaator=df_kodifikaator,
        dok_liik_nimi="ELUKOHATEADE",
        person_count=5,
        seed=42
    )
    
    df_isikuaadress.to_csv("output/05_isikuaadress.csv", sep=",", index=False)
    df_dokumendid.to_csv("output/09_dokument.csv", sep=",", index=False)
    df_isikudokument.to_csv("output/08_isikudokument.csv", sep=",", index=False)
    
    print("New invalid residence (document) records added to isikuaadress, dokumendid, and isikudokument tables.")
