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


def add_invalid_citizenship(
        df_citizenship: pd.DataFrame,
        df_documents: pd.DataFrame,
        df_person_document: pd.DataFrame,
        df_person: pd.DataFrame,
        df_codebook: pd.DataFrame,
        doc_type: str = "MÄÄRATLEMATA",
        person_count: int = 5,
        seed: int = None
) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    Creates an invalid citizenship record for selected persons by:
      - Generating a new citizenship record in df_citizenship,
      - Generating a new document in df_documents with additional data fields and the specified doc_type,
      - And adding a new row to the bridging table (df_person_document) linking the person and the document.
      
    After creating the document record, the "IsID" column is dropped from df_documents.

    :param df_citizenship: DataFrame for citizenship records.
    :param df_documents: DataFrame for document records.
    :param df_person_document: DataFrame for the person–document bridging table.
    :param df_person: DataFrame for person records.
    :param df_codebook: DataFrame for codebook lookup.
    :param doc_type: Document type (e.g., "MÄÄRATLEMATA").
    :param person_count: Number of persons to randomly select.
    :param seed: Fixed seed for reproducibility.
    :return: Tuple of updated (df_citizenship, df_documents, df_person_document).
    """
    if seed is not None:
        random.seed(seed)

    # Retrieve necessary KdIDs from the codebook
    kd_id_valid = get_kdid_for_name(df_codebook, "KEHTIV")
    kd_id_citizenship_doc = get_kdid_for_name(df_codebook, doc_type)
    kd_id_country_EE = get_kdid_for_name(df_codebook, "EE")

    selected_persons = df_person.sample(n=person_count, replace=False)

    start_doc_id = df_documents["DokID"].max() + 1 if len(df_documents) > 0 else 1
    start_citizenship_id = df_citizenship["KodID"].max() + 1 if len(df_citizenship) > 0 else 1
    start_person_document_id = df_person_document["IDokID"].max() + 1 if len(df_person_document) > 0 else 1

    doc_id_counter = start_doc_id
    citizenship_id_counter = start_citizenship_id
    person_document_id_counter = start_person_document_id

    now = datetime.now()
    earliest_date = datetime(1950, 1, 1)

    new_documents_rows = []
    new_citizenship_rows = []
    new_person_document_rows = []

    for _, person_row in selected_persons.iterrows():
        is_id = person_row["IsID"]

        # Create a new document record with additional fields
        doc_record = {
            "DokID": doc_id_counter,
            "KdIDDokumendiLiik": kd_id_citizenship_doc,
            "DokNumber": f"TEST-{doc_id_counter}",
            "DokSeeria": f"S-{random.randint(100, 999)}",
            "DokValjaantudKpv": random_date(earliest_date, now),
            "DokKehtivKuniKpv": None,
            "DokKehtetuAlatesKpv": None,
            "AsIDValjaandjaAsutus": random.randint(1, 50),
            "KaIDKanne": None,
            "IAsIDLooja": None,
            "LoodiKpv": random_date(earliest_date, now),
            "IAsIDMuutja": None,
            "MuudetiKpv": None,
            "KustutatiKpv": None,
            "KdIDDokumendiStaatus": kd_id_valid,
            "DokIDVanem": None,
            "DokKehtivAlates": random_date(earliest_date, now - timedelta(days=70)),
            "KdIDRiik": kd_id_country_EE,
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
            "IsID": is_id  # in the final df_documents it will be dropped
        }
        new_documents_rows.append(doc_record)

        # Create a new citizenship record (expired validity)
        valid_from = random_date(earliest_date, now - timedelta(days=60))
        citizenship_record = {
            "KodID": citizenship_id_counter,
            "IsID": is_id,
            "KdIDRiik": kd_id_country_EE,
            "KodKehtibAlates": valid_from,
            "KodKehtibKuni": None,
            "DokIDAlguseAlus": doc_id_counter,
            "DokIDLopuAlus": None,
            "KdIDStaatus": kd_id_valid,
            "IAsIDLooja": None,
            "IAsIDMuutja": None,
            "LoodiKpv": random_date(earliest_date, valid_from),
            "MuudetiKpv": None,
            "KustutatiKpv": None
        }
        new_citizenship_rows.append(citizenship_record)

        # --- Create a new person–document bridging record ---
        person_document_record = {
            "IDokID": person_document_id_counter,
            "IsID": is_id,
            "DokID": doc_id_counter,
            "IDokIsikukood": person_row.get("IsIsikukood", None),
            "IDokEesnimi": person_row.get("IsEesnimi", None),
            "IDokPerenimi": person_row.get("IsPerenimi", None),
            "IDokIsanimi": person_row.get("IsIsanimi", None),
            "IDokVanaEesnimi": None,
            "IDokVanaPerenimi": None,
            "IDokVanaIsikukood": None,
            "KdIDIsikuRoll": 2,  # For example, 2 = applicant
            "AsID": None,
            "IAsIDLooja": None,
            "LoodiKpv": random_date(earliest_date, now),
            "IAsIDMuutja": None,
            "MuudetiKpv": None,
            "KustutatiKpv": None
        }
        new_person_document_rows.append(person_document_record)

        doc_id_counter += 1
        citizenship_id_counter += 1
        person_document_id_counter += 1

    if new_documents_rows:
        new_docs_df = pd.DataFrame(new_documents_rows).dropna(axis=1, how='all')
        df_documents = pd.concat([df_documents, new_docs_df], ignore_index=True)
        # Drop the 'IsID' column from df_documents
        if 'IsID' in df_documents.columns:
            df_documents = df_documents.drop(columns=['IsID'])
    if new_citizenship_rows:
        new_citizenship_df = pd.DataFrame(new_citizenship_rows).dropna(axis=1, how='all')
        df_citizenship = pd.concat([df_citizenship, new_citizenship_df], ignore_index=True)
    if new_person_document_rows:
        new_person_doc_df = pd.DataFrame(new_person_document_rows).dropna(axis=1, how='all')
        df_person_document = pd.concat([df_person_document, new_person_doc_df], ignore_index=True)

    return df_citizenship, df_documents, df_person_document
