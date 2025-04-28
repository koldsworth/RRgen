import random
import pandas as pd
from datetime import datetime, timedelta
from src.generation.utils import adjust_timeline_for_death, get_kdid_for_name, random_date

def generate_isikuaadress(
    df_relationships: pd.DataFrame,
    final_address_map: dict,
    df_kodifikaator: pd.DataFrame,
    possible_addresses: list,
    min_address_count: int = 1,
    max_address_count: int = 3,
    earliest_date: datetime = datetime(2000, 1, 1),
    latest_date: datetime = datetime(2020, 12, 31),
    ensure_new_address: bool = True,
    seed: int = None
) -> pd.DataFrame:
    """
    Generate an address-history DataFrame for a group of people, grouped by 'Perekonna ID'.
    This function checks that 'IAdrKehtibAlatesKpv' <= 'IAdrKehtibKuniKpv', and if not,
    we adjust or remove the period. It also applies arrival/departure logic at the family level,
    death-date truncation, and a parent-child timeline copy.

    Logic:
      Optionally set random seed.
      1) For each family, derive earliest arrival (family_arrival) and latest departure (family_departure).
      2) For adults, generate address periods (min..max_count). The last period is open (KuniKpv=None => KEHTIV).
         All others have KuniKpv != None => KEHTETU. Link them with IAdrIDJargmine, DokIDLopuAlus.
         Then apply death-date truncation via adjust_timeline_for_death().
      3) For children, simply copy a chosen parent's timeline. Also apply death-date truncation.
      4) Apply family_arrival/family_departure logic:
         - shift earliest period if it starts before family_arrival
         - cut the last period if it extends beyond family_departure
      5) Remove invalid periods where start_>end_, then re-link remaining. 
         This ensures no more references to nonexistent periods. 
      6) Assign final LoodiKpv, KustutatiKpv, MuudetiKpv (these do not alter start_/end_).
      Collect all records and return as a single DataFrame.

    :param df_relationships: DataFrame of people, grouped by 'Perekonna ID'. 
                            It must contain at least:
                              ['IsID', 'Vanuse staatus', 'Vanem(ad)', 'Surmaaeg', 
                               'IsSaabusEesti', 'IsLahkusEestist', 'Perekonna ID']
    :param final_address_map: Dict mapping each IsID to their "final" address (AdrID). 
                              The last period for that IsID will keep KuniKpv=None => KEHTIV.
    :param df_kodifikaator:  DataFrame holding code references (status, address types, etc.), 
                             used by get_kdid_for_name.
    :param possible_addresses: A list of potential AdrID values for non-final periods.
    :param min_address_count: Minimal number of address periods per adult. Default=1.
    :param max_address_count: Maximum number of address periods per adult. Default=3.
    :param earliest_date: The earliest possible date for any address period to begin. 
                         Default = 2000-01-01.
    :param latest_date: The latest date for a non-final period to end. Default=2020-12-31.
    :param ensure_new_address: If True, each new period picks a different address from 
                               the previous (unless no alternative remains).
    :param seed: Optional random seed for reproducibility.
    :return: A DataFrame with columns like:
               [
                 "IAdrID", "AdrID", "IsID", "IAdrKehtibAlatesKpv", "IAdrKehtibKuniKpv",
                 "KdIDAadressiLiik", "KdIDAadressiStaatus", "DokIDAlus", 
                 "IAdrIDJargmine", "DokIDLopuAlus", "LoodiKpv", "MuudetiKpv", 
                 "KustutatiKpv", ...
               ]
             representing address history for each person.
    """

    if seed is not None:
        random.seed(seed)

    all_records = []
    iadr_counter = 1
    doc_id_counter = 1

    # Group by 'Perekonna ID'
    families = df_relationships.groupby("Perekonna ID")

    for family_id, group in families:
        # -------------------------------------------------------------------
        # 1) Derive family-level arrival & departure
        # -------------------------------------------------------------------
        arrival_dates = [d for d in group["IsSaabusEesti"] if pd.notnull(d)]
        departure_dates = [d for d in group["IsLahkusEestist"] if pd.notnull(d)]
        family_arrival = min(arrival_dates) if arrival_dates else None
        family_departure = max(departure_dates) if departure_dates else None

        # Separate adults vs children
        adults = group[group["Vanuse staatus"] == "Täiskasvanu"].copy()
        children = group[group["Vanuse staatus"] == "Laps"].copy()

        person_history_map = {}

        # -------------------------------------------------------------------
        # 2) BUILD TIMELINE FOR EACH ADULT
        # -------------------------------------------------------------------
        for _, adult_row in adults.iterrows():
            pid = adult_row["IsID"]

            # If no final address, skip
            if pid not in final_address_map:
                person_history_map[pid] = []
                continue

            death_date = adult_row.get("Surmaaeg", None)
            address_count = random.randint(min_address_count, max_address_count)
            current_start = earliest_date
            previous_adr = None
            timeline = []

            # Build multiple consecutive periods
            for idx in range(address_count):
                is_last = (idx == address_count - 1)

                if not is_last:
                    period_end = random_date(current_start, latest_date)
                    if period_end < current_start:
                        period_end = current_start + timedelta(days=1)
                else:
                    period_end = None  # final => KEHTIV

                # Choose address
                if is_last:
                    adr_id = final_address_map[pid]
                else:
                    if ensure_new_address and previous_adr is not None:
                        candidates = [a for a in possible_addresses if a != previous_adr]
                        if not candidates:
                            candidates = possible_addresses
                        adr_id = random.choice(candidates)
                    else:
                        adr_id = random.choice(possible_addresses)

                # Decide status & type
                if period_end is None:
                    kd_id_staatus = get_kdid_for_name(df_kodifikaator, "KEHTIV")
                    kd_id_liik = get_kdid_for_name(df_kodifikaator, "ELUKOHT")
                else:
                    kd_id_staatus = get_kdid_for_name(df_kodifikaator, "KEHTETU")
                    kd_id_liik = get_kdid_for_name(df_kodifikaator, "ENDINE ELUKOHT")

                creation_doc_id = doc_id_counter
                doc_id_counter += 1

                rec = {
                    "IAdrID": iadr_counter,
                    "AdrID": adr_id,
                    "IsID": pid,
                    "IAdrKehtibAlatesKpv": current_start,
                    "IAdrKehtibKuniKpv": period_end,
                    "KdIDAadressiLiik": kd_id_liik,
                    "KdIDAadressiStaatus": kd_id_staatus,
                    "DokIDAlus": creation_doc_id,
                    "IAdrIDJargmine": None,
                    "DokIDLopuAlus": None,
                    "ADSILID": None,
                    "IAdrPiirang": None,
                    "IAsIDLooja": None,
                    "LoodiKpv": None,
                    "IAsIDMuutja": None,
                    "MuudetiKpv": None,
                    "KustutatiKpv": None
                }
                timeline.append(rec)
                iadr_counter += 1
                previous_adr = adr_id

                if period_end is not None:
                    current_start = period_end + timedelta(days=1)

            # Link consecutive periods (initial)
            for i in range(len(timeline) - 1):
                timeline[i]["IAdrIDJargmine"] = timeline[i + 1]["IAdrID"]
                timeline[i]["DokIDLopuAlus"] = timeline[i + 1]["DokIDAlus"]

            # Death-date truncation
            timeline = adjust_timeline_for_death(df_kodifikaator, timeline, death_date)

            person_history_map[pid] = timeline

        # -------------------------------------------------------------------
        # 3) BUILD TIMELINE FOR EACH CHILD (COPY FROM PARENT)
        # -------------------------------------------------------------------
        for _, child_row in children.iterrows():
            child_id = child_row["IsID"]
            parent_ids = child_row["Vanem(ad)"]
            if not isinstance(parent_ids, list):
                parent_ids = []

            if not parent_ids:
                person_history_map[child_id] = []
                continue

            # pick parent whose timeline is available
            chosen_parent_id = random.choice(parent_ids)
            if chosen_parent_id not in person_history_map:
                alt_par = [p for p in parent_ids if p in person_history_map]
                if alt_par:
                    chosen_parent_id = random.choice(alt_par)
                else:
                    person_history_map[child_id] = []
                    continue

            parent_timeline = person_history_map[chosen_parent_id]
            child_timeline = []
            death_date_child = child_row.get("Surmaaeg", None)

            for idx, pt in enumerate(parent_timeline):
                start_date = pt["IAdrKehtibAlatesKpv"]
                end_date = pt["IAdrKehtibKuniKpv"]

                creation_doc_id = doc_id_counter
                doc_id_counter += 1

                new_rec = {
                    "IAdrID": iadr_counter,
                    "AdrID": pt["AdrID"],
                    "IsID": child_id,
                    "IAdrKehtibAlatesKpv": start_date,
                    "IAdrKehtibKuniKpv": end_date,
                    "KdIDAadressiLiik": pt["KdIDAadressiLiik"],
                    "KdIDAadressiStaatus": pt["KdIDAadressiStaatus"],
                    "DokIDAlus": creation_doc_id,
                    "IAdrIDJargmine": None,
                    "DokIDLopuAlus": None,
                    "ADSILID": None,
                    "IAdrPiirang": None,
                    "IAsIDLooja": None,
                    "LoodiKpv": None,
                    "IAsIDMuutja": None,
                    "MuudetiKpv": None,
                    "KustutatiKpv": None
                }
                child_timeline.append(new_rec)
                iadr_counter += 1

                # Link them
                if idx > 0:
                    child_timeline[idx - 1]["IAdrIDJargmine"] = new_rec["IAdrID"]
                    child_timeline[idx - 1]["DokIDLopuAlus"] = new_rec["DokIDAlus"]

            # Child death-date truncation
            child_timeline = adjust_timeline_for_death(df_kodifikaator, child_timeline, death_date_child)
            person_history_map[child_id] = child_timeline

        # -------------------------------------------------------------------
        # 4) APPLY FAMILY ARRIVAL/DEPARTURE => modifies start_/end_
        # -------------------------------------------------------------------
        for pid in group["IsID"]:
            tml = person_history_map.get(pid, [])
            if not tml:
                continue

            if family_arrival is not None and len(tml) > 0:
                first_p = tml[0]
                if first_p["IAdrKehtibAlatesKpv"] < family_arrival:
                    first_p["IAdrKehtibAlatesKpv"] = family_arrival

            if family_departure is not None and len(tml) > 0:
                last_p = tml[-1]
                if last_p["IAdrKehtibKuniKpv"] is None or last_p["IAdrKehtibKuniKpv"] > family_departure:
                    last_p["IAdrKehtibKuniKpv"] = family_departure
                    last_p["KdIDAadressiStaatus"] = get_kdid_for_name(df_kodifikaator, "KEHTETU")
                    last_p["KdIDAadressiLiik"]    = get_kdid_for_name(df_kodifikaator, "ENDINE ELUKOHT")

        # -------------------------------------------------------------------
        # 5) REMOVE INVALID (start_>end_) + RELINK
        # -------------------------------------------------------------------
        def remove_invalid_and_relink(timeline):
            fixed = []
            for rec in timeline:
                st_ = rec["IAdrKehtibAlatesKpv"]
                en_ = rec["IAdrKehtibKuniKpv"]
                if en_ is not None and st_ > en_:
                    # skip this record entirely
                    continue
                fixed.append(rec)

            # Re-link after removing invalid
            for i in range(len(fixed) - 1):
                fixed[i]["IAdrIDJargmine"] = fixed[i+1]["IAdrID"]
                fixed[i]["DokIDLopuAlus"]  = fixed[i+1]["DokIDAlus"]

            if fixed:
                fixed[-1]["IAdrIDJargmine"] = None
                fixed[-1]["DokIDLopuAlus"]  = None
            return fixed

        for pid in group["IsID"]:
            tml = person_history_map.get(pid, [])
            if not tml:
                continue
            tml = remove_invalid_and_relink(tml)
            person_history_map[pid] = tml

        # -----------------------------------------------------------------------
        # 6) SET LOODIKPV, MUUDETIPV, KUSTUTATIKPV (no more changes to st_/end_)
        # -----------------------------------------------------------------------
        def assign_dates_final(timeline):
            for row_ in timeline:
                st_ = row_["IAdrKehtibAlatesKpv"]
                en_ = row_["IAdrKehtibKuniKpv"]
                new_latest_date = latest_date

                # LoodiKpv in [earliest_date..st_]
                if not st_ or st_ < earliest_date:
                    st_ = earliest_date
                row_["LoodiKpv"] = random_date(earliest_date, st_)

                # KustutatiKpv (10% chance) if the period has an end
                if en_ is not None and random.random() < 0.1:
                    # random date in [en_..latest_date]
                    row_["KustutatiKpv"] = random_date(en_, latest_date)
                    new_latest_date = row_["KustutatiKpv"]
                else:
                    row_["KustutatiKpv"] = None

                # MuudetiKpv depends on whether end_ is set
                if en_ is not None:
                    upper_ = max(en_, row_["LoodiKpv"])
                    if upper_ < new_latest_date and random.random() < 0.5:
                        row_["MuudetiKpv"] = random_date(upper_, new_latest_date)
                    else:
                        row_["MuudetiKpv"] = None
                else:
                    # open => 30% chance
                    if random.random() < 0.3:
                        row_["MuudetiKpv"] = random_date(row_["LoodiKpv"], new_latest_date)
                    else:
                        row_["MuudetiKpv"] = None

            return timeline

        for pid in group["IsID"]:
            tml = person_history_map.get(pid, [])
            if not tml:
                continue
            tml = assign_dates_final(tml)
            person_history_map[pid] = tml

        # Collect final
        for pid in group["IsID"]:
            all_records.extend(person_history_map.get(pid, []))

    df_history = pd.DataFrame(all_records)

    id_cols     = ["IAdrID", "DokIDAlus", "IAdrIDJargmine", "DokIDLopuAlus"]
    pointer_cols = ["DokIDAlus", "IAdrIDJargmine", "DokIDLopuAlus"]

    # --- 1. make sure the raw data are integers, not floats ---------------------
    for col in id_cols:
        if col in df_history.columns:
            df_history[col] = df_history[col].astype("Int64")        # nullable int

    # --- 2. build the map (after they’re integers!) ----------------------------
    df_history = df_history.sort_values("IAdrID").reset_index(drop=True)
    id_map = {old_id: new_id for new_id, old_id in enumerate(df_history["IAdrID"], start=1)}

    # primary key
    df_history["IAdrID"] = df_history["IAdrID"].map(id_map).astype("Int64")

    # every column that points at IAdrID (single pass each)
    for col in pointer_cols:
        if col in df_history.columns:
            df_history[col] = (
                df_history[col]
                .map(lambda x: id_map.get(int(x)) if pd.notna(x) else x)
                .astype("Int64")
            )

    return df_history
