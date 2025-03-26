import random
import pandas as pd
from datetime import datetime, timedelta
from src.generation.utils import random_date, get_education_for_age

def generate_temp_relatsionships(
    n_people: int = 10,
    seed: int = None,
    min_age_for_parenthood: int = 18,
    death_probability: float = 0.1,
    df_possible_aadresses: pd.DataFrame = None,
    df_kd: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Generate a random list of people (n_people) with relationships, addresses,
    education, and Estonian constants (like 'Täiskasvanu', 'Laps', etc.).
    The logic is fully expanded below, with no omitted steps:

    Logic:
        1) Prepare a list of address IDs from df_possible_aadresses (or a fallback list).
        2) Build initial person records, each labeled as 'Täiskasvanu' or 'Laps',
        and 'MEES' or 'NAINE'. Adults will later get a marital status.
        3) Assign marital status for adults: 'Vallaline', 'Abielus', or 'Lahutatud'
        (children remain 'Vallaline').
        4) Form married couples (both must be 'Abielus').
        5) Separate the list into adults vs. children, and identify actual couples
        as a list of (adult1, adult2).
        6) Assign children to 1 or 2 parents. If 2, pick from existing married couples.
        7) Assign birthdates to adults (18..100 years old),
        plus possible Surmaaeg with probability = death_probability.
        8) Assign birthdates to children (<=17 years old),
        plus possible Surmaaeg with probability = death_probability/2.
        9) Assign 'Haridus' (education) by calling get_education_for_age(...) if df_kd is provided.
        10) Assign 'Kodakondsus' (70% 'EE', 30% 'MUU'). 
            For 'MUU', random arrival date (IsSaabusEesti), 20% also get IsLahkusEestist.
        11) Use a BFS approach to determine 'Perekonna ID' for connected individuals 
            (partner -> children -> parents).
        12) Clear out addresses (Aadress) so we can assign them freshly.
            Then assign addresses for married couples (80% share the same address).
        13) For single adults with no address assigned, pick a random address.
        14) For children: if under 18, always take a parent's address; 
            if 18 or older, 70% chance they keep a parent's address, else random.
        15) Fill 'KOV' based on 'AkpIDTase2' from df_possible_aadresses, 
            handle arrival logic for children if parent arrived later, 
            and assign each adult a possible AsID (institution) with the stated logic.

    :param n_people: How many total individuals to generate.
    :param seed: Seed for randomization (optional).
    :param min_age_for_parenthood: Minimum parent age, default = 18.
    :param death_probability: Probability an adult is deceased (child probability = death_probability / 2).
    :param df_possible_aadresses: A DataFrame of possible addresses, must have at least the column 'AdrID' 
                                  and optionally 'AkpIDTase2' for the KOV logic.
    :param df_kd: A DataFrame for kodifikaator, if needed by 'get_education_for_age'.
    :return: A Pandas DataFrame with columns:
             [
               "IsID", "Vanuse staatus", "Suhteseis", "Partneri ID",
               "Laps(ed)", "Vanem(ad)", "Sugu", "Sünniaeg", "Surmaaeg", "Haridus",
               "Kodakondsus", "IsSaabusEesti", "IsLahkusEestist", "Aadress",
               "Perekonna ID", "KOV", "AsID", "vanus_aastates"
             ]
             fully populated according to the logic above.
    """
    # If a seed is provided, fix the random state
    if seed is not None:
        random.seed(seed)

    # -------------------------------------------------------------------
    # 1) Prepare the list of addresses from df_possible_aadresses
    # -------------------------------------------------------------------
    if df_possible_aadresses is not None and "AdrID" in df_possible_aadresses.columns:
        possible_addresses = df_possible_aadresses["AdrID"].tolist()
    else:
        # fallback if none provided
        possible_addresses = [100001, 100002, 100003, 100010, 100040]

    now = datetime.now()

    # Basic sets for statuses
    age_status_choices = ["Täiskasvanu", "Laps"]
    marital_status_choices = ["Vallaline", "Abielus", "Lahutatud"]

    data_list = []
    all_ids = list(range(1, n_people + 1))

    # -------------------------------------------------------------------
    # 2) Create base records (adult/child)
    # -------------------------------------------------------------------
    for person_id in all_ids:
        status = random.choice(age_status_choices)
        gender = random.choice(["MEES", "NAINE"])

        person = {
            "IsID": person_id,
            "Vanuse staatus": status,  # 'Täiskasvanu' or 'Laps'
            "Suhteseis": None,        # Will be set if adult
            "Partneri ID": None,
            "Laps(ed)": [],
            "Vanem(ad)": [],
            "Sugu": gender,          # 'MEES' or 'NAINE'
            "Sünniaeg": None,
            "Surmaaeg": None,
            "Haridus": None,
            "Kodakondsus": None,
            "IsSaabusEesti": None,
            "IsLahkusEestist": None,
            "Aadress": None
        }
        data_list.append(person)

    # -------------------------------------------------------------------
    # 3) Set marital status for adults
    # -------------------------------------------------------------------
    for p in data_list:
        if p["Vanuse staatus"] == "Laps":
            p["Suhteseis"] = "Vallaline"
        else:
            p["Suhteseis"] = random.choice(marital_status_choices)

    # -------------------------------------------------------------------
    # 4) Form married couples
    # -------------------------------------------------------------------
    def has_partner(person):
        return person["Partneri ID"] is not None

    # Only adults who are 'Abielus'
    potential_partners = [
        p for p in data_list
        if p["Vanuse staatus"] == "Täiskasvanu" and p["Suhteseis"] == "Abielus"
    ]
    random.shuffle(potential_partners)

    for i in range(len(potential_partners) - 1):
        p1 = potential_partners[i]
        if not has_partner(p1):
            for j in range(i + 1, len(potential_partners)):
                p2 = potential_partners[j]
                if not has_partner(p2):
                    p1["Partneri ID"] = p2["IsID"]
                    p2["Partneri ID"] = p1["IsID"]
                    break

    # -------------------------------------------------------------------
    # 5) Separate adults and children; identify couples
    # -------------------------------------------------------------------
    adults = [p for p in data_list if p["Vanuse staatus"] == "Täiskasvanu"]
    children = [p for p in data_list if p["Vanuse staatus"] == "Laps"]

    # Identify actual couples as (ad1, ad2) for BFS usage
    couples = []
    for a in adults:
        pid = a["Partneri ID"]
        if pid and a["IsID"] < pid:
            partner = next((x for x in data_list if x["IsID"] == pid), None)
            if partner:
                couples.append((a, partner))

    # -------------------------------------------------------------------
    # 6) Assign children to 1 or 2 parents
    # -------------------------------------------------------------------
    for c in children:
        num_parents = random.choice([1, 2])
        if num_parents == 2 and couples:
            mom, dad = random.choice(couples)
            c["Vanem(ad)"] = [mom["IsID"], dad["IsID"]]
            mom["Laps(ed)"].append(c["IsID"])
            dad["Laps(ed)"].append(c["IsID"])
        elif num_parents == 1 and adults:
            single_parent = random.choice(adults)
            c["Vanem(ad)"] = [single_parent["IsID"]]
            single_parent["Laps(ed)"].append(c["IsID"])
        # If no parents found, the child remains with empty "Vanem(ad)".

    # -------------------------------------------------------------------
    # 7) Assign birthdates & deathdates for adults
    # -------------------------------------------------------------------
    # Adult age range = [18..100]
    earliest_adult = now.replace(year=now.year - 100)
    latest_adult = now.replace(year=now.year - 18)

    for ad in adults:
        birth_date = random_date(earliest_adult, latest_adult)
        ad["Sünniaeg"] = birth_date
        if random.random() < death_probability:
            ad["Surmaaeg"] = random_date(birth_date, now)

    # -------------------------------------------------------------------
    # 8) Assign birthdates & deathdates for children
    # -------------------------------------------------------------------
    for child in children:
        parents = child["Vanem(ad)"]
        if parents:
            # pick the first parent's birth date
            par_id = parents[0]
            parent_obj = next((x for x in data_list if x["IsID"] == par_id), None)
            if parent_obj and parent_obj["Sünniaeg"] is not None:
                earliest_child_bd = parent_obj["Sünniaeg"] + timedelta(days=365 * min_age_for_parenthood)
                if earliest_child_bd > now:
                    earliest_child_bd = now - timedelta(days=365)
                child["Sünniaeg"] = random_date(min(earliest_child_bd, now - timedelta(days=365)), max(earliest_child_bd, now - timedelta(days=365)))
            else:
                # random child age 0..17
                child["Sünniaeg"] = now - timedelta(days=365 * random.randint(1, 17))
        else:
            # no parents => random child age 0..17
            child["Sünniaeg"] = now - timedelta(days=365 * random.randint(1, 17))

        # children have half the death probability
        if random.random() < (death_probability / 2.0):
            child["Surmaaeg"] = random_date(child["Sünniaeg"], now)

    # -------------------------------------------------------------------
    # 9) Education assignment via get_education_for_age
    # -------------------------------------------------------------------

    for p in data_list:
        bd = p["Sünniaeg"]
        if bd:
            age_years = (now - bd).days // 365
            p["Haridus"] = get_education_for_age(age_years, df_kd)  # or local logic
        else:
            p["Haridus"] = None

    # -------------------------------------------------------------------
    # 10) Kodakondsus & arrival/departure logic
    # -------------------------------------------------------------------
    for p in data_list:
        if random.random() < 0.7:
            p["Kodakondsus"] = "EE"
        else:
            p["Kodakondsus"] = "MUU"

        if p["Kodakondsus"] != "EE" and p["Sünniaeg"]:
            arrival = random_date(p["Sünniaeg"], now)
            p["IsSaabusEesti"] = arrival
            # 20% chance to leave
            if random.random() < 0.2:
                p["IsLahkusEestist"] = random_date(arrival, now)

    # -------------------------------------------------------------------
    # 11) BFS to form "Perekonna ID"
    # -------------------------------------------------------------------
    df_people = pd.DataFrame(data_list)
    df_people["Perekonna ID"] = None

    def bfs_collect(start_id):
        queue_list = [start_id]
        visited_set = set()
        while queue_list:
            current = queue_list.pop(0)
            if current not in visited_set:
                visited_set.add(current)
                matching_rows = df_people.loc[df_people["IsID"] == current]
                if matching_rows.empty:
                    continue
                row_ = matching_rows.iloc[0]
                # partner
                partner_id = row_["Partneri ID"]
                if partner_id is not None:
                    queue_list.append(partner_id)
                # children
                for c_id in row_["Laps(ed)"]:
                    queue_list.append(c_id)
                # parents
                for par_id in row_["Vanem(ad)"]:
                    queue_list.append(par_id)
        return visited_set

    fam_id_counter = 1
    for person_id in df_people["IsID"]:
        curr_val = df_people.loc[df_people["IsID"] == person_id, "Perekonna ID"].values[0]
        if pd.isnull(curr_val):
            connected = bfs_collect(person_id)
            df_people.loc[df_people["IsID"].isin(connected), "Perekonna ID"] = fam_id_counter
            fam_id_counter += 1

    # -------------------------------------------------------------------
    # 12) Clear addresses, then reassign them
    # -------------------------------------------------------------------
    df_people["Aadress"] = None

    # Build a simplified list of couples as (id1, id2) from the earlier 'couples' structure
    real_couples = []
    for (ad1, ad2) in couples:
        real_couples.append((ad1["IsID"], ad2["IsID"]))

    # Married couples share addresses
    for (id1, id2) in real_couples:
        addr1 = df_people.loc[df_people["IsID"] == id1, "Aadress"].values[0]
        addr2 = df_people.loc[df_people["IsID"] == id2, "Aadress"].values[0]
        if pd.isnull(addr1) and pd.isnull(addr2):
            # pick a random address
            chosen_address = random.choice(possible_addresses)
            df_people.loc[df_people["IsID"] == id1, "Aadress"] = chosen_address
            # 80% chance spouse uses same address
            if random.random() < 0.8:
                df_people.loc[df_people["IsID"] == id2, "Aadress"] = chosen_address
            else:
                df_people.loc[df_people["IsID"] == id2, "Aadress"] = random.choice(possible_addresses)

    # Assign random addresses to single adults with no address
    for idx, row_ in df_people.iterrows():
        if row_["Vanuse staatus"] == "Täiskasvanu" and pd.isnull(row_["Aadress"]):
            df_people.at[idx, "Aadress"] = random.choice(possible_addresses)

    # Children address logic
    for idx, row_ in df_people.iterrows():
        if row_["Vanuse staatus"] == "Laps":
            birth_date = row_["Sünniaeg"]
            age_years = 0
            if pd.notnull(birth_date):
                age_years = (now - birth_date).days // 365

            parents_list = row_["Vanem(ad)"]
            if parents_list:
                chosen_parent_id = random.choice(parents_list)
                parent_addr = df_people.loc[df_people["IsID"] == chosen_parent_id, "Aadress"].values[0]
                if pd.isnull(parent_addr):
                    parent_addr = random.choice(possible_addresses)

                # If < 18, always parent's address; if >= 18, 70% parent's address
                if age_years < 18:
                    df_people.at[idx, "Aadress"] = parent_addr
                else:
                    if random.random() < 0.7:
                        df_people.at[idx, "Aadress"] = parent_addr
                    else:
                        df_people.at[idx, "Aadress"] = random.choice(possible_addresses)
            else:
                # no parents => just pick a random address
                df_people.at[idx, "Aadress"] = random.choice(possible_addresses)

    # -------------------------------------------------------------------
    # 13) Fill "KOV" by referencing 'AkpIDTase2' in df_possible_aadresses
    # -------------------------------------------------------------------
    df_people["KOV"] = None
    if df_possible_aadresses is not None and "AkpIDTase2" in df_possible_aadresses.columns:
        for idx, row_ in df_people.iterrows():
            adr_id = row_["Aadress"]
            match = df_possible_aadresses.loc[df_possible_aadresses["AdrID"] == adr_id, "AkpIDTase2"]
            if len(match) > 0:
                df_people.at[idx, "KOV"] = match.iloc[0]
            else:
                df_people.at[idx, "KOV"] = None
    else:
        # If no df_possible_aadresses or no AkpIDTase2 column, KOV remains None
        pass

    # -------------------------------------------------------------------
    # 14) Arrival rule for child if parent arrived after child's birth
    # -------------------------------------------------------------------
    for idx, row_ in df_people.iterrows():
        if row_["Vanuse staatus"] == "Laps":
            if row_["Kodakondsus"] != "EE":
                parents_list = row_["Vanem(ad)"]
                child_birth = row_["Sünniaeg"]
                if parents_list and pd.notnull(child_birth):
                    for pid in parents_list:
                        parent_arrival = df_people.loc[df_people["IsID"] == pid, "IsSaabusEesti"].values[0]
                        if pd.notnull(parent_arrival) and parent_arrival > child_birth:
                            # child must also arrive
                            if pd.isnull(row_["IsSaabusEesti"]):
                                earliest_ = max(child_birth, parent_arrival)
                                if earliest_ < now:
                                    new_saabumine = random_date(earliest_, now)
                                    df_people.at[idx, "IsSaabusEesti"] = new_saabumine
                                    # 20% chance child also leaves
                                    if random.random() < 0.2:
                                        df_people.at[idx, "IsLahkusEestist"] = random_date(new_saabumine, now)
                            break

    # -------------------------------------------------------------------
    # 15) Assign realistic AsID values to adults
    # -------------------------------------------------------------------
    df_people["AsID"] = None
    df_people["vanus_aastates"] = None

    for idx, row_ in df_people.iterrows():
        bd = row_["Sünniaeg"]
        if pd.notnull(bd):
            df_people.at[idx, "vanus_aastates"] = (now - bd).days // 365

    # Filter adults (18+)
    adult_indexes = df_people[df_people["vanus_aastates"] >= 18].index.tolist()
    random.shuffle(adult_indexes)

    # Get asutus IDs
    if df_possible_aadresses is not None and "AsID" in df_possible_aadresses.columns:
        asutus_ids = df_possible_aadresses["AsID"].dropna().unique().tolist()
    else:
        # fallback to 1..50 if unknown
        asutus_ids = list(range(1, 51))

    if not asutus_ids:
        asutus_ids = list(range(1, 6))  # minimum fallback

    # Assign AsID to all adults with 10% probability
    for idx in adult_indexes:
        if random.random() < 0.1:
            df_people.at[idx, "AsID"] = random.choice(asutus_ids)

    return df_people
