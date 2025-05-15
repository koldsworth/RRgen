import random
import pandas as pd
from datetime import datetime, timedelta
from src.generation.utils import random_date, get_education_for_age


def generate_temp_relatsionships(
        n_people: int = 10,
        seed: int = None,
        min_age_for_parenthood: int = 18,
        death_probability: float = 0.1,
        child_death_probability: float = 0.1,
        non_ee_probability: float = 0.3,
        leave_probability: float = 0.2,
        share_address_probability: float = 0.8,
        keep_parent_address_probability: float = 0.7,
        as_id_assign_probability: float = 0.1,
        df_possible_aadresses: pd.DataFrame = None,
        df_kd: pd.DataFrame = None,
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
        10) Assign 'Kodakondsus' (% chance 'EE', % chance 'MUU'). 
            For 'MUU', random arrival date (IsSaabusEesti), % chance also get IsLahkusEestist.
        11) Use a BFS approach to determine 'Perekonna ID' for connected individuals 
            (partner -> children -> parents).
        12) Clear out addresses (Aadress) so we can assign them freshly.
            Then assign addresses for married couples (% chance share the same address).
        13) For single adults with no address assigned, pick a random address.
        14) For children: if under 18, always take a parent's address; 
            if 18 or older, % chance they keep a parent's address, else random.
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
            "Suhteseis": None,  # Will be set if adult
            "Partneri ID": None,
            "Laps(ed)": [],
            "Vanem(ad)": [],
            "Sugu": gender,  # 'MEES' or 'NAINE'
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

    chance_adult_death_all = [random.random() < death_probability for _ in range(len(adults))]

    for idx, ad in enumerate(adults):
        bdate = random_date(earliest_adult, latest_adult)
        ad["Sünniaeg"] = bdate
        if chance_adult_death_all[idx]:
            ad["Surmaaeg"] = random_date(bdate, now)

    # -------------------------------------------------------------------
    # 8) Assign birthdates & deathdates for children
    # -------------------------------------------------------------------
    min_age_days = int(min_age_for_parenthood * 365.25)  # ~ 18 y
    chance_child_death_all = [random.random() < child_death_probability for _ in range(len(children))]


    for idx, child in enumerate(children):
        parents = child["Vanem(ad)"] or []
        parents_bd = [p["Sünniaeg"] for p in data_list if p["IsID"] in parents and p["Sünniaeg"]]

        if parents_bd:
            # Child must be ≥ 18 y younger than each parent
            youngest_allowed_bd = max(bd + timedelta(days=min_age_days) for bd in parents_bd)

            # we still want the kid to be ≤ 1 y old today
            latest_allowed_bd = now - timedelta(days=365)

            # if parents are too young, clamp both ends to the same date
            if youngest_allowed_bd > latest_allowed_bd:
                latest_allowed_bd = youngest_allowed_bd

            child_bd = random_date(youngest_allowed_bd, latest_allowed_bd)
        else:
            # no or unknown parents -> random 0-17-year-old
            child_bd = now - timedelta(days=365 * random.randint(1, 17))
        
        child["Sünniaeg"] = child_bd

        # optional death date (half the adult probability)
        if chance_child_death_all[idx]:
            child["Surmaaeg"] = random_date(child_bd, now)

    # -------------------------------------------------------------------
    # 9) Education assignment via get_education_for_age
    # -------------------------------------------------------------------

    for p in data_list:
        bd = p["Sünniaeg"]
        if bd:
            age_years = (now - bd).days // 365
            p["Haridus"] = get_education_for_age(age_years, df_kd)

    # -------------------------------------------------------------------
    # 10) Kodakondsus & arrival/departure logic
    # -------------------------------------------------------------------
    chance_non_ee_all = [random.random() < non_ee_probability for _ in range(len(data_list))]
    chance_leave_all = [random.random() < leave_probability for _ in range(len(data_list))]

    for idx, p in enumerate(data_list):
        non_ee = chance_non_ee_all[idx]
        p["Kodakondsus"] = "MUU" if non_ee else "EE"

        if non_ee and p["Sünniaeg"]:
            arrival = random_date(p["Sünniaeg"], now)
            p["IsSaabusEesti"] = arrival
            if chance_leave_all[idx]:
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
    real_couples = [(c[0]["IsID"], c[1]["IsID"]) for c in couples]
    chance_share_address_couple = [random.random() < share_address_probability for _ in real_couples]

    # Married couples share addresses
    for idx, (id1, id2) in enumerate(real_couples):
        shared = chance_share_address_couple[idx]
        addr1 = random.choice(possible_addresses)
        df_people.loc[df_people["IsID"] == id1, "Aadress"] = addr1
        if shared:
            df_people.loc[df_people["IsID"] == id2, "Aadress"] = addr1
        else:
            df_people.loc[df_people["IsID"] == id2, "Aadress"] = random.choice(possible_addresses)

    # Assign random addresses to single adults with no address
    for idx, row_ in df_people.iterrows():
        if row_["Vanuse staatus"] == "Täiskasvanu" and pd.isnull(row_["Aadress"]):
            df_people.at[idx, "Aadress"] = random.choice(possible_addresses)

    # Children address logic
    chance_child_keep_parent_addr = [random.random() < keep_parent_address_probability for _ in range(len(df_people))]

    for idx, row in df_people.iterrows():
        if row["Vanuse staatus"] == "Laps":
            birth = row["Sünniaeg"]
            age_years = (now - birth).days // 365 if pd.notnull(birth) else 0
            parents = row["Vanem(ad)"]
            if parents:
                chosen_parent_id = random.choice(parents)
                parent_addr = df_people.loc[df_people["IsID"] == chosen_parent_id, "Aadress"].values[0]
                if pd.isnull(parent_addr):
                    parent_addr = random.choice(possible_addresses)
                if age_years < 18 or chance_child_keep_parent_addr[idx]:
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
                                    # A chance child also leaves
                                    if leave_probability:
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
    asutus_ids = list(range(1, 51))

    # Assign AsID to all adults with a probability
    chance_asid = [random.random() < as_id_assign_probability for _ in adult_indexes]
    for idx in adult_indexes:
        if chance_asid:
            df_people.at[idx, "AsID"] = random.choice(asutus_ids)

    return df_people
