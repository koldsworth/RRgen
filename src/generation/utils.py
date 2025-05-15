import random
from datetime import datetime, timedelta
import pandas as pd


def random_date(start: datetime, end: datetime) -> datetime:
    """
    Generate a random date between 'start' and 'end', ensuring valid range.

    If 'start' is later than 'end', it swaps them to avoid errors.

    :param start: The earliest possible date.
    :param end: The latest possible date.
    :return: A random datetime object.
    """
    if start > end:
        start, end = end, start  # Swap dates to prevent issues

    delta = (end - start).days
    if delta <= 0:
        return start  # Return start date if no valid range

    random_days = random.randint(0, delta)
    return start + timedelta(days=random_days)


def generate_isikukood(gender: str, birth_date: datetime) -> str:
    """
    Generate a simplistic Estonian-style personal code (isikukood) based on birthdate and gender.

    Note: This is a simplified example for demonstration purposes.
          In real usage, proper control numbers and uniqueness checks would be needed.

    Logic:
      1) Determine the century marker (sajandikood) based on the gender and birth year.
      2) Extract the two-digit birth year, month, and day.
      3) Create a random 4-digit sequence.
      4) Concatenate these parts into a string.

    :param gender: "MEES" (male) or "NAINE" (female).
    :param birth_date: The person's date of birth.
    :return: A string representing the generated isikukood.
    """
    sajandikood = (
        5 if gender == "MEES" and birth_date.year >= 2000 else
        6 if gender == "NAINE" and birth_date.year >= 2000 else
        3 if gender == "MEES" else
        4
    )
    year = birth_date.year % 100
    month = birth_date.month
    day = birth_date.day
    random_seq = random.randint(1000, 9999)  # Simplistic; no real control digit here

    return f"{sajandikood}{year:02d}{month:02d}{day:02d}{random_seq}"


def get_kdid_for_name(df_kodifikaator: pd.DataFrame, short_name: str):
    """
    Return the 'KdID' from the kodifikaator DataFrame where 'KdLyhikeNimi' equals 'short_name'.

    :param df_kodifikaator: DataFrame containing the kodifikaator table.
    :param short_name: The short name we want to look up, e.g. "KEHTIV".
    :return: The KdID value if found, otherwise None.
    """
    rows = df_kodifikaator.loc[df_kodifikaator['KdLyhikeNimi'] == short_name, 'KdID']
    if len(rows) == 0:
        return None
    return rows.values[0]


def adjust_timeline_for_death(df_kodifikaator: pd.DataFrame, timeline: list, death_date):
    """
    Adjust address (or similar) timeline entries if the person is deceased.

    Logic:
      1) If 'death_date' is None or NaT, do nothing (the person is alive).
      2) Remove all periods that start strictly after the death date.
      3) If a period ends after the death date or has no end,
         truncate that period to the death date and mark it as "Kehtetu" / "Endine elukoht".

    :param df_kodifikaator: DataFrame with code references.
                         E.g., used by get_kdid_for_name for 'ELUS','SURNUD','REGISTRIS','ARHIIVIS'.
    :param timeline: A list of dictionaries, each representing a period with keys like
                     ["IAdrKehtibAlatesKpv", "IAdrKehtibKuniKpv", "KdIDAadressiStaatus", etc.].
    :param death_date: The person's date of death (datetime or None).
    :return: A new list of timeline entries with adjustments for the death date.
    """
    if pd.isnull(death_date):
        # Person is alive; no adjustments needed
        return timeline

    kdid_kehtetu = get_kdid_for_name(df_kodifikaator=df_kodifikaator, short_name='KEHTETU')
    kdid_endine_elukoht = get_kdid_for_name(df_kodifikaator=df_kodifikaator, short_name='ENDINE ELUKOHT')

    updated = []
    for row in timeline:
        start_kpv = row.get("IAdrKehtibAlatesKpv", None)
        end_kpv = row.get("IAdrKehtibKuniKpv", None)

        if start_kpv is None:
            start_kpv = datetime.min

        # 1) Skip periods that begin strictly after the person's death date
        if start_kpv > death_date:
            continue

        # 2) If the period ends after the death date (or is None),
        #    truncate it to the death date and set status to "Kehtetu".
        if end_kpv is None or end_kpv > death_date:
            row["IAdrKehtibKuniKpv"] = death_date
            row["KdIDAadressiStaatus"] = kdid_kehtetu
            row["KdIDAadressiLiik"] = kdid_endine_elukoht

        updated.append(row)

    return updated


def get_education_for_age(age: int, df_kodifikaator: pd.DataFrame):
    """
    Return one educational level 'KdID' from kodifikaator, depending on the person's age,
    using certain probabilities.

    Logic (example mapping):
      - age < 7 => mostly preschool
      - age 7..15 => basic, vocational
      - age 16..18 => secondary, vocational secondary
      - etc.

    :param age: The age in whole years.
    :param df_kodifikaator: The kodifikaator DataFrame, containing KdID and KdElemendiKood.
    :return: A single KdID value representing the chosen educational level.
    """
    kd = df_kodifikaator

    if age < 7:
        # Mostly preschool
        options = kd.loc[kd["KdElemendiKood"].isin(['02', '020']), 'KdID'].tolist()
        return random.choice(options) if options else None

    elif 7 <= age < 16:
        # Basic education or vocational based on basic
        options = kd.loc[kd["KdElemendiKood"].isin(['1', '25']), 'KdID'].tolist()
        return random.choice(options) if options else None

    elif 16 <= age < 19:
        # Secondary, vocational secondary
        options = kd.loc[kd["KdElemendiKood"].isin(['34', '45', '55']), 'KdID'].tolist()
        return random.choice(options) if options else None

    elif 19 <= age < 23:
        # Secondary, vocational secondary, or bachelor's
        options = kd.loc[kd["KdElemendiKood"].isin(['34', '45', '55', '66']), 'KdID'].tolist()
        weights = [0.3, 0.1, 0.2, 0.4]
        return random.choices(options, weights=weights, k=1)[0] if options else None

    else:
        # 23+ => mostly bachelor's or higher
        options = kd.loc[kd["KdElemendiKood"].isin(['34', '45', '55', '66', '76', '86', '99']), 'KdID'].tolist()
        weights = [0.2, 0.1, 0.1, 0.2, 0.2, 0.1, 0.1]
        return random.choices(options, weights=weights, k=1)[0] if options else None
