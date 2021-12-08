import pandas as pd
import numpy as np

# Small functions used in the larger combine_facilities.py functions

def clean_age_served(row):
    newstr = row.AgeServed
    if pd.isna(newstr):
        return None
    if isinstance(newstr, np.float):
        return None
    newstr = newstr \
        .replace("infant", "Infants (0 to 23 months)") \
        .replace("infat", "Infants (0 to 23 months)") \
        .replace("Infats", "Infants (0 to 23 months)") \
        .replace("11mos", "Infants (0 to 23 months)") \
        .replace("school age", "School Age (6 years and older)") \
        .replace("preschool", "Preschool (2 to 5 years)") \
        .replace("0mos to 5yrs", "Preschool (2 to 5 years)") \
        .replace("Ages 3 - 4", "Preschool (2 to 5 years)") \
        .replace('Do Not Use', "") \
        .replace("PFA Use Only", "") \
        .replace("Do Not Use", "")

    words = [x.strip() for x in newstr.split(",") if len(x.strip()) > 0]
    words = sorted(words)
    newstr = ", ".join(list(set(words)))

    return newstr


def get_status(row):
    if 7 <= row.fac_status <= 16:
        return "Closed"
    if 20 <= row.fac_status <= 21:
        return "Closed"
    if 1 <= row.fac_status <= 6:
        return "Open"
    return None

def get_ftype(row):
    if row.fac_type in [810]:
        return "Family Home"
    if row.fac_type in [830, 840, 845, 850]:
        return "Day Care Center"
    if row.fac_type.isnull():
        return "School Based Center"

def get_license_type(row):
    if 3 <= row.fac_status <= 6:
        return "Licensed"
    if 7 <= row.fac_status <= 16:
        return "Unlicensed"
    if 20 <= row.fac_status <= 21:
        return "Unlicensed"
    return None

regoffices = {
    1: "Rhonert Park",
    2: "Oakland",
    3: "Sacramento",
    4: "Fresno",
    5: "San Bruno",
    6: "Orange",
    7: "San Jose",
    9: "Riverside",
    10: "Riveride South",
    12: "Palmdale",
    13: "Chico",
    17: "Santa Barbara",
    20: "San Diego",
    30: "El Segundo",
    33: "Monterey Park",
    51: "San Diego North",
    52: "Oakland South",
    53: "Sacramento South",
    54: "Monterey Park South"
}

def get_regoffice(row):
    if row.fac_do_nbr in regoffices.keys():
        return regoffices[row.fac_do_nbr]
    print("Unknown regional office: "+ str(row.fac_do_nbr))

clients_dict = {
    910: "Developmentally Disabled",
    950: "Children",
    955: "Infants",
    960: "Children/Infants",
    961: "Children/Toddlers"
}

def get_clients_served(row):
    if row.fac_client_served in clients_dict.keys():
        return clients_dict[row.fac_client_served]
    print("Unknown clients type: " + str(row.fac_client_served))


