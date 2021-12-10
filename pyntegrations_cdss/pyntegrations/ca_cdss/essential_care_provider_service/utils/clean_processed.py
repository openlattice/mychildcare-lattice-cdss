from geopy.distance import lonlat, distance
from dateutil.parser import parse
from dateutil.tz import gettz
from datetime import datetime
from dateutil import parser
import pandas as pd
import numpy as np
import requests
import hashlib
import random
import geopy
import math
import pytz
import os
import re
import time

# Small functions used in the larger functions in combine_facilities.py and integration_definitions.py

def get_zip(x):
    if pd.isna(x):
        return None
    spl = str(x).split(" ")
    spl = [x for x in spl if not x == ""]
    if len(spl) > 0:
        num = float(spl[0])
        if not np.isnan(num):
            return(int(num))
    
def get_coordinates(lon,lat):
    if pd.isna(lon) or pd.isna(lat) or lon == '' or lat == '':
        return None
    else:
        try:
            return "%s,%s"%(np.round(np.float(lon), 6),np.round(np.float(lat), 6))
        except TypeError:
            print(f'lat:{lat}, lon:{lon}')

def pretty_case(x):
    if pd.isna(x):
        return None
    return " ".join([y.capitalize() for y in str(x).split(" ")])
    
def get_distance_from_rows(row1, row2):
    try:
        row1_lonlat = lonlat(row1.geo_x, row1.geo_y)
        row2_lonlat = lonlat(row2.geo_x, row2.geo_y)
        return(distance(row1_lonlat, row2_lonlat).miles)
    except ValueError:
        print("ALARM")

def get_distance_from_values(lat1, lon1, lat2, lon2):
    try:
        row1_lonlat = lonlat(lon1, lat1)
        row2_lonlat = lonlat(lon2, lat2)
        return(distance(row1_lonlat, row2_lonlat).miles)
    except ValueError:
        print("ALARM")

def get_string_address(hospital):
    strings = [
        hospital['ecps.facility_name'][0],
        hospital['ecps.facility_address'][0],
        hospital['ecps.facility_zip'][0],
        hospital['ecps.facility_city'][0]
    ]
    
    return ", ".join(strings)

def get_license_url(facility_id):
    fac_9 = str(facility_id).zfill(9)
    url = "https://www.ccld.dss.ca.gov/carefacilitysearch/FacDetail/%s"%fac_9
    return(url)

def jitterLocation(location, seed=0, maxMeters=200):
    origin = geopy.Point(location[0], location[1])
    random.seed(seed)             #generate same random number each time - by using same seed
    b = random.randint(0, 360)    #random int btwn 0-360
    d = math.sqrt(random.random()) * (float(maxMeters) / 1000)
    destination = geopy.distance.distance(kilometers=d).destination(origin, b)
    return (destination.latitude, destination.longitude)

def get_jittered_location_from_zip(zip):
    """
    This function seems to be deprecated.

    :param zip: The zipcode of the facility.
    :return: A location that is scattered around the geographic center of the zipcode.
    """
    url=os.path.join(
        "https://osm.openlattice.com/",
        "search?format=json&addressdetails=1&limit=1&q={zip}".format(zip = str(zip)))
    r = requests.get(url = url)
    geocoded = r.json()[0]

    if not (geocoded['address']['state'] == "California" and geocoded['type'] == "postcode"):
        return None
    jittered = jitterLocation([geocoded['lat'], geocoded['lon']], maxMeters = 200)
    coords = get_coordinates(jittered[0], jittered[1])
    return coords

def get_jittered_location_from_lonlat(lon, lat, seed, meters = 200):
    if pd.isna(lon) or pd.isna(lat):
        return None
    jittered = jitterLocation([lon, lat], seed, maxMeters=meters)
    coords = get_coordinates(jittered[0], jittered[1])
    return coords

def reformat_phone(number_string):
    if number_string is None:
        return None
    if number_string == 'PHONE MASKED':
        return number_string
    if number_string == "None":
        return None
    number_string = number_string.split('ext')[0].strip()
    number_string = re.sub('[^0-9]','', number_string)
    phone_list = re.sub("[^0-9]", "", str(int(np.float(number_string))))
    reformatted = "("+"".join(phone_list[:3])+") "+"".join(phone_list[3:6])+ "-"+"".join(phone_list[6:10])
    if len(phone_list) > 10:
        ext = " x "+"".join(phone_list[10:])
        reformatted = reformatted + ext
    return reformatted

def get_date(object_in):
    if object_in is None:
        return pd.NaT
    if pd.isnull(object_in):
        return pd.NaT
    if isinstance(object_in, datetime):
        return(object_in.strftime("%Y/%m/%d"))
    else:
        if str(object_in) == "":
            return pd.NaT
        if str(object_in) == 'nan':
            return pd.NaT
        try:
            return parser.parse(str(object_in)).strftime("%Y/%m/%d")
        except parser.ParserError:
            print("Couldn't deserialize %s"%str(object_in))
            return pd.NaT

def mask(row, column, bool_col = "hide_contact"):
    if row[bool_col]:
        return None
    else:
        return row[column]

# For opening/closing hours
def time_to_datetime(value):
    # tz = pytz.timezone('America/Los_Angeles')
    tz = pytz.utc  # we process the operating hours as UTC, and apparently it shows the correct time (which is PST).
                  # If marked as PST it will appear incorrectly.
    if value == "":
        return pd.NaT
    if value is None:
        return pd.NaT
    dtloc = tz.localize(parser.parse(value).replace(day=1, month=1))
    return dtloc

# For setting the timezone to PST of given datetimes
def datetime_clean(col):
    if pd.isnull(col):
        datetime_col = datetime.now(gettz("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M:%S") #to prevent ms in isoformat
    else:
        tzinfos = gettz("America/Los_Angeles")
        col2 = str(col)
        pdt_stamp = parse(col2).replace(tzinfo=tzinfos)
        datetime_col = str(pdt_stamp)

    return datetime_col

# WHY ARE WE FILLING EMPTY COLS WITH THE 1/1/2000? very few empty cells though.
def mccp_vacancy_updated_date(col):
    if pd.isnull(col) or col == "":
        datetime_col = None
        # tzinfos = gettz("America/Los_Angeles")
        # datetime_col = pd.Timestamp('200001010000+00', tzinfo=tzinfos)
    else:
        tzinfos = gettz("America/Los_Angeles")
        col2 = str(col)
        pdt_stamp = parse(col2).replace(tzinfo=tzinfos).isoformat()
        datetime_col = str(pdt_stamp)

    return datetime_col

def hash_columns(row):
    newstr = "".join(filter(None, [row.rrname, row.rrurl, row.rrcounty]))
    return hashlib.sha256(newstr.encode('utf-8')).hexdigest()

def get_hospital_match(fac_row, hospital_loc):
    if np.isnan(fac_row.geo_x):
        print("No geocoding for facility with id %s" % fac_row.id_ccp)
        return (pd.Series({"id_fac": fac_row.id_ccp}))

    all_hospitals = hospital_loc.apply(
        lambda hospital_row: get_distance_from_rows(fac_row, hospital_row),
        axis=1
    )
    minidx = all_hospitals.idxmin()
    bestrow = hospital_loc.loc[minidx]

    return (pd.Series({
        "id_hospital": bestrow.id_hospital,
        "id_ccp": fac_row.id_ccp,
        "distance_miles": all_hospitals.loc[minidx],
        "id_closeto": str(bestrow.id_hospital) + str(fac_row.id_ccp)
    }))


def geocode_facilities(row):
    '''
    Function now takes in a row of data. It attempts to geocode the row 3 different times:
        1. First by {row.faddress}, {row.fcity} {row.fzip}
        2. If that fails then {row.faddress}, {row.fcity}, CA
        3. If that fails then by zip
    :param row: a dict that contains faddress, fcity, fzip
    :return: a dict contain containing geo_x (longitude) and geo_y (latitude)
    '''
    if not pd.isna(row.geo_x):
        return pd.Series({"geo_y": row.geo_y, "geo_x": row.geo_x})

    # Attempt 1: try using the OL hosted OSM
    geocode_url = "https://osm.openlattice.com/search"

    addr_string = f'{row.faddress}, {row.fcity} {row.fzip}'
    params = {'q': addr_string, 'addressdetails': '1', 'format': 'json', "limit": "1"}
    out = requests.get(geocode_url, params=params)

    if out.status_code == 200:
        addr = out.json()

        if len(addr) > 0:
            return pd.Series({"geo_y": addr[0]['lat'], "geo_x": addr[0]['lon']})

    # Attempt 2: try using the OL hosted OSM with just the zip code
    params = {'q': row.fzip + ', USA', 'addressdetails': '1', 'format': 'json', 'limit': "1"}
    out = requests.get(geocode_url, params=params)

    if out.status_code == 200:
        addr = out.json()

        if len(addr) > 0:
            return pd.Series({"geo_y": addr[0]['lat'], "geo_x": addr[0]['lon']})

    # Attempt 3: try using the OL hosted OSM with the city and abbreviation CA
    params = {'q': row.fcity + ', CA', 'addressdetails': '1', 'format': 'json', 'limit': "1"}
    out = requests.get(geocode_url, params=params)

    if out.status_code == 200:
        addr = out.json()

        if len(addr) > 0:
            return pd.Series({"geo_y": addr[0]['lat'], "geo_x": addr[0]['lon']})

    # Attempt 4: try using public OSM (this is the worst case scenario)
    out = requests.get('https://nominatim.openstreetmap.org/search?country=usa&format=json&postalcode=' + row.fzip)
    time.sleep(2)
    print('sleeping to avoid limiting')

    if out.status_code == 200:
        addr = out.json()
        if len(addr) > 0:
            return pd.Series({"geo_y": addr[0]['lat'], "geo_x": addr[0]['lon']})

    return pd.Series()


def geocode_new_facilities(df, engine, coordinates_with_address):
    '''
    This function uses osm to geocode all previously un-geocoded locations. The goal is to speed up the geocoding
    process by never geocoding the same location twice.

    :param df: Data frame that needs to be geocoded
    :return: The same data frame with more complete geo_x, geo_y, and fcoordinates columns
    '''
    # Reformats the data being read into the table
    df['faddress'] = df['faddress'].str.upper()
    df['fcity'] = df['fcity'].str.upper()
    df['fzip'] = df.fzip.str[0:5]

    # First try and get coordinates from the addresses
    df_all = df[['fname', 'faddress', 'fcity', 'fzip']].merge(coordinates_with_address,
                                                              how='left',
                                                              left_on=['faddress', 'fcity', 'fzip'],
                                                              right_on=['faddress', 'fcity', 'fzip'])

    # Create the geo_x and geo_y columns, then create a dataframe for rows to be geocoded.
    df_all[['geo_y', 'geo_x']] = df_all['fcoordinates'].str.split(',', expand=True)
    df_needs_geocoding = df_all[df_all['fcoordinates'].isna()].copy()

    # Skip this part if all locations have been geocoded
    if len(df_needs_geocoding) > 0:
        print("New coordinates need to be geocoded!")

        # This calls the function that tries to geocode the address. If that fails,
        # then the function tries to geocode the zip code.
        codes = df_needs_geocoding.apply(geocode_facilities, axis=1)
        df_needs_geocoding['fcoordinates'] = codes['geo_y'] + ',' + codes['geo_x']
        df_needs_geocoding['geo_y'], df_needs_geocoding['geo_x'] = codes['geo_y'], codes['geo_x']

        # Appends the new coordinates to the master table, and writes it to postgres
        new_coords = df_needs_geocoding.loc[
            df_needs_geocoding['faddress'].notna(), ['faddress', 'fcity', 'fzip', 'fcoordinates']]
        new_coords.to_sql('zzz_locationcoordinates_with_address', engine,
                          if_exists='append', index=False, method='multi')

        print("Added x new rows to the geocode cache: " + str(len(new_coords)))

        df_all = df_all[df_all['fcoordinates'].notna()].append(df_needs_geocoding)

    else:
        print("No new coordinates added to geocode cache.")

    return df_all[['geo_y', 'geo_x']]

