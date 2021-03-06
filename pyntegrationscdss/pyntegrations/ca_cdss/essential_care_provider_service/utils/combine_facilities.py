from dateutil import parser
import sqlalchemy as sq
import pandas as pd
import numpy as np
import datetime
import os

from pyntegrationscdss.pyntegrations.ca_cdss.essential_care_provider_service.utils import clean_raw, clean_processed
# from pyntegrationscdss.pyntegrations.ca_cdss.essential_care_provider_service.utils import openlattice_functions as of

# Takes 3 source tables received from CDSS and combines them into 1 called 'f"ccl_openlattice_{datetime.now().strftime("%Y_%m_%d")}'
# ...and this combined table is what gets passed into the integration cleaning function
def get_and_process_ccp_data(organization_id):
    print("reading popup data...")
    # client_id = os.environ.get("RD_OPTION_OL_CLIENT_ID") # this credential is stored within Rundeck and retrieved here
    # token = of.get_jwt(user, pw, client_id)
    # conf = of.get_config(jwt = token, base_url='https://api.openlattice.com')

    dbuser = os.environ.get("RD_OPTION_DB_USER") # credential is stored within Rundeck and retrieved here
    dbpw = os.environ.get("RD_OPTION_DB_PASSWORD") # this credential is stored within Rundeck and retrieved here
    db = "org_ab960cc5511b4fd7ad39e365ead19ba8"  # STAGING DATABASE NAME
    engine = sq.create_engine(f'''postgresql://{dbuser}:{dbpw}@atlas-writer.cukntkiejy0u.us-west-2.rds.amazonaws.com:30001/{db}''')


    print("reading closure data...")
    clos_data = pd.read_sql_table('cdss_export_facilityclosures', engine) #auto-updated table via launchpad
    clos = process_closures(clos_data)

    print("reading mccp data...")
    mccp_data = pd.read_sql_table("cdss_export_cdss_export", engine) #auto-updated table via launchpad
    mccp = process_mccp(mccp_data)

    print("reading facility universe data...")
    universe_data = pd.read_sql_table('cdss_export_mcc_facilityuniverse', engine) #auto-updated table via launchpad
    universe = process_facility_universe(universe_data)

    # Get the static CDN data from an atlas table, using function defined at bottom
    print("reading latest CDN data...")
    cdn_data = pd.read_sql_table("ccp_licensed_67e885d41762", engine)
    cdn = process_cdn_latest(cdn_data)

    print("merging...")
    # facility universe to closures
    merged = pd.merge(universe.assign(fnumber=universe.fnumber.astype(str)),
                      clos.assign(fnumber=clos.fnumber.astype(str)),
                      how='left', on='fnumber')

    # add cdn info (everbridge + geo)
    merged = pd.merge(merged.assign(fnumber=merged.fnumber.astype(str)),
                      cdn.assign(
                          fnumber=cdn.fnumber.astype(str)),
                      how='left', on='fnumber')

    # mask small family homes
    smallhomes = np.where((merged.ftype == 'Family Home') | (merged.fcap <= 8))[0]
    merged.at[smallhomes, 'ftelephone'] = "PHONE MASKED"

    hide_name = np.where((merged.ftype == 'Family Home') & (merged.fcap <= 8))[0]
    merged.at[hide_name, 'fname'] = "FACILITY NAME MASKED"

    # popup data
    # merged = pd.merge(merged.assign(fnumber=merged.fnumber.astype(str)),
    #                   popup.assign(fnumber=popup.fnumber.astype(str)),
    #                   how='left', on='fnumber')

    # add mccp data
    merged = pd.merge(merged.assign(fnumber=merged.fnumber.astype(str)),
                      mccp.assign(fnumber=mccp.fnumber.astype(str)),
                      how='left', on='fnumber')

    merged.ccl_closure_last_update = merged.ccl_closure_last_update.astype('datetime64[ns, America/Los_Angeles]') # the 'Modified' datetime from closures file
    merged.eb_last_update = merged.eb_last_update.astype('datetime64[ns, US/Eastern]')
    merged.mccp_last_update = merged.mccp_last_update.astype('datetime64[ns, US/Eastern]')
    merged.ccp_last_update = merged.ccp_last_update.astype('datetime64[ns, US/Eastern]')

    return merged


## This used to be to process an excel file before we set up launchpad.
# # Doesn't seem to be used anymore
# def process_popup(filepath):
#     '''
#     :param filepath: local path of excel file generated by CCL
#     :return: clean popup data
#
#     At this point, the only information coming from this dataset is whether
#     the state issued the facility a waver, what ???type??? of waiver that was,
#     and the data of issue.
#     '''
#     popup = pd.read_excel(filepath)
#
#     popup = popup \
#         .rename(
#         columns={
#             "Point of Contact Phone": "ftelephone",
#             'Point of Contact Email': "femail",
#             "Facility Name": "fname",
#             "Facility License Number (if applicalble)": "fnumber",
#             'Modified': "modifydate",
#             "Waiver: Date Requested": "waiverdate",
#             "Waiver: Reason": 'waiverreason'
#         }) \
#         .dropna(subset=['fnumber'])
#
#     popup = popup[popup.fnumber != "Not Applicable"]
#
#     popup.fnumber = popup.fnumber. \
#         astype(int). \
#         astype(str). \
#         str.zfill(9)
#
#     popup = popup.drop_duplicates(subset='fnumber')
#
#     return popup[['waiverdate', 'waiverreason', 'fnumber']]

# Clean up the facilities closure atlas table
def process_closures(clos):
    '''
    :param clos: dataset generated by CCL
    :return: clean popup data

    Function to return closure information on facilities
    '''

    def parse_datetime_if_can(dt):
        if isinstance(dt,datetime.datetime):
            return dt
        try:
            return parser.parse(str(dt))
        except:
            if dt == '4/15/2020  7:26:51 AM+N3669P3574N3596:N3649':
                return datetime.datetime(
                    month = 4,
                    day = 15,
                    year = 2020,
                    hour = 7,
                    minute = 26,
                    second = 51
                )
            return dt

    clos = clos \
        .rename(
        columns={
            "Facility Name": "fname",
            "Facility Number ": "fnumber",
            "Modified": "ccl_closure_last_update"
        }) \
        .dropna(subset=['fnumber'])

    clos.fnumber = clos.fnumber.astype(int).astype(str).str.zfill(9)
    
    clos['closesource'] = "ccp closure data"

    clos['ccl_closure_last_update'] = clos['ccl_closure_last_update'].apply(
        parse_datetime_if_can
    )
    return clos[['ccl_closure_last_update', 'fnumber', 'closesource']]

# clean up the MCCP table
def process_mccp(mccp):
    '''
    :param mccp: dataset generated by CCL
    :return: clean popup data

    Returns processed mccp data
    '''
    mccp = mccp \
        .rename(columns={
        "MondayStartTime1": "mon_start",
        "MondayEndTIme1": "mon_end",
        "TuesdayStartTime1": "tues_start",
        "TuesdayEndTIme1": "tues_end",
        "WednesdayStartTime1": "wed_start",
        "WednesdayEndTIme1": "wed_end",
        "ThursdayStartTime1": "thurs_start",
        "ThursdayEndTIme1": "thurs_end",
        "FridayStartTime1": "fri_start",
        "FridayEndTIme1": "fri_end",
        "SaturdayStartTime1": "sat_start",
        "SaturdayEndTIme1": "sat_end",
        "SundayStartTime1": "sun_start",
        "SundayEndTIme1": "sun_end",
        "ProviderID": "providerid",
        "InfantLicenseNumber": "licensenum_infant",
        "SchoolAgeLicenseNumber": "licensenum_school",
        "LicenseNumber": "licensenum",
        "ActiveStatus": "mccp_activestatus",
        "ReferralStatus": "mccp_referralstatus",
        "InfantVacancies": "mccp_infantvacancies",
        "PreschoolVacancies": "mccp_preschoolvacancies",
        "SchoolAgeVacancies": "mccp_schoolagevacancies",
        "FamilyChildCareVacancies": "mccp_familychildcarevacancies",
        'VacanciesLastUpdated': 'mccp_last_update'
    })


    mccp['hoursavail'] = mccp.apply(lambda x: \
                                        x['Monday'] == "True" \
                                        or x['Tuesday'] == "True" \
                                        or x['Wednesday'] == "True" \
                                        or x['Thursday'] == "True" \
                                        or x['Friday'] == "True" \
                                        or x['Saturday'] == "True" \
                                        or x['Sunday'] == "True", axis=1)
    mccp.hoursavail = mccp.hoursavail\
        .replace(False, "NO")\
        .fillna("NO")\
        .replace(True, "YES")

    # restrict to license only
    mccp = mccp[mccp['Regulation'] != "Exempt"]

    # process age served
    mccp['ageserved'] = mccp.apply(clean_raw.clean_age_served, axis=1)

    tmp = mccp \
        [
        mccp.licensenum_infant.notna() | \
        mccp.licensenum_school.notna()] \
        [['licensenum_infant', 'licensenum_school', 'providerid']]. \
        drop_duplicates(subset=['providerid']). \
        reset_index(drop=True)

    mccp_long_age = pd.wide_to_long(
        tmp,
        stubnames='licensenum',
        i="providerid",
        j='agelicensed',
        sep="_", suffix='\\w+'
    ) \
        .reset_index() \
        .rename(columns={'licensenum': "fnumber"}) \
        .dropna(subset=['fnumber'])

    mccp_long_no_age = mccp[
        mccp.licensenum.notna()][['licensenum', 'providerid']].rename(columns={'licensenum': 'fnumber'})
    mccp_long_no_age['agelicensed'] = None

    mccp_long = pd.concat([mccp_long_age, mccp_long_no_age], sort = False)
    mccp_long = pd.merge(
        mccp,
        mccp_long,
        how='left',
        on='providerid'
    ).reset_index()

    # clean up fnumber
    mccp_long = mccp_long[mccp_long.fnumber.str.isnumeric().fillna(False)]
    mccp_long = mccp_long[~mccp_long.fnumber.isin(['0', '1'])]

    # referral status
    mccp_long['mccp_referralstatus'] = mccp_long['mccp_referralstatus'] \
        .str.lower() \
        .str.replace(
        "no web referrals|no referals|no referrrals",
        "no referrals"
    )
    mccp_long.mccp_referralstatus = mccp_long.mccp_referralstatus.apply(clean_processed.pretty_case)

    mccp_long = mccp_long[[
        'mon_start', 'mon_end',
        'tues_start', 'tues_end',
        'wed_start', 'wed_end',
        'thurs_start', 'thurs_end',
        'fri_start', 'fri_end',
        'sat_start', 'sat_end',
        'sun_start', 'sun_end',
        'hoursavail', 'ageserved',
        'mccp_activestatus',
        'mccp_referralstatus',
        'mccp_infantvacancies',
        'mccp_preschoolvacancies',
        'mccp_schoolagevacancies',
        'mccp_familychildcarevacancies',
        'fnumber', 'mccp_last_update'
    ]]
    mccp_long['mccp_vacancies'] = mccp_long[[
        'mccp_infantvacancies',
        'mccp_preschoolvacancies',
        'mccp_schoolagevacancies',
        'mccp_familychildcarevacancies'
    ]].sum(axis = 1, skipna = True) > 0
    return mccp_long.drop_duplicates(subset="fnumber")

def process_facility_universe(ccp):
    ccp.columns = ccp.columns.str.lower()
    ccp['fstatus'] = ccp.apply(clean_raw.get_status,axis =1)
    ccp['ftype'] = ccp.apply(clean_raw.get_ftype, axis = 1)

    ccp = ccp.rename(columns={
        'fac_name': 'fname',
        'fac_nbr': 'fnumber',
        'fac_phone_nbr': "ftelephone",
        "fac_res_street_addr": 'faddress',
        'fac_res_zip_code': 'fzip',
        'fac_res_city': "fcity",
        'fac_co_name': 'fcounty',
        'fac_co_nbr': 'fcountynum',
        'fac_capacity': "fcap",
        'fac_last_upd_date': "ccp_last_update",
        'type a count': "type_a",
        'fac_email_address': "femail"       #now getting emails from facility universe, previously from CDN popup
    })

    ccp['licensetype'] = ccp.apply(clean_raw.get_license_type, axis=1)
    ccp['regoffice'] = ccp.apply(clean_raw.get_regoffice, axis=1)
    ccp['fclientserved'] = ccp.apply(clean_raw.get_clients_served, axis=1)
    ccp['fnumber'] = ccp.fnumber.astype(int).astype(str).str.zfill(9)

    return ccp[['fstatus', 'ftype',  'fname', 'fnumber', 'ftelephone',
                'faddress', 'fcity', 'fcounty', 'fcountynum', 'fzip',
                'fcap', 'fclientserved', 'licensetype', 'ccp_last_update', 'type_a', 'femail']]

def process_cdn_latest(cdn):
    '''
    :param cdn: dataset created by cdn
    :return: clean popup data

    We're using the last pushed data from CDN for two reasons:
    - EB: the everbridge action finished before the last push. so we can get
          all everbridge data from there, to avoid the need for extra processing
    - geocoding: the geocoding done by CDN was probably done using arcGIS, which
          is probably more reliable than OpenStreetMaps.
    '''

    cdn['fnumber'] = cdn.fnumber.astype(int).astype(str).str.zfill(9)

    cdn_columns = [
        'fnumber',
        'eb_openclose',
        'eb_vacancy',
        'eb_date',
        'eb_lang',
        'eb_closedwilling',
        'geo_x',
        'geo_y',
        # these columns should come from the departments, but I can't find where
        'femail',
        'finspvisitdt_last',
        'fcomplaints'
    ]

    return cdn[cdn_columns].rename(columns = {'eb_date': "eb_last_update",
                                              'femail': 'cdn_email'}) #to avoid name conflict with email from fac universe




