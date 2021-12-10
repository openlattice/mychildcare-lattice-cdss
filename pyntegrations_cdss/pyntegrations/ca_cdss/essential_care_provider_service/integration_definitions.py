from ..essential_care_provider_service.utils import combine_facilities, clean_processed
from pyntegrations_cdss.pyntegrations.ca_cdss.essential_care_provider_service.utils.integration_base_classes import Integration
import pyntegrations_cdss.pyntegrations.ca_cdss.essential_care_provider_service.utils as utils
from pkg_resources import resource_filename
from dateutil.tz import gettz
from datetime import datetime
import sqlalchemy as sq
import pandas as pd
import numpy as np
import os

db = "org_5752d58c68a544b0906b67e885d41762" # STAGING DATABASE NAME
user = os.environ.get("RD_OPTION_DB_USER") # credential is stored within Rundeck and retrieved here
pw = os.environ.get("RD_OPTION_DB_PASSWORD") # this credential is stored within Rundeck and retrieved here

class EssentialCareProviderServiceIntegration(Integration):

    def __init__(self):
        super().__init__(
            flight_path=resource_filename(__name__, "childcare_ccp_flight.yaml"),
            sql="select nothing",  # We need to specify sql after the flight
            # has been initiated, since it needs the organization id.
            # The actual sql is initiated below super().__init__(),
            # and this served as a placeholder, otherwise would give
            # an error.
            if_exists="replace",
            clean_table_name_root="essential_care_provider_service_facilities", #"zzz_kimtest_transfer",
            standardize_clean_table_name=False,
            drop_table_on_success=False
        )
        self.sql = self.get_updated_data_sql()
        self.atlas_engine = sq.create_engine(f'''postgresql://{user}:{pw}@atlas-writer.cukntkiejy0u.us-west-2.rds.amazonaws.com:30001/{db}''')

        self.coordinates_with_address = pd.DataFrame(self.atlas_engine.execute('''select distinct * 
                                                                        from zzz_locationcoordinates_with_address
                                                                        where fcoordinates is not null and 
                                                                        fcoordinates != ','; '''),
                                            columns=['faddress', 'fcity', 'fzip',
                                                     'fcoordinates'])
        self.coordinates_with_address['faddress'] = self.coordinates_with_address['faddress'].str.upper()
        self.coordinates_with_address['fcity'] = self.coordinates_with_address['fcity'].str.upper()

    def get_updated_data_sql(self):
        data = combine_facilities.get_and_process_ccp_data(self.flight.organization_id)

        # write data to database
        engine = sq.create_engine(f'''postgresql://{user}:{pw}@atlas-writer.cukntkiejy0u.us-west-2.rds.amazonaws.com:30001/{db}''')
        table_name =  f'ccl_openlattice_{datetime.now().strftime("%Y_%m_%d")}' #'zzz_kim_testcombine'
        data.to_sql(f'{table_name}', engine, chunksize=10000, method="multi", if_exists="replace")
        return f"select * from {table_name}"

    def clean_df(self, df):
        clean_df = pd.DataFrame()

        clean_df['hide_contact'] = (df['fcap'] <= 8)
        clean_df['hide_location'] = (df['fcap'] <= 8) | (df['ftype'] == 'Family Home')

        # enums
        clean_df['facility_type'] = df['ftype'].astype(str).str.title()

        # address
        clean_df['facility_name'] = df['fname'].apply(utils.clean_processed.pretty_case)
        clean_df['facility_email'] = df['femail'].apply(clean_processed.pretty_case)
        clean_df['facility_address'] = df['faddress'].apply(clean_processed.pretty_case)
        clean_df['facility_city'] = df['fcity'].apply(clean_processed.pretty_case)
        clean_df['facility_phone'] = df['ftelephone'].astype(str).apply(clean_processed.reformat_phone)
        clean_df['id'] = df['fnumber']

        # masking contact information
        clean_df.at[clean_df['facility_name'] == "Facility Name Masked", 'facility_name'] = 'Small Family Home'
        clean_df['facility_address'] = clean_df.apply(clean_processed.mask, column='facility_address',
                                                      bool_col='hide_location', axis=1)
        clean_df['facility_email'] = clean_df.apply(clean_processed.mask, column='facility_email',
                                                    bool_col='hide_contact', axis=1)
        clean_df['facility_phone'] = clean_df.apply(clean_processed.mask, column='facility_phone',
                                                    bool_col='hide_contact', axis=1)


        # license
        clean_df['license_type'] = df['licensetype']
        clean_df['license_number'] = df['fnumber']
        clean_df['license_url'] = df['fnumber'].apply(clean_processed.get_license_url)
        clean_df['license_last_inspection_date'] = df['finspvisitdt_last'].apply(clean_processed.get_date)
        clean_df['license_complaints'] = df['type_a'].fillna(-1).astype(int).astype(str) #complaints from facilityuniverse file
        clean_df.at[clean_df.license_complaints == "-1", 'license_complaints'] = None
        clean_df['datasource'] = 'CDSS'

        clean_df['capacity_age_unknown'] = df['fcap']

        clean_df['sunday_start'] = df['sun_start'].apply(clean_processed.time_to_datetime)
        clean_df['sunday_end'] = df['sun_end'].apply(clean_processed.time_to_datetime)
        clean_df['monday_start'] = df['mon_start'].apply(clean_processed.time_to_datetime)
        clean_df['monday_end'] = df['mon_end'].apply(clean_processed.time_to_datetime)
        clean_df['tuesday_start'] = df['tues_start'].apply(clean_processed.time_to_datetime)
        clean_df['tuesday_end'] = df['tues_end'].apply(clean_processed.time_to_datetime)
        clean_df['wednesday_start'] = df['wed_start'].apply(clean_processed.time_to_datetime)
        clean_df['wednesday_end'] = df['wed_end'].apply(clean_processed.time_to_datetime)
        clean_df['thursday_start'] = df['thurs_start'].apply(clean_processed.time_to_datetime)
        clean_df['thursday_end'] = df['thurs_end'].apply(clean_processed.time_to_datetime)
        clean_df['friday_start'] = df['fri_start'].apply(clean_processed.time_to_datetime)
        clean_df['friday_end'] = df['fri_end'].apply(clean_processed.time_to_datetime)
        clean_df['saturday_start'] = df['sat_start'].apply(clean_processed.time_to_datetime)
        clean_df['saturday_end'] = df['sat_end'].apply(clean_processed.time_to_datetime)
        clean_df['hours_unknown'] = 1 - (clean_df['sunday_start'].notnull() | \
                                         clean_df['sunday_end'].notnull() | \
                                         clean_df['monday_start'].notnull() | \
                                         clean_df['monday_end'].notnull() | \
                                         clean_df['tuesday_start'].notnull() | \
                                         clean_df['tuesday_end'].notnull() | \
                                         clean_df['wednesday_start'].notnull() | \
                                         clean_df['wednesday_end'].notnull() | \
                                         clean_df['thursday_start'].notnull() | \
                                         clean_df['thursday_end'].notnull() | \
                                         clean_df['friday_start'].notnull() | \
                                         clean_df['friday_end'].notnull() | \
                                         clean_df['saturday_start'].notnull() | \
                                         clean_df['saturday_end'].notnull()).astype(int)

        # --------------------------------- skip for local testing
            # REPLACE LINES BELOW WITH THESE FOR LOCAL TESTING (run into OSM max hits limit)
        # clean_df['geo_x'] = df['geo_x']
        # clean_df['geo_y'] = df['geo_y']

        # ADVANCED GEOCODING
        clean_df['facility_zip'] = df['fzip']
        clean_df['facility_zip'] = clean_df['facility_zip'].apply(
            lambda x: clean_processed.get_zip(x)).fillna(-1).astype(int)

        print("Geocoding facilities...")
        time1 = datetime.now()
        codes = clean_processed.geocode_new_facilities(df, self.atlas_engine, self.coordinates_with_address)
        df['geo_y'], df['geo_x'] = codes['geo_y'], codes['geo_x']
        time2 = datetime.now()
        print("Geocoding facilities took %i seconds"%(time2 - time1).seconds)

        nojitter = df.apply(
            lambda row: clean_processed.get_coordinates(row.geo_y, row.geo_x), axis=1)
        jitter = df.apply(
            lambda row: clean_processed.get_jittered_location_from_lonlat(
                row.geo_y,
                row.geo_x,
                row.fnumber,
                meters=500), axis=1)

        # jitter all locations
        clean_df['locationcoordinates'] = jitter

        # if hide_location is FALSE, unjitter those locations
        clean_df.at[~clean_df.hide_location, 'locationcoordinates'] = nojitter[~clean_df.hide_location]
        # --------------------------------- skip for local testing

        # Date website was updated.
        # NOTE this just puts in the timestamp of whenever the cleaning is run.
        #   Not from any datetime in the actual data
        clean_df['last_updated'] = datetime.now(gettz("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M:%S") #to prevent ms in isoformat


        cols = [x for x in clean_df.columns if 'start' in x or 'end' in x]

        # openness
        clean_df.loc[df.ccl_closure_last_update.isna(), 'status'] = 'Open'
        clean_df.loc[df.ccl_closure_last_update.notna(), 'status'] = 'Closed'
        clean_df['last_modified'] = df['ccl_closure_last_update'].apply(clean_processed.datetime_clean) #reading the given timestamp as PST

        clean_df['vacancy_last_updated'] = df['mccp_last_update'].apply(clean_processed.mccp_vacancy_updated_date) #vacancy datetime from facility universe
        clean_df['eb_last_update'] = df['eb_last_update'].apply(clean_processed.mccp_vacancy_updated_date)
        clean_df['eb_last_update'] = df['eb_last_update'].fillna(pd.Timestamp('200001010000+0')) #from before, need to check why filling in this date
        clean_df.loc[df.mccp_last_update > df.eb_last_update, 'vacancies'] = \
            df.loc[df.mccp_last_update > df.eb_last_update, 'mccp_vacancies']
        clean_df.loc[df.mccp_last_update < df.eb_last_update, 'vacancies'] = \
            df.loc[df.mccp_last_update < df.eb_last_update, 'eb_vacancy'] == "Vacancies"

        served = pd.DataFrame(
            df['fclientserved'].astype(str).str.split('/').tolist(),
            index=df['fnumber']
        ).stack()
        served = served \
            .reset_index(drop=False) \
            .drop('level_1', axis=1) \
            .rename(columns={0: "clients_served", "fnumber": "id"})

        merged = pd.merge(
            clean_df, served, on='id'
        )

        return merged


class RRIntegration(Integration):
    def __init__(self):
        super().__init__(
            flight_path=resource_filename(__name__, "rr_flight.yaml"),
            sql="""select * from cdss_export_rr;""",
            if_exists="replace",
            clean_table_name_root="essential_care_provider_service_rr", #"kim_rr_test",
            standardize_clean_table_name=False,
            drop_table_on_success=False
        )

    def clean_df(cls, df):
        clean_df = pd.DataFrame()
        clean_df['facility_county'] = df['rrcounty'].str.strip()
        clean_df['facility_name'] = df['rrname']
        clean_df['rr_email'] = df['rremail']
        clean_df['facility_phone'] = df['rrnum'].astype(str).apply(clean_processed.reformat_phone)
        clean_df['url'] = df['rrurl']
        clean_df['id'] = df.apply(lambda x: clean_processed.hash_columns(x), axis=1)
        clean_df['facility_type'] = "RR Network"
        clean_df['facility_address'] = df['rraddress'].apply(clean_processed.pretty_case)
        clean_df['facility_city'] = df['rrcity'].apply(clean_processed.pretty_case)
        clean_df['facility_zip'] = df['rrzip'].apply(lambda x: clean_processed.get_zip(x)).fillna(-1).astype(int)
        clean_df['facility_zip_served'] = df['rrzipserved'].apply(lambda x: clean_processed.get_zip(x)).fillna(-1).astype(int)
        clean_df['datasource'] = "10-23-2020 - updated rr_processed 10.23.2020_geo"

        clean_df['locationcoordinates'] = df.apply(lambda row: clean_processed.get_coordinates(row.geo_y, row.geo_x), axis=1)

        clean_df = clean_df.drop_duplicates()

        return clean_df

class HospitalIntegration(Integration):
    def __init__(self):
        super().__init__(
            flight_path=resource_filename(__name__, "hospitals_flight.yaml"),
            sql="""select facility_type, facility_address, facility_city, locationcoordinates, facility_zip, capacity_age_unknown, id, datasource, facility_name from hospitals;""",
            if_exists="replace",
            clean_table_name_root="essential_care_provider_service_hospitals",
            standardize_clean_table_name=False,
            drop_table_on_success=False
        )

# RR table changes infrequently, but this joins to the newly-created facilities table
# so is run whenever the facilities table is updated
class FacilitiesToRRIntegration(Integration):
    def __init__(self):
        super().__init__(
            flight_path=resource_filename(__name__, "associations_flight.yaml"),
            sql = '''
            select 
              essential_care_provider_service_facilities.id as id_ccp, 
              essential_care_provider_service_facilities.facility_zip as fzip, 
              cdss_export_rr.rrname as rrname,
              cdss_export_rr.rrurl as rrurl,
              cdss_export_rr.rrcounty as rrcounty  
            from essential_care_provider_service_facilities
            inner join cdss_export_rr
            on essential_care_provider_service_facilities.facility_zip = cdss_export_rr.rrzipserved;
            ''',
            if_exists="replace",
            clean_table_name_root="essential_care_provider_service_facilities_to_rr",
            standardize_clean_table_name=False,
            drop_table_on_success=False
        )

    def clean_df(cls, df):
        df.drop_duplicates(subset = ['id_ccp', 'rrname', 'rrurl', 'rrcounty'], inplace = True)
        df['id_rr'] = df.apply(lambda x: clean_processed.hash_columns(x), axis=1)
        df['id_partof'] = df['id_ccp'] + df['id_rr']

class FacilitiesToHospitalsIntegration(Integration):
    def __init__(self):
        super().__init__(
            flight_path=resource_filename(__name__, "associations_flight.yaml"),
            sql = 'select nothing', # see above
            if_exists="replace",
            clean_table_name_root="essential_care_provider_service_facilities_to_hospitals",
            standardize_clean_table_name=False,
            drop_table_on_success=False
        )

        self.sql = self.get_updated_data_sql()


    def get_updated_data_sql(self):

        engine = self.flight.get_atlas_engine_for_organization()
        hospitals = pd.read_sql('select locationcoordinates, id as id_hospital from essential_care_provider_service_hospitals', engine)
        hospitals_loc_splitted = hospitals.locationcoordinates.str.split(",", expand=True)
        hospitals_loc_splitted = hospitals_loc_splitted.replace('', np.nan, regex=True)
        hospitals['geo_y'] = hospitals_loc_splitted[0].astype(float)
        hospitals['geo_x'] = hospitals_loc_splitted[1].astype(float)

        facilities = pd.read_sql('select locationcoordinates, id as id_ccp from essential_care_provider_service_facilities', engine)
        facilities = facilities.drop_duplicates()
        facilities_loc_splitted = facilities.locationcoordinates.str.split(",", expand=True)
        facilities_loc_splitted = facilities_loc_splitted.replace('', np.nan, regex=True)
        facilities['geo_y'] = facilities_loc_splitted[0].astype(float)
        facilities['geo_x'] = facilities_loc_splitted[1].astype(float)

        fac_to_hospital = facilities \
            .parallel_apply(lambda x: clean_processed.get_hospital_match(x, hospitals), axis=1)

        # write data to database
        fac_to_hospital.to_sql('facility_to_hospital', engine, chunksize=10000, method="multi", if_exists="replace")
        return f"select * from facility_to_hospital"

