import sqlalchemy as sq
import os

# Log into atlas
db = "org_ab960cc5511b4fd7ad39e365ead19ba8" # STAGING DATABASE NAME
dbuser = os.environ.get("RD_OPTION_DB_USER") # credential is stored within Rundeck and retrieved here
dbpw = os.environ.get("RD_OPTION_DB_PASSWORD") # this credential is stored within Rundeck and retrieved here

engine = sq.create_engine(f'''postgresql://{dbuser}:{dbpw}@atlas-writer.cukntkiejy0u.us-west-2.rds.amazonaws.com:30001/{db}''')

# This should be rerun if the integration ever needs to be reset. This SQL statement:
# 1. drops the tables if for some reason the tables still exist
# 2. creates a table with addresses, cities, zipcodes, and locationcoordinates
# 3. creates a second table with cities, zipcodes, and locationcoordinates if the address is null
engine.execute('''
    drop table if exists zzz_locationcoordinates_with_address;
    drop table if exists zzz_locationcoordinates_no_address;
    
    create temp table zzz_locationcoordinates on commit drop as
    
    with ranked_facs as (
        select distinct facility_address, facility_city, facility_zip, locationcoordinates,
            dense_rank() over (partition by facility_address, facility_city, facility_zip 
                                order by locationcoordinates) as drank
        from essential_care_provider_service_facilities ecpsf)
        
    select distinct facility_address as faddress, 
        facility_city as fcity, 
        facility_zip::varchar as fzip, 
        locationcoordinates as fcoordinates
    from ranked_facs where drank = 1;
    
    create table zzz_locationcoordinates_with_address as
        select distinct * from zzz_locationcoordinates 
        where faddress is not null;
    
    create table zzz_locationcoordinates_no_address as
        select distinct * from zzz_locationcoordinates 
        where faddress is null;''')


### Note
# All locationcoordinates have a city and a zip. Most have an address, some have none.

# no_add = pd.DataFrame(engine.execute('''
#     with ranked_facs as (
# 	select distinct facility_address, facility_city, facility_zip, locationcoordinates,
# 		dense_rank() over (partition by facility_address, facility_city, facility_zip
# 							order by locationcoordinates) as drank
# 	from essential_care_provider_service_facilities ecpsf),
#
#     unique_facs as (
#         select distinct facility_address, facility_city, facility_zip, locationcoordinates
#         from ranked_facs where drank = 1)
#
#     --select distinct facility_address, facility_city, facility_zip, locationcoordinates
#     --from unique_facs where facility_address is null;
#
#     select distinct
#         case
#             when facility_address is null then 'NoAddress'
#             else 'Address'
#         end as address_present,
#         case
#             when facility_city is null then 'NoCity'
#             else 'City'
#         end as city_present,
#         case
#             when facility_zip is null then 'NoZip'
#             else 'Zip'
#         end as zip_present,
#         count(*)
#     from unique_facs
#     group by address_present, city_present, zip_present;
#     '''))
#
# print(no_add)



