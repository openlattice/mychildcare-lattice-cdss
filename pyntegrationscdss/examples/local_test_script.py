import os
import pyntegrationscdss
from pyntegrationscdss.pyntegrations.ca_cdss.essential_care_provider_service.utils import flight
from pyntegrationscdss.pyntegrations.ca_cdss.essential_care_provider_service.utils import openlattice_functions as of

'''
    Calls upon "get_jwt" and "get_config" in openlattice_functions.py to obtain your authorization to the needed entity sets
    by grabbing username and password from the environment. If this job is run in Rundeck, the function grabs
    them from Rundeck key storage.  
'''
user = os.environ.get("RD_OPTION_OL_USER") # credential is stored within Rundeck and retrieved here
pw = os.environ.get("RD_OPTION_OL_PASSWORD") # this credential is stored within Rundeck and retrieved here
client_id = os.environ.get("RD_OPTION_OL_CLIENT_ID") # this credential is stored within Rundeck and retrieved here

token = of.get_jwt(user, pw, client_id)
configuration = of.get_config(jwt = token, base_url='https://api.openlattice.com')


integrations = [
    pyntegrationscdss.pyntegrations.ca_cdss.essential_care_provider_service.integration_definitions.EssentialCareProviderServiceIntegration,
    pyntegrationscdss.ca_cdss.essential_care_provider_service.integration_definitions.RRIntegration,
    pyntegrationscdss.ca_cdss.essential_care_provider_service.integration_definitions.HospitalIntegration,
    pyntegrationscdss.ca_cdss.essential_care_provider_service.integration_definitions.FacilitiesToRRIntegration,
    pyntegrationscdss.ca_cdss.essential_care_provider_service.integration_definitions.FacilitiesToHospitalsIntegration
]

for x in integrations:
    print("")
    print("°º¤ø,¸¸,ø¤º°`°º¤ø,¸,ø¤°º¤ø,¸¸,ø¤º°`°º¤ø,¸¸,ø¤º°`°º¤ø,¸")
    print(f"°º¤ø,¸   Running integration {x.__name__}")
    print("°º¤ø,¸¸,ø¤º°`°º¤ø,¸,ø¤°º¤ø,¸¸,ø¤º°`°º¤ø,¸¸,ø¤º°`°º¤ø,¸")
    print("")
    case = x()
    fl = flight.Flight(configuration=configuration)
    fl.deserialize(case.flight_path)
    # fl.organization_id = "00000000-0000-0001-0000-000000000000"
    # rep2 = fl.check_entsets_against_stack(create_by=['kim@openlattice.com'])

    clean_table_name = case.clean_and_upload()
    case.integrate_table(
        clean_table_name=clean_table_name,
        shuttle_path=os.path.join(
            os.environ.get("HOME"),
            "opt/openlattice/shuttle/bin/shuttle",
        ),
        # local=True, #uncomment this if you are integrating to your local computer (and have a copy of the stack running)
        shuttle_args =  ' --upload-size 1000'
    )
