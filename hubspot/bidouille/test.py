import argparse
import json
import os

from hubspot import HubSpot

# def api_key():
#     load_dotenv()
#     return os.environ["HUBSPOT_API_KEY"]


def import_request(csv_filename, column_mapping):
    return {
        "name": "test_import",
        "files": [
            {
                "fileName": csv_filename,
                "fileImportPage": {
                    "hasHeader": True,
                    "columnMappings": json.loads(column_mapping),
                },
            }
        ],
    }


# parser = argparse.ArgumentParser(description="Parse Hubspot API arguments")
# parser.add_argument("-m", "--method", help="Method to run")
# parser.add_argument("-i", "--import_id", help="Import id")
# parser.add_argument("-f", "--files", help="CSV filename")
# parser.add_argument("-c", "--column", help="Column mapping config")
# parser.add_argument("-k", "--kwargs", help="kwargs to pass")
# args = parser.parse_args()

# if args.method is None:
#     raise Exception(
#         "Please, provide method with -m or --method. See --help to get more info."
#     )

# api_client = HubSpot(api_key=api_key())
api_client = HubSpot(api_key="eu1-fd74-6c90-4dc8-a93b-a1b33969e03c")
# result = api_client.crm.companies.get_all()
result = api_client.crm.o
for r in result:
    print(r)


# from hubspot.crm.associations import BatchInputPublicObjectId, PublicObjectId

# for r in result:
#     # batch_input_public_object_id = BatchInputPublicObjectId(
#     #     inputs=[PublicObjectId(id=r.id)]
#     # )
#     # api_response = api_client.crm.associations.batch_api.read(
#     #     "CONTACTS",
#     #     "ENGAGEMENTS",
#     #     batch_input_public_object_id=batch_input_public_object_id,
#     # )

#     api_response = api_client.events.events_api.get_page(
#         object_type="contacts", object_id=r.id
#     )

#     print(r.id)
#     if api_response.results:
#         for i in api_response.results:
#             if len(i) > 1:
#                 print("DADADADADADADADDADADADADADADA")
#             print(i)
