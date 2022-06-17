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
result = api_client.crm.contacts.get_all()


# kwargs = vars(args)
# filtered_kwargs = dict(
#     (k, v)
#     for k, v in kwargs.items()
#     if v is not None and k != "method" and k != "column" and k != "properties"
# )
# if args.method == "create":
#     import_request = import_request(csv_filename=args.files, column_mapping=args.column)
#     filtered_kwargs["import_request"] = json.dumps(import_request)

# result = getattr(api, args.method)(**filtered_kwargs)
print(result)
