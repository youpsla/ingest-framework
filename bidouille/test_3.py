# Optionally you can include logging to output traffic, for example the SOAP request and response.
# import logging

import json

from src.clients.bing.auth_helper import *
from suds.client import Client
from suds.sudsobject import asdict

# from customermanagement_example_helper import *

# You must provide credentials in auth_helper.py.


# logging.basicConfig(level=logging.INFO)
# logging.getLogger("suds.client").setLevel(logging.DEBUG)
# logging.getLogger("suds.transport.http").setLevel(logging.DEBUG)


def main(authorization_data):

    ## Get accounts informations
    # dede = customer_service.GetLinkedAccountsAndCustomersInfo(
    #     CustomerId=authorization_data.customer_id
    # )
    # print("dodod")
    # print(authorization_data.customer_id)
    # dede = customer_service.GetAccountsInfo(CustomerId=authorization_data.customer_id)
    # print(dede)

    # dada = recursive_asdict(dede)
    # accounts = dada["AccountsInfo"]["AccountInfo"]
    # for d in accounts:
    #     print(d)

    # for d in dede:
    ## Get campaigns informations
    # print("dodo")
    # dede = campaign_service.GetCampaignsByAccountId(
    #     CampaignType=["Search Audience DynamicSearchAds Shopping"],
    #     AccountId=authorization_data.account_id,
    # )

    # print("dodo")
    # dede = campaign_service.GetMediaMetaDataByAccountId(
    #     MediaEnabledEntities="ResponsiveAd", PageInfo=None
    # )

    # 7559316666939
    dede = campaign_service.GetMediaAssociations(
        MediaEnabledEntities="ResponsiveAd ImageAdExtension",
        MediaIds={"long": [7559219463432]},
    )

    dada = recursive_asdict(dede)
    print(dada)
    import pprint

    print("dede")
    print(pprint.pprint(dada))
    print("zaza")


# pass


def recursive_asdict(d):
    """Convert Suds object into serializable format."""
    out = {}
    for k, v in asdict(d).items():
        if hasattr(v, "__keylist__"):
            out[k] = recursive_asdict(v)
        elif isinstance(v, list):
            out[k] = []
            for item in v:
                if hasattr(item, "__keylist__"):
                    out[k].append(recursive_asdict(item))
                else:
                    print(item)
                    out[k].append(item)
        else:
            out[k] = "%s" % v
    return out


def suds_to_json(data):
    return json.dumps(recursive_asdict(data))


# Main execution
if __name__ == "__main__":

    print("Loading the web service client proxies...")

    authorization_data = AuthorizationData(
        account_id=None,
        customer_id=None,
        developer_token=DEVELOPER_TOKEN,
        authentication=None,
    )

    customer_service = ServiceClient(
        service="CustomerManagementService",
        version=13,
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
    )

    campaign_service = ServiceClient(
        service="CampaignManagementService",
        version=13,
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
    )

    print(authorization_data)

    authenticate(authorization_data)

    main(authorization_data)
