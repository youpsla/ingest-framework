from src.clients.bing.bing_client import BingAdsClient
from src.clients.hubspot.hubspot_client import HubspotClient
from src.clients.linkedin.linkedin_client import LinkedInClient
from src.clients.on24.on24_client import On24Client
from src.clients.redshift.redshift_client import RedshiftClient


def get_client(env, client_name, task, db_connection=None):
    if client_name == "bing":
        client = BingAdsClient(task=task, env=env)
    elif client_name == "redshift":
        client = RedshiftClient(task=task, env=env)
    elif client_name == "linkedin":
        client = LinkedInClient(task=task, env=env)
    elif client_name == "hubspot":
        client = HubspotClient(task=task, env=env, db_connection=db_connection)
    elif client_name == "on24":
        client = On24Client(task=task, env=env, db_connection=db_connection)
    elif client_name == "eloqua":
        client = EloquaClient(task=task, env=env, db_connection=db_connection)

    else:
        raise ValueError(
            "client_name is not valid. Must be in ['s3', 'bing', 'reshift', 'linkedin', 'hubspot']"  # noqa: E501
        )

    client.name = client_name
    return client
