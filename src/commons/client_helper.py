from src.clients.bing.bing_client import BingAdsClient
from src.clients.hubspot.hubspot_client import HubspotClient
from src.clients.linkedin.linkedin_client import LinkedInClient
from src.clients.redshift.redshift_client import RedshiftClient


def get_client(env, client_name, task, db_connection=None):
    if client_name == "bing":
        client = BingAdsClient(task=task, env=env)
        client.name = client_name
        return client
    elif client_name == "redshift":
        client = RedshiftClient(task=task, env=env)
        client.name = client_name
        return client
    elif client_name == "linkedin":
        client = LinkedInClient(task=task, env=env)
        client.name = client_name
        return client
    elif client_name == "hubspot":
        client = HubspotClient(task=task, env=env, db_connection=db_connection)
        client.name = client_name
        return client
    else:
        raise ValueError(
            "client_name is not valid. Must be in ['s3', 'bing', 'reshift', 'linkedin', 'hubspot']"  # noqa: E501
        )
