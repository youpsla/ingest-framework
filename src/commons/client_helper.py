from src.clients.bing.bing_client import BingAdsClient
from src.clients.linkedin.linkedin_client import LinkedInClient
from src.clients.redshift.redshift_client import RedshiftClient
from src.clients.s3.s3_client import S3Client


def get_client(env, client_name, task):
    if client_name == "s3":
        client = S3Client(task=task, env=env)
        client.name = client_name
        return client
    elif client_name == "bing":
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
    else:
        raise ValueError(
            "client_name is not valid. Must be in ['s3', 'bing', 'reshift', 'linkedin']"
        )
