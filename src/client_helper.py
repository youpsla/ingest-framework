from src.client import BingAdsClient, S3Client
from src.redshift_client import RedshiftClient


def get_client(env, client_name, task):
    if client_name == "s3":
        return S3Client(task=task, env=env)
    if client_name == "bing":
        return BingAdsClient(task=task, env=env)
    if client_name == "redshift":
        return RedshiftClient(task=task, env=env)
