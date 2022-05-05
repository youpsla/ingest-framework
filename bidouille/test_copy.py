import boto3
import psycopg2
from src.clients.redshift.redshift_client import RedshiftClient

db_connection = RedshiftClient().db_connection


sql = """copy bing.demographic_metrics (time_period, account_id, account_name, campaign_id, adgroup_id, adgroup_name, company_name, industry_name, job_function, impressions, clicks, spend) from 's3://linkedin-ingest-dev-staging/application=ingest/channel=bing/environment=development/state=unprocessed/task=daily_demographic_metrics_update/' iam_role 'arn:aws:iam::467882466042:role/aa-ingest-dev-redshift-s3' csv IGNOREHEADER 1 FILLRECORD;"""


# sql = """copy bing.demographic_metrics from 's3://linkedin-ingest-dev-staging/application=ingest/channel=bing/environment=development/state=unprocessed/task=daily_demographic_metrics_update/' iam_role 'arn:aws:iam::467882466042:role/aa-ingest-dev-redshift-s3' csv IGNOREHEADER 1 FILLRECORD;"""

with db_connection.cursor() as cursor:
    cursor.execute(sql)
    db_connection.commit()
