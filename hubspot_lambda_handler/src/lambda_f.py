# TODO: manage logger for having logger output in terminal when running locally + cleanup print statements

import json
import os
import time

import boto3
import sentry_sdk

# Temporary solution. This import allow init of some envs variables
# TODO: Envs management needs better system.
from configs.globals import CHANNEL
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

# Import redshift here for being able to rollback()/commit() transaction.
from src.clients.redshift.redshift_client import RedshiftClient
from src.commons.task import Task
from src.utils.custom_logger import logger
from src.utils.various_utils import get_running_env


def activate_sentry():
    sentry_sdk.init(
        dsn=os.environ["SENTRY_DNS"],
        integrations=[AwsLambdaIntegration(timeout_warning=True)],
        traces_sample_rate=1.0,
    )
    logger.info("Sentry activated")


# Activate Sentry only when running in Lambda
if os.environ.get("AWS_EXECUTION_ENV") is not None and get_running_env() in [
    "production",
    "staging",
]:
    activate_sentry()


def get_params_json_file_path():
    app_home = os.environ["APPLICATION_HOME"]
    return os.path.realpath(os.path.join(app_home, "configs", CHANNEL, "channel.json"))


def get_channel_params():
    with open(get_params_json_file_path(), "r") as f:
        f = json.load(f)
    return f


def lambda_handler(event, context):
    main()


def run_task(channel, task_name, db_connection):
    """Runs a task

    Returns:
        result: str
        Can be "success" or "error" depending on the task run result.
        A dict with params
    """
    result, destination = Task(
        channel=channel,
        name=task_name,
        db_connection=db_connection,
    ).run()
    return result, destination


def main():
    logger.info("### Starting Ingest lambda ###")
    start = time.time()
    # dd = {}
    # dudu = dd["dede"]

    channel_params = get_channel_params()

    # Daily tasks run
    logger.info(f"Daily tasks run: {channel_params['daily_tasks_list']}")

    db_connection = RedshiftClient().db_connection
    with db_connection.cursor() as cursor:
        cursor.execute("BEGIN;")

    for task_name in channel_params["daily_tasks_list"]:
        result, _ = run_task(channel_params["name"], task_name, db_connection)

    with db_connection.cursor() as cursor:
        cursor.execute("COMMIT;")
    logger.info("All tasks have runned successfully. Daily Worflow ended with success.")

    end = time.time()
    logger.info(end - start)

    logger.info("### Ingest lambda ended ###")

    # Invoke deduplicate Lambda
    logger.info("### Invoke Redshift deduplication Lambda ###")
    event = {
        "schemas": ["hubspot_development"],
        "tables": [],
        "do_delete_duplicates": True,
        "partition_order_by_field": "jab_id",
        "env": "prod",
        "mode": "readwrite",
    }

    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName="jabmo-ingest-redshift-deduplicator-Function-jhJwND5R8vnY",
        Payload=json.dumps(event),
        InvocationType="Event",
    )
    logger.info("### Invocation sent###")


if __name__ == "__main__":
    main()
