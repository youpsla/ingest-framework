import json
import os
import time

import boto3
import sentry_sdk
from sentry_sdk.integrations.excepthook import ExcepthookIntegration

from configs.globals import APP_VERSION, PROVIDER, SENTRY_DSN, TASK_GROUP
from src.clients.redshift.redshift_client import RedshiftClient
from src.commons.task import Task
from src.utils.custom_logger import logger
from src.utils.various_utils import get_running_env, get_schema_name


def activate_sentry():
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        integrations=[ExcepthookIntegration(always_run=True)],
        traces_sample_rate=1.0,
        environment=get_running_env()
    )
    sentry_sdk.set_tag("provider", PROVIDER)
    sentry_sdk.set_tag("task_group", TASK_GROUP)
    sentry_sdk.set_tag("app_version", APP_VERSION)
    logger.info("Sentry activated")


# Activate Sentry only when running in Lambda
if os.environ.get("AWS_EXECUTION_ENV") is not None and get_running_env() in [
    "production",
    "staging",
]:
    activate_sentry()


def get_params_json_file_path():
    app_home = os.environ["APPLICATION_HOME"]
    return os.path.realpath(
        os.path.join(app_home, "configs", PROVIDER, "channel.json")
    )


def get_channel_params():
    with open(get_params_json_file_path(), "r") as f:
        f = json.load(f)
    return f


def run_task(channel, task_name, db_connection):
    """Runs a task
    """
    result, destination = Task(
        channel=channel,
        name=task_name,
        db_connection=db_connection,
    ).run()
    return result, destination


def lambda_handler(event, context):
    logger.info("### Starting Ingest lambda ###")
    start = time.time()

    channel_params = get_channel_params()

    task_group_list = channel_params[TASK_GROUP]

    logger.info(f"Tasks group: {TASK_GROUP}")
    logger.info(f"Tasks names: {task_group_list}")
    logger.info(f"App version: {APP_VERSION}")

    db_connection = RedshiftClient().db_connection
    with db_connection.cursor() as cursor:
        cursor.execute("BEGIN;")

    for task_name in task_group_list:
        result, _ = run_task(channel_params["name"], task_name, db_connection)

    with db_connection.cursor() as cursor:
        cursor.execute("COMMIT;")
    logger.info(
        "All tasks have runned successfully. Daily Worflow ended with success."
    )

    end = time.time()
    logger.info(end - start)

    logger.info("### Ingest lambda ended ###")

    # Invoke deduplicate Lambda
    logger.info("### Invoke Redshift deduplication Lambda ###")
    event = {
        "schemas": [get_schema_name(PROVIDER)],
        "tables": [],
        "do_delete_duplicates": True,
        "partition_order_by_field": "jab_id",
        "env": get_running_env(),
        "mode": "readwrite",
    }

    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName=os.environ["LAMBDA_DEDUPLICATOR_FUNCTION_ARN"],
        Payload=json.dumps(event),
        InvocationType="Event",
    )
    logger.info("### Invocation sent###")


if __name__ == "__main__":
    lambda_handler(None, None)
