# TODO: manage logger for having logger output in terminal when running locally + cleanup print statements # noqa: E501


# TODO: For migrating production
# Update medias table (Column account_id added.)


import json
import logging
import os
import time

import boto3
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

from configs.globals import PROVIDER

# Import redshift here for being able to rollback()/commit() transaction.
from src.clients.redshift.redshift_client import RedshiftClient
from src.commons.task import Task
from src.utils.custom_logger import logger
from src.utils.various_utils import get_running_env, get_schema_name

# logging.basicConfig(level=logging.INFO)
# logging.getLogger("suds.client").setLevel(logging.DEBUG)
# logging.getLogger("suds.transport.http").setLevel(logging.DEBUG)

sentry_sdk.init(
    dsn=os.environ["SENTRY_DSN"],
    integrations=[AwsLambdaIntegration(timeout_warning=True)],
    traces_sample_rate=1.0,
)


def get_params_json_file_path():
    app_home = os.environ["APPLICATION_HOME"]
    return os.path.realpath(
        os.path.join(app_home, "configs", PROVIDER, "channel.json")
    )  # noqa: E501


def get_channel_params():
    with open(get_params_json_file_path(), "r") as f:
        f = json.load(f)
    return f


def get_task_group_name():
    task_group_name = os.environ.get("TASK_GROUP")
    if not task_group_name:
        raise Exception("Can't find task group.")
    # return "daily_tasks_list"
    return task_group_name


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

    channel_params = get_channel_params()

    task_group_list = channel_params[get_task_group_name()]

    # Daily tasks run
    logger.info(f"{PROVIDER} - {task_group_list}")

    db_connection = RedshiftClient().db_connection
    with db_connection.cursor() as cursor:
        cursor.execute("BEGIN;")

    for task_name in task_group_list:
        result, _ = run_task(channel_params["name"], task_name, db_connection)

    with db_connection.cursor() as cursor:
        cursor.execute("COMMIT;")
        # Transfer from tmp dir to s3
    logger.info(
        "All tasks have runned successfully. Daily Worflow ended with success."
    )  # noqa: E501

    end = time.time()
    logger.info(end - start)

    logger.info("### Ingest lambda ended ###")

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
    main()
