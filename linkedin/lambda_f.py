# TODO: manage logger for having logger output in terminal when running locally + cleanup print statements


import datetime
import json
import os
import time

import sentry_sdk

# Temporary solution. This import allow init of some envs variables
# TODO: Envs management needs better system.
from configs.globals import CHANNEL
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

# Import redshift here for being able to rollback()/commit() transaction.
from src.clients.redshift.redshift_client import RedshiftClient
from src.commons.task import Task
from src.utils.custom_logger import logger

sentry_sdk.init(
    dsn=os.environ["SENTRY_DNS"],
    integrations=[AwsLambdaIntegration(timeout_warning=True)],
    traces_sample_rate=1.0,
)


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

    channel_params = get_channel_params()

    # Daily tasks run
    logger.info(f"Daily tasks run: {channel_params['daily_tasks_list']}")

    db_connection = RedshiftClient().db_connection
    with db_connection.cursor() as cursor:
        cursor.execute("BEGIN;")

    for task_name in channel_params["daily_tasks_list"]:
        result, _ = run_task(channel_params["name"], task_name, db_connection)

    today = datetime.datetime.now()
    run_monthly = True if today.day == 1 else False
    if run_monthly:
        monthly_tasks_list = channel_params.get("monthly_tasks_list", None)
        if monthly_tasks_list:
            for task_name in monthly_tasks_list:
                result, _ = run_task(channel_params["name"], task_name, db_connection)

    with db_connection.cursor() as cursor:
        cursor.execute("COMMIT;")
        # Transfer from tmp dir to s3
    logger.info("All tasks have runned successfully. Daily Worflow ended with success.")

    end = time.time()
    logger.info(end - start)

    logger.info("### Ingest lambda ended ###")


if __name__ == "__main__":
    main()
