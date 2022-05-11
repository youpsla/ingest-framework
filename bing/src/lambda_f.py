# TODO: manage logger for having logger output in terminal when running locally + cleanup print statements


import os
import sys
import time

# Import redshift here for being able to rollback()/commit() transaction.
from src.clients.redshift.redshift_client import RedshiftClient
from src.commons.task import Task
from src.utils.custom_logger import logger

sys.path.append(os.path.join(os.path.dirname(__file__)))

SOURCE_CHANNEL = "bing"

DAILY_TASKS_LIST = [
    # "daily_accounts_update",
    # "daily_campaigns_update",
    # "daily_adgroups_update",
    # "daily_ads_update",
    "daily_medias_update",
    # "daily_geo_metrics_update",
    # "daily_geo_metrics_update_s3_to_reshift",
    # "daily_demographic_metrics_update",
    # "daily_demographic_metrics_update_s3_to_reshift",
]

MONTHLY_TASKS_LIST = []


def get_running_env():
    running_env = os.environ.get("RUNNING_ENV")
    if not running_env:
        raise ValueError("RUNNING_ENV cannot be None.")
    return running_env


def lambda_handler(event, context):
    main(SOURCE_CHANNEL)


def run_task(channel, task_name, running_env, db_connection):
    """Runs a task

    Returns:
        result: str
        Can be "success" or "error" depending on the task run result.
        A dict with params
    """
    result, destination = Task(
        channel=channel,
        name=task_name,
        running_env=running_env,
        db_connection=db_connection,
    ).run()
    return result, destination


def main(channel):
    logger.info("### Starting Ingest lambda ###")
    start = time.time()

    running_env = get_running_env()

    # Daily tasks run
    logger.info(f"Daily tasks run: {DAILY_TASKS_LIST}")
    workflow_result = []

    db_connection = RedshiftClient().db_connection
    with db_connection.cursor() as cursor:
        cursor.execute("BEGIN;")
    for task_name in DAILY_TASKS_LIST:
        result, _ = run_task(channel, task_name, running_env, db_connection)
        workflow_result.append(result)
        # if result != "success":
        #     destination.rollback()
        #     logger.error(
        #         f"Error while running DAILY task{task_name}. All Db transactions have"
        #         " been rollbacked. No datas write to destination."
        #     )

    # Monthly tasks run
    # today = datetime.datetime.now()
    # if today.day == 1:
    #     logger.info(f"Monthly tasks run: {MONTHLY_TASKS_LIST}")
    #     for task_name in MONTHLY_TASKS_LIST:
    #         result = run_task(source, destination, task_name)
    #         if result != "success":
    #             destination.write_results_db_connection.rollback()
    #             logger.error(
    #                 f"Error while running MONTHLY task: {task_name}. All Db"
    #                 " transactions have been rollbacked. No datas write to"
    #                 " destination."
    #             )
    with db_connection.cursor() as cursor:
        cursor.execute("COMMIT;")
        # Transfer from tmp dir to s3
    logger.info(
        "All tasks have runned successfully. Daily Worflow ended with success."
    )

    end = time.time()
    logger.info(end - start)

    logger.info("### Ingest lambda ended ###")


if __name__ == "__main__":
    os.environ["RUNNING_ENV"] = "development"
    main(SOURCE_CHANNEL)
