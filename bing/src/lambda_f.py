# TODO: Put all insert and update Db requests in a transaction for commiting at the end of completion of the group of tasks.# noqa E:501
# TODO: Create a list of task in lambda_f.py. This allow better error and logging messages (Add running task name).# noqa E:501
# TODO: manage logger for having logger output in terminal when running locally + cleanup print statements

import datetime
import logging
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__)))


from bing.src.bingads_client import BingAdsClient

from src.client import S3Client
from src.task import Task

logger = logging.getLogger(__name__)


# ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
# logger.addHandler(ch)


# logger.setLevel(logging.INFO)


if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


SOURCE_CHANNEL = "bing"

DAILY_TASKS_LIST = [
    "daily_accounts_update",
]


MONTHLY_TASKS_LIST = [
    # "daily_campaigns_update",
]


def get_running_env():
    running_env = os.environ.get("RUNNING_ENV")
    if not running_env:
        raise ValueError("RUNNING_ENV cannot be None.")
    return running_env


def lambda_handler(event, context):
    print("Enter Linkedin Ingest Lambda.")
    print(context)
    channel = "bing"
    main(channel)


def run_task(channel, source, destination, task_name):
    """Runs a task

    Returns:
        result: str
        Can be "success" or "error" depending on the task run result.
        A dict with params
    """
    result = Task(
        channel=channel, source=source, destination=destination, name=task_name
    ).run()
    return result


def main(channel):
    logger.info("### Starting Ingest lambda ###")
    start = time.time()

    running_env = get_running_env()

    destination = S3Client(running_env)
    source = BingAdsClient(destination=destination)

    # Daily tasks run
    logger.info(f"Daily tasks run: {DAILY_TASKS_LIST}")
    for task_name in DAILY_TASKS_LIST:
        result = run_task(channel, source, destination, task_name)
        if result != "success":
            destination.write_results_db_connection.rollback()
            logger.error(
                f"Error while running DAILY task{task_name}. All Db transactions have"
                " been rollbacked. No datas write to destination."
            )

    # Monthly tasks run
    today = datetime.datetime.now()
    if today.day == 1:
        logger.info(f"Monthly tasks run: {MONTHLY_TASKS_LIST}")
        for task_name in MONTHLY_TASKS_LIST:
            result = run_task(source, destination, task_name)
            if result != "success":
                destination.write_results_db_connection.rollback()
                logger.error(
                    f"Error while running MONTHLY task: {task_name}. All Db"
                    " transactions have been rollbacked. No datas write to"
                    " destination."
                )

    destination.write_results_db_connection.commit()
    destination.write_results_db_connection.close()
    logger.info("All tasks have runned successfully. Daily Worflow ended with success.")

    end = time.time()
    logger.info(end - start)

    logger.info("### Ingest lambda ended ###")


if __name__ == "__main__":
    channel = "bing"
    os.environ["RUNNING_ENV"] = "development"
    main(channel)
