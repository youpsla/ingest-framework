# TODO: Put all insert and update Db requests in a transaction for commiting at the end of completion of the group of tasks.# noqa E:501
# TODO: Create a list of task in lambda_f.py. This allow better error and logging messages (Add running task name).# noqa E:501
# TODO: manage logger for having logger output in terminal when running locally + cleanup print statements

import datetime
import logging
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__)))

from linkedin_client import LinkedInClient
from redshift_client import RedshiftClient
from task import Task

logger = logging.getLogger(__name__)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)


logger.setLevel(logging.INFO)

DAILY_TASKS_LIST = [
    "daily_accounts_update",
    "daily_campaigns_update",
    "daily_social_metrics_update",
    "creative_sponsored_video_daily_update",
    "creative_sponsored_video__creative_name_daily_update",
    "account_pivot_campaign_daily_update",
    "creative_url_daily_update",
    "campaign_groups_daily_update",
]

# Query ignored because:
# select max(start_date) from linkedin.pivot_creative; > 2021-05-24 00:00:00.000
# Task("pivot_creative_daily_update", source, destination).run()

MONTHLY_TASKS_LIST = [
    "daily_campaigns_update",
    "daily_campaigns_update",
    "daily_social_metrics_update",
    "creative_sponsored_video_daily_update",
    "creative_sponsored_video__creative_name_daily_update",
    "account_pivot_campaign_daily_update",
    "creative_url_daily_update",
    "campaign_groups_daily_update",
]


def lambda_handler(event, context):
    print("Enter Linkedin Ingest Lambda.")
    main()


def run_task(source, destination, task_name):
    """Runs a task

    Returns:
        result: str
        Can be "success" or "error" depending on the task run result.
        A dict with params
    """
    result = Task(task_name, source, destination).run()
    return result


def main():
    logger.info("### Starting Ingest lambda ###")
    start = time.time()

    destination = RedshiftClient()
    source = LinkedInClient(destination=destination)

    # Daily tasks run
    logger.info(f"Daily tasks run: {DAILY_TASKS_LIST}")
    for task_name in DAILY_TASKS_LIST:
        result = run_task(source, destination, task_name)
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
    main()
