# TODO: Put all insert and update Db requests in a transaction for commiting at the end of completion of the group of tasks.# noqa E:501
# TODO: Create a list of task in lambda_f.py. This allow better error and logging messages (Add running task name).# noqa E:501

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


def main(event, context):
    print("Enter Linkedin Ingest Lambda.")
    main()


def main():
    start = time.time()

    destination = RedshiftClient()
    source = LinkedInClient(destination=destination)

    workflow_result_state = "Success"

    while workflow_result_state == "Success":
        ## Daily tasks tasks # noqa: E266
        workflow_result_state = Task("daily_accounts_update", source, destination).run()
        workflow_result_state = Task(
            "daily_campaigns_update", source, destination
        ).run()
        workflow_result_state = Task(
            "daily_social_metrics_update", source, destination
        ).run()
        workflow_result_state = Task(
            "creative_sponsored_video_daily_update", source, destination
        ).run()
        workflow_result_state = Task(
            "creative_sponsored_video__creative_name_daily_update", source, destination
        ).run()

        workflow_result_state = Task(
            "account_pivot_campaign_daily_update", source, destination
        ).run()
        workflow_result_state = Task(
            "creative_url_daily_update", source, destination
        ).run()
        workflow_result_state = Task(
            "campaign_groups_daily_update", source, destination
        ).run()

        # Query ignored because:
        # select max(start_date) from linkedin.pivot_creative; > 2021-05-24 00:00:00.000
        # Task("pivot_creative_daily_update", source, destination).run()

        ## Monthly tasks # noqa: E266
        today = datetime.datetime.now()
        if today.day == 1:
            print("It's the first of the month!")
            print(
                """We run monthly tasks:
            - pivot_job_title_monthly_update
            - pivot_member_geo_monthly_update
            - pivot_member_organization_monthly_update"""
            )
            workflow_result_state = Task(
                "pivot_job_title_monthly_update", source, destination
            ).run()
            workflow_result_state = Task(
                "pivot_member_geo_monthly_update", source, destination
            ).run()
            workflow_result_state = Task(
                "pivot_member_organization_monthly_update", source, destination
            ).run()

        ### Those task are not in Mark list # noqa: E266
        # Task("creative_text_ads_daily_update", source, destination).run()

    if workflow_result_state != "Succes":
        destination.write_results_db_connection.rollback()
        logger.error(
            "Current task finished abnormally. All transactions have been rollbacked."
            " No datas write to destination."
        )
    else:
        destination.write_results_db_connection.commit()
        logger.info("Worflow ended with success.")

    end = time.time()
    logger.info(end - start)


if __name__ == "__main__":
    main()
