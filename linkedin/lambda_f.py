import datetime
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__)))

from linkedin_client import LinkedInClient
from redshift_client import RedshiftClient
from task import Task


def lambda_handler(event, context):
    print("Enter Linkedin Ingest Lambda.")
    main()


def main():
    start = time.time()

    destination = RedshiftClient()
    source = LinkedInClient(destination=destination)

    ## Daily tasks tasks # noqa: E266
    Task("daily_accounts_update", source, destination).run()
    Task("daily_campaigns_update", source, destination).run()
    Task("daily_social_metrics_update", source, destination).run()
    Task("creative_sponsored_video_daily_update", source, destination).run()
    Task(
        "creative_sponsored_video__creative_name_daily_update", source, destination
    ).run()
    Task("account_pivot_campaign_daily_update", source, destination).run()
    Task("creative_url_daily_update", source, destination).run()
    Task("campaign_groups_daily_update", source, destination).run()

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
        Task("pivot_job_title_monthly_update", source, destination).run()
        Task("pivot_member_geo_monthly_update", source, destination).run()
        Task("pivot_member_organization_monthly_update", source, destination).run()

    ### Those task are not in Mark list # noqa: E266
    # Task("creative_text_ads_daily_update", source, destination).run()

    end = time.time()
    print(end - start)


if __name__ == "__main__":
    main()
