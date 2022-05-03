import logging
import time
from datetime import datetime

from bingads.v13.reporting import ReportingServiceManager

from src.client import Client

# from campaignmanagement_example_helper import output_array_of_campaign

MODE = "prod"

if MODE == "dev":
    from sandbox_auth_helper import (
        DEVELOPER_TOKEN,
        ENVIRONMENT,
        AuthorizationData,
        ServiceClient,
        authenticate,
        output_status_message,
    )
else:
    from bing.src.auth_helper import (
        DEVELOPER_TOKEN,
        ENVIRONMENT,
        AuthorizationData,
        ServiceClient,
        authenticate,
        output_status_message,
    )


# Optionally you can include logging to output traffic, for example the SOAP request and response.
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("suds.client").setLevel(logging.DEBUG)
logging.getLogger("suds.transport.http").setLevel(logging.DEBUG)

# You must provide credentials in auth_helper.py.

# Init logger
logger = logging.getLogger(__name__)

# The report file extension type.
REPORT_FILE_FORMAT = "Csv"

# Defineoutput type. If set to local, don't forget to set FILE_DIRECTORY to an existing local file.
# OUTPUT_TYPE = "S3"
OUTPUT_TYPE = "local"

# The S3 bucket name and path
S3_BUCKET = "s3://jabmo-eu-west-1-prod-ingest-datas"

S3_DIRECTORY = "bing"

# The directory for the report files.
FILE_DIRECTORY = "/Users/alain/Projects/tmp/"


# The maximum amount of time (in milliseconds) that you want to wait for the report download.
TIMEOUT_IN_MILLISECONDS = 3600000

REPORTS_NAME_LIST = [
    "campaign_performance_report",
    # "user_location_performance_report",
    # "professional_demographics_audience_report",
    # "account_performance_report_request",
    # "adgroup_performance_report_request",
    # "ad_performance_report_request",
    # "professional_demographic_audience_report_request",
]


def main(authorization_data):
    try:
        output_status_message("#### Starting Ingest bing ####")  # noqa :F405

        for report_name in REPORTS_NAME_LIST:
            output_status_message(f"Running {report_name} task")  # noqa :F405

            output_status_message(
                "-----\nAwaiting Submit and Download..."
            )  # noqa :F405
            report_request = get_report_request(
                authorization_data.account_id, report_name
            )
            submit_and_download(report_request, report_name)

    except WebFault as ex:
        output_webfault_errors(ex)
    except Exception as ex:
        output_status_message(ex)


class BingAdsClient(Client):
    def __init__(self, params=None, **kwargs):
        super().__init__(params)
        self.authorization_data = AuthorizationData(
            account_id=None,
            customer_id=None,
            developer_token=DEVELOPER_TOKEN,
            authentication=None,
        )

        self.reporting_service_manager = ReportingServiceManager(
            authorization_data=self.authorization_data,
            poll_interval_in_milliseconds=5000,
            environment=ENVIRONMENT,
        )

        authenticate(self.authorization_data)

        self.params = params
        self.model = None

    def get(self, model=None, **_ignored):
        # task_name = task_params.keys()[0]
        # report_request = get_report_request(authorization_data.account_id, task_name)
        # submit_and_download(report_request, task_name)

        zip_datas, kwargs = self.get_zip_datas(self.params["url"])
        print(zip_datas)
        print(kwargs)

        result = []
        return [{"datas": d} for d in result]

    def get_report_request(self):

        aggregation = "Daily"
        exclude_column_headers = False
        exclude_report_footer = True
        exclude_report_header = True
        time = reporting_service.factory.create("ReportTime")

        start_date = reporting_service.factory.create("Date")
        start_date.Day = 1
        start_date.Month = 1
        start_date.Year = 2022
        time.CustomDateRangeStart = start_date

        end_date = reporting_service.factory.create("Date")
        end_date.Day = 12
        end_date.Month = 4
        end_date.Year = 2022
        time.CustomDateRangeEnd = end_date

        # You can either use a custom date range or predefined time.
        # time.PredefinedTime = "Yesterday"
        time.ReportTimeZone = "BrusselsCopenhagenMadridParis"
        time.CustomDateRangeStart = start_date
        time.CustomDateRangeEnd = end_date
        return_only_complete_data = True

        if self.task_name == "user_location_performance_report":
            result_report = get_user_location_performance_report_request(
                account_id=self.account_id,
                aggregation=aggregation,
                exclude_column_headers=exclude_column_headers,
                exclude_report_footer=exclude_report_footer,
                exclude_report_header=exclude_report_header,
                report_file_format=REPORT_FILE_FORMAT,
                return_only_complete_data=return_only_complete_data,
                time=time,
            )

        if report_name == "professional_demographics_audience_report":
            result_report = get_professional_demographics_audience_report_request(
                account_id=account_id,
                aggregation=aggregation,
                exclude_column_headers=exclude_column_headers,
                exclude_report_footer=exclude_report_footer,
                exclude_report_header=exclude_report_header,
                report_file_format=REPORT_FILE_FORMAT,
                return_only_complete_data=return_only_complete_data,
                time=time,
            )

        if report_name == "campaign_performance_report":
            result_report = get_campaign_performance_report_request(
                account_id=account_id,
                aggregation=aggregation,
                exclude_column_headers=exclude_column_headers,
                exclude_report_footer=exclude_report_footer,
                exclude_report_header=exclude_report_header,
                report_file_format=REPORT_FILE_FORMAT,
                return_only_complete_data=return_only_complete_data,
                time=time,
            )

        if report_name == "account_performance_report_request":
            result_report = get_account_performance_report_request(
                account_id=account_id,
                aggregation=aggregation,
                exclude_column_headers=exclude_column_headers,
                exclude_report_footer=exclude_report_footer,
                exclude_report_header=exclude_report_header,
                report_file_format=REPORT_FILE_FORMAT,
                return_only_complete_data=return_only_complete_data,
                time=time,
            )

        if report_name == "adgroup_performance_report_request":
            result_report = get_adgroup_performance_report_request(
                account_id=account_id,
                aggregation=aggregation,
                exclude_column_headers=exclude_column_headers,
                exclude_report_footer=exclude_report_footer,
                exclude_report_header=exclude_report_header,
                report_file_format=REPORT_FILE_FORMAT,
                return_only_complete_data=return_only_complete_data,
                time=time,
            )

        if report_name == "ad_performance_report_request":
            result_report = get_ad_performance_report_request(
                account_id=account_id,
                aggregation=aggregation,
                exclude_column_headers=exclude_column_headers,
                exclude_report_footer=exclude_report_footer,
                exclude_report_header=exclude_report_header,
                report_file_format=REPORT_FILE_FORMAT,
                return_only_complete_data=return_only_complete_data,
                time=time,
            )

        if report_name == "professional_demographic_audience_report_request":
            result_report = get_professional_demographic_audience_report_request(
                account_id=account_id,
                aggregation=aggregation,
                exclude_column_headers=exclude_column_headers,
                exclude_report_footer=exclude_report_footer,
                exclude_report_header=exclude_report_header,
                report_file_format=REPORT_FILE_FORMAT,
                return_only_complete_data=return_only_complete_data,
                time=time,
            )

        return result_report


def submit_and_download(self, report_request, report_name):
    """Submit the download request and then use the ReportingDownloadOperation result to
    track status until the report is complete e.g. either using
    ReportingDownloadOperation.track() or ReportingDownloadOperation.get_status()."""

    # global reporting_service_manager
    reporting_download_operation = reporting_service_manager.submit_download(
        report_request
    )

    # You may optionally cancel the track() operation after a specified time interval.
    # reporting_operation_status = reporting_download_operation.track(
    #     timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS
    # )

    # You can use ReportingDownloadOperation.track() to poll until complete as shown above,
    # or use custom polling logic with get_status() as shown below.
    for i in range(10):
        time.sleep(reporting_service_manager.poll_interval_in_milliseconds / 1000.0)

        download_status = reporting_download_operation.get_status()

        if download_status.status == "Success":
            break

    result_file_path = reporting_download_operation.download_result_file(
        result_file_directory=FILE_DIRECTORY,
        result_file_name=get_result_file_name(report_name),
        decompress=True,
        overwrite=True,  # Set this value true if you want to overwrite the same file.
        timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS,  # You may optionally cancel the download after a specified time interval.
    )

    output_status_message("Download result file: {0}".format(result_file_path))


class ReportRequest:
    def __init__(self, authorization_data=None, aggregation="Daily"):
        self.authorization_data = authorization_data
        self.reporting_service = ServiceClient(
            service="ReportingService",
            version=13,
            authorization_data=authorization_data,
            environment=ENVIRONMENT,
        )
        self.aggregation = aggregation

        self.exclude_column_headers = False
        self.exclude_report_footer = True
        self.exclude_report_header = True

    def get_custom_date_range(self):
        time = self.reporting_service.factory.create("ReportTime")

        start_date = self.reporting_service.factory.create("Date")
        start_date.Day = 1
        start_date.Month = 1
        start_date.Year = 2022
        time.CustomDateRangeStart = start_date

        end_date = self.reporting_service.factory.create("Date")
        end_date.Day = 12
        end_date.Month = 4
        end_date.Year = 2022
        time.CustomDateRangeEnd = end_date

        return time


def get_result_file_name(report_name):
    """Generate the filename of the report

    Generation made using current day and task/report name.
    Atm, only csv output is managed.

    Args:
        report_name:
          The name of the report. Must be of str type.

    Returns:
        A str representing the filename with extension.

        "YYYY-MM-DD_<report_name>.csv

    Raises:
        TypeError: Type of report name is not str.
    """

    if not isinstance(report_name, str):
        raise TypeError(
            f"report_name must be of type str. Got {type(report_name)} instead"
        )

    dt = datetime.now().strftime("%Y-%m-%d")
    result_file_name = "bing_{}_{}.{}".format(
        dt, report_name, REPORT_FILE_FORMAT.lower()
    )
    return result_file_name


def get_report_request(account_id, report_name):
    """
    Use a sample report request or build your own.
    """

    aggregation = "Daily"
    exclude_column_headers = False
    exclude_report_footer = True
    exclude_report_header = True
    time = reporting_service.factory.create("ReportTime")

    start_date = reporting_service.factory.create("Date")
    start_date.Day = 1
    start_date.Month = 1
    start_date.Year = 2022
    time.CustomDateRangeStart = start_date

    end_date = reporting_service.factory.create("Date")
    end_date.Day = 12
    end_date.Month = 4
    end_date.Year = 2022
    time.CustomDateRangeEnd = end_date

    # You can either use a custom date range or predefined time.
    # time.PredefinedTime = "Yesterday"
    time.ReportTimeZone = "BrusselsCopenhagenMadridParis"
    time.CustomDateRangeStart = start_date
    time.CustomDateRangeEnd = end_date
    return_only_complete_data = True

    if report_name == "user_location_performance_report":
        result_report = get_user_location_performance_report_request(
            account_id=account_id,
            aggregation=aggregation,
            exclude_column_headers=exclude_column_headers,
            exclude_report_footer=exclude_report_footer,
            exclude_report_header=exclude_report_header,
            report_file_format=REPORT_FILE_FORMAT,
            return_only_complete_data=return_only_complete_data,
            time=time,
        )

    if report_name == "professional_demographics_audience_report":
        result_report = get_professional_demographics_audience_report_request(
            account_id=account_id,
            aggregation=aggregation,
            exclude_column_headers=exclude_column_headers,
            exclude_report_footer=exclude_report_footer,
            exclude_report_header=exclude_report_header,
            report_file_format=REPORT_FILE_FORMAT,
            return_only_complete_data=return_only_complete_data,
            time=time,
        )

    if report_name == "campaign_performance_report":
        result_report = get_campaign_performance_report_request(
            account_id=account_id,
            aggregation=aggregation,
            exclude_column_headers=exclude_column_headers,
            exclude_report_footer=exclude_report_footer,
            exclude_report_header=exclude_report_header,
            report_file_format=REPORT_FILE_FORMAT,
            return_only_complete_data=return_only_complete_data,
            time=time,
        )

    if report_name == "account_performance_report_request":
        result_report = get_account_performance_report_request(
            account_id=account_id,
            aggregation=aggregation,
            exclude_column_headers=exclude_column_headers,
            exclude_report_footer=exclude_report_footer,
            exclude_report_header=exclude_report_header,
            report_file_format=REPORT_FILE_FORMAT,
            return_only_complete_data=return_only_complete_data,
            time=time,
        )

    if report_name == "adgroup_performance_report_request":
        result_report = get_adgroup_performance_report_request(
            account_id=account_id,
            aggregation=aggregation,
            exclude_column_headers=exclude_column_headers,
            exclude_report_footer=exclude_report_footer,
            exclude_report_header=exclude_report_header,
            report_file_format=REPORT_FILE_FORMAT,
            return_only_complete_data=return_only_complete_data,
            time=time,
        )

    if report_name == "ad_performance_report_request":
        result_report = get_ad_performance_report_request(
            account_id=account_id,
            aggregation=aggregation,
            exclude_column_headers=exclude_column_headers,
            exclude_report_footer=exclude_report_footer,
            exclude_report_header=exclude_report_header,
            report_file_format=REPORT_FILE_FORMAT,
            return_only_complete_data=return_only_complete_data,
            time=time,
        )

    if report_name == "professional_demographic_audience_report_request":
        result_report = get_professional_demographic_audience_report_request(
            account_id=account_id,
            aggregation=aggregation,
            exclude_column_headers=exclude_column_headers,
            exclude_report_footer=exclude_report_footer,
            exclude_report_header=exclude_report_header,
            report_file_format=REPORT_FILE_FORMAT,
            return_only_complete_data=return_only_complete_data,
            time=time,
        )

    return result_report


def get_campaign_performance_report_request(
    account_id,
    aggregation,
    exclude_column_headers,
    exclude_report_footer,
    exclude_report_header,
    report_file_format,
    return_only_complete_data,
    time,
    campaign_id=435766866,
    # campaign_id=None,
):

    report_request = reporting_service.factory.create(
        "CampaignPerformanceReportRequest"
    )
    report_request.Aggregation = aggregation
    report_request.ExcludeColumnHeaders = exclude_column_headers
    report_request.ExcludeReportFooter = exclude_report_footer
    report_request.ExcludeReportHeader = exclude_report_header
    report_request.Format = report_file_format
    report_request.ReturnOnlyCompleteData = return_only_complete_data
    report_request.Time = time
    report_request.ReportName = "My Campaign Performance Report"
    scope = reporting_service.factory.create("AccountThroughCampaignReportScope")

    if campaign_id:
        campaigns = reporting_service.factory.create("ArrayOfCampaignReportScope")
        campaign_report_scope = reporting_service.factory.create("CampaignReportScope")
        campaign_report_scope.AccountId = account_id
        campaign_report_scope.CampaignId = campaign_id
        campaigns.CampaignReportScope.append(campaign_report_scope)
        scope.Campaigns = campaigns
    else:
        pass

    # scope.AccountIds = {"long": [account_id]}

    report_request.Scope = scope

    report_columns = reporting_service.factory.create(
        "ArrayOfCampaignPerformanceReportColumn"
    )
    report_columns.CampaignPerformanceReportColumn.append(
        [
            # "TimePeriod",
            "AccountId",
            "CampaignId",
            "CampaignName",
            "CampaignType",
            "CampaignStatus",
            "Impressions",
            "Clicks",
            "Ctr",
            "Spend",
        ]
    )
    report_request.Columns = report_columns

    return report_request


def get_account_performance_report_request(
    account_id,
    aggregation,
    exclude_column_headers,
    exclude_report_footer,
    exclude_report_header,
    report_file_format,
    return_only_complete_data,
    time,
):

    report_request = reporting_service.factory.create("AccountPerformanceReportRequest")
    report_request.Aggregation = aggregation
    report_request.ExcludeColumnHeaders = exclude_column_headers
    report_request.ExcludeReportFooter = exclude_report_footer
    report_request.ExcludeReportHeader = exclude_report_header
    report_request.Format = report_file_format
    report_request.ReturnOnlyCompleteData = return_only_complete_data
    report_request.Time = time
    report_request.ReportName = "Accounts Performance Report"

    scope = reporting_service.factory.create("AccountReportScope")
    scope.AccountIds = {"long": [account_id]}

    report_request.Scope = scope

    report_columns = reporting_service.factory.create(
        "ArrayOfAccountPerformanceReportColumn"
    )
    report_columns.AccountPerformanceReportColumn.append(
        [
            "AccountName",
            "AccountNumber",
            "AccountId",
            "AccountStatus",
            "Impressions",
            "Clicks",
            "Ctr",
            "AverageCpc",
            "Spend",
            "TimePeriod",
        ]
    )
    report_request.Columns = report_columns

    return report_request


def get_user_location_performance_report_request(
    account_id,
    aggregation,
    exclude_column_headers,
    exclude_report_footer,
    exclude_report_header,
    report_file_format,
    return_only_complete_data,
    time,
):

    report_request = reporting_service.factory.create(
        "UserLocationPerformanceReportRequest"
    )
    report_request.Aggregation = aggregation
    report_request.ExcludeColumnHeaders = exclude_column_headers
    report_request.ExcludeReportFooter = exclude_report_footer
    report_request.ExcludeReportHeader = exclude_report_header
    report_request.Format = report_file_format
    report_request.ReturnOnlyCompleteData = return_only_complete_data
    report_request.Time = time
    report_request.ReportName = "My User Location Performance Report"
    scope = reporting_service.factory.create("AccountThroughAdGroupReportScope")
    scope.AccountIds = {"long": [account_id]}
    scope.Campaigns = None
    scope.AdGroups = None
    report_request.Scope = scope

    report_columns = reporting_service.factory.create(
        "ArrayOfUserLocationPerformanceReportColumn"
    )
    report_columns.UserLocationPerformanceReportColumn.append(
        [
            # "TimePeriod",
            "AccountId",
            "AccountName",
            "AdGroupId",
            "AverageCpc",
            "CampaignId",
            "LocationId",
            "State",
            "City",
            "PostalCode",
            "MetroArea",
            "Country",
            "County",
            "Clicks",
            "Ctr",
            "Impressions",
            "Spend",
        ]
    )
    report_request.Columns = report_columns

    return report_request


def get_professional_demographics_audience_report_request(
    account_id,
    aggregation,
    exclude_column_headers,
    exclude_report_footer,
    exclude_report_header,
    report_file_format,
    return_only_complete_data,
    time,
    campaign_id=435766866,
    # campaign_id=None,
):

    report_request = reporting_service.factory.create(
        "ProfessionalDemographicsAudienceReportRequest"
    )
    report_request.Aggregation = aggregation
    report_request.ExcludeColumnHeaders = exclude_column_headers
    report_request.ExcludeReportFooter = exclude_report_footer
    report_request.ExcludeReportHeader = exclude_report_header
    report_request.Format = report_file_format
    report_request.ReturnOnlyCompleteData = return_only_complete_data
    report_request.Time = time
    report_request.ReportName = "Demographics Audience Report"
    scope = reporting_service.factory.create("AccountThroughCampaignReportScope")

    if campaign_id:
        campaigns = reporting_service.factory.create("ArrayOfCampaignReportScope")
        campaign_report_scope = reporting_service.factory.create("CampaignReportScope")
        campaign_report_scope.AccountId = account_id
        campaign_report_scope.CampaignId = campaign_id
        campaigns.CampaignReportScope.append(campaign_report_scope)
        scope.Campaigns = campaigns
    else:
        scope.AccountIds = {"long": [account_id]}

    report_request.Scope = scope

    report_columns = reporting_service.factory.create(
        "ArrayOfProfessionalDemographicsAudienceReportColumn"
    )
    report_columns.AccountPerformanceReportColumn.append(
        [
            "AccountName",
            "AccountNumber",
            "AccountId",
            "AccountStatus",
            "Impressions",
            "Clicks",
            "Ctr",
            "AverageCpc",
            "Spend",
            "TimePeriod",
        ]
    )
    report_request.Columns = report_columns

    return report_request


def get_adgroup_performance_report_request(
    account_id,
    aggregation,
    exclude_column_headers,
    exclude_report_footer,
    exclude_report_header,
    report_file_format,
    return_only_complete_data,
    time,
):

    report_request = reporting_service.factory.create("AdGroupPerformanceReportRequest")
    report_request.Aggregation = aggregation
    report_request.ExcludeColumnHeaders = exclude_column_headers
    report_request.ExcludeReportFooter = exclude_report_footer
    report_request.ExcludeReportHeader = exclude_report_header
    report_request.Format = report_file_format
    report_request.ReturnOnlyCompleteData = return_only_complete_data
    report_request.Time = time
    report_request.ReportName = "Ad groups Performance Report"

    scope = reporting_service.factory.create("AccountThroughAdGroupReportScope")

    campaign_id = 435766866
    campaign_id = None
    if campaign_id:
        campaigns = reporting_service.factory.create("ArrayOfCampaignReportScope")
        campaign_report_scope = reporting_service.factory.create("CampaignReportScope")
        campaign_report_scope.AccountId = account_id
        campaign_report_scope.CampaignId = campaign_id
        campaigns.CampaignReportScope.append(campaign_report_scope)
        scope.Campaigns = campaigns
    else:
        scope.AccountIds = {"long": [account_id]}

    report_request.Scope = scope

    report_columns = reporting_service.factory.create(
        "ArrayOfAdGroupPerformanceReportColumn"
    )
    report_columns.AdGroupPerformanceReportColumn.append(
        [
            "TimePeriod",
            "AccountId",
            "CampaignId",
            "AdGroupId",
            "AdGroupName",
            "Status",
            "Impressions",
            "Clicks",
            "Ctr",
            "Spend",
        ]
    )
    report_request.Columns = report_columns

    return report_request


def get_ad_performance_report_request(
    account_id,
    aggregation,
    exclude_column_headers,
    exclude_report_footer,
    exclude_report_header,
    report_file_format,
    return_only_complete_data,
    time,
):

    report_request = reporting_service.factory.create("AdPerformanceReportRequest")
    report_request.Aggregation = aggregation
    report_request.ExcludeColumnHeaders = exclude_column_headers
    report_request.ExcludeReportFooter = exclude_report_footer
    report_request.ExcludeReportHeader = exclude_report_header
    report_request.Format = report_file_format
    report_request.ReturnOnlyCompleteData = return_only_complete_data
    report_request.Time = time
    report_request.ReportName = "Ad Performance Report"

    scope = reporting_service.factory.create("AccountThroughAdGroupReportScope")

    # campaign_id = 435766866
    campaign_id = None
    if campaign_id:
        campaigns = reporting_service.factory.create("ArrayOfCampaignReportScope")
        campaign_report_scope = reporting_service.factory.create("CampaignReportScope")
        campaign_report_scope.AccountId = account_id
        campaign_report_scope.CampaignId = campaign_id
        campaigns.CampaignReportScope.append(campaign_report_scope)
        scope.Campaigns = campaigns
    else:
        scope.AccountIds = {"long": [account_id]}

    report_request.Scope = scope

    report_columns = reporting_service.factory.create(
        "ArrayOfAdPerformanceReportColumn"
    )
    report_columns.AdPerformanceReportColumn.append(
        [
            "AccountId",
            "CampaignId",
            "AdGroupId",
            "AdId",
            "AdTitle",
            "AdType",
            "AdStatus",
            "Impressions",
            "Clicks",
            "Ctr",
            "Spend",
            # "TimePeriod",
            "DisplayUrl",
        ]
    )
    report_request.Columns = report_columns

    return report_request


def get_professional_demographic_audience_report_request(
    account_id,
    aggregation,
    exclude_column_headers,
    exclude_report_footer,
    exclude_report_header,
    report_file_format,
    return_only_complete_data,
    time,
):

    report_request = reporting_service.factory.create(
        "ProfessionalDemographicsAudienceReportRequest"
    )
    report_request.Aggregation = aggregation
    report_request.ExcludeColumnHeaders = exclude_column_headers
    report_request.ExcludeReportFooter = exclude_report_footer
    report_request.ExcludeReportHeader = exclude_report_header
    report_request.Format = report_file_format
    report_request.ReturnOnlyCompleteData = return_only_complete_data
    report_request.Time = time
    report_request.ReportName = "Professional Demographic Audience Report"

    scope = reporting_service.factory.create("AccountThroughAdGroupReportScope")

    # campaign_id = 435766866
    campaign_id = None
    if campaign_id:
        campaigns = reporting_service.factory.create("ArrayOfCampaignReportScope")
        campaign_report_scope = reporting_service.factory.create("CampaignReportScope")
        campaign_report_scope.AccountId = account_id
        campaign_report_scope.CampaignId = campaign_id
        campaigns.CampaignReportScope.append(campaign_report_scope)
        scope.Campaigns = campaigns
    else:
        scope.AccountIds = {"long": [account_id]}

    report_request.Scope = scope

    report_columns = reporting_service.factory.create(
        "ArrayOfProfessionalDemographicsAudienceReportColumn"
    )
    report_columns.ProfessionalDemographicsAudienceReportColumn.append(
        [
            "AccountId",
            "AccountName",
            "AdGroupName",
            "CampaignId",
            "AdGroupId",
            "CompanyName",
            "IndustryName",
            "JobFunctionName",
            "Impressions",
            "Clicks",
            "Spend",
            "TimePeriod",
        ]
    )
    report_request.Columns = report_columns

    return report_request


# Main execution
if __name__ == "__main__":

    print("Loading the web service client proxies...")

    authorization_data = AuthorizationData(
        account_id=None,
        customer_id=None,
        developer_token=DEVELOPER_TOKEN,
        authentication=None,
    )

    reporting_service_manager = ReportingServiceManager(
        authorization_data=authorization_data,
        poll_interval_in_milliseconds=5000,
        environment=ENVIRONMENT,
    )

    # In addition to ReportingServiceManager, you will need a reporting ServiceClient
    # to build the ReportRequest.

    reporting_service = ServiceClient(
        service="ReportingService",
        version=13,
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
    )

    campaign_service = ServiceClient(
        service="CampaignManagementService",
        version=13,
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
    )

    authenticate(authorization_data)

    main(authorization_data)
