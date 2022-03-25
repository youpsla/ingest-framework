import logging
from datetime import datetime

from bingads.v13.reporting import *

from auth_helper import *

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
    # "campaign_performance_report",
    # "user_location_performance_report",
    "professional_demographics_audience_report",
]


def main(authorization_data):
    # try:
    output_status_message("#### Starting Ingest bing ####")

    for report_name in REPORTS_NAME_LIST:
        output_status_message(f"Running {report_name} task")

        output_status_message("-----\nAwaiting Submit and Download...")
        report_request = get_report_request(authorization_data.account_id, report_name)
        submit_and_download(report_request, report_name)

    # except WebFault as ex:
    #     output_webfault_errors(ex)
    # except Exception as ex:
    #     output_status_message(ex)


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


def submit_and_download(report_request, report_name):
    """Submit the download request and then use the ReportingDownloadOperation result to
    track status until the report is complete e.g. either using
    ReportingDownloadOperation.track() or ReportingDownloadOperation.get_status()."""

    global reporting_service_manager
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


def get_report_request(account_id, report_name):
    """
    Use a sample report request or build your own.
    """

    aggregation = "Monthly"
    exclude_column_headers = False
    exclude_report_footer = False
    exclude_report_header = False
    time = reporting_service.factory.create("ReportTime")

    start_date = reporting_service.factory.create("Date")
    start_date.Day = 1
    start_date.Month = 3
    start_date.Year = 2022
    time.CustomDateRangeStart = start_date

    end_date = reporting_service.factory.create("Date")
    end_date.Day = 31
    end_date.Month = 3
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

    return result_report

    campaign_performance_report_request = get_campaign_performance_report_request(
        account_id=account_id,
        aggregation=aggregation,
        exclude_column_headers=exclude_column_headers,
        exclude_report_footer=exclude_report_footer,
        exclude_report_header=exclude_report_header,
        report_file_format=REPORT_FILE_FORMAT,
        return_only_complete_data=return_only_complete_data,
        time=time,
    )

    user_location_performance_report_request = (
        get_user_location_performance_report_request(
            account_id=account_id,
            aggregation=aggregation,
            exclude_column_headers=exclude_column_headers,
            exclude_report_footer=exclude_report_footer,
            exclude_report_header=exclude_report_header,
            report_file_format=REPORT_FILE_FORMAT,
            return_only_complete_data=return_only_complete_data,
            time=time,
        )
    )

    professional_demographics_audience_report_request = ()

    return user_location_performance_report_request


def get_campaign_performance_report_request(
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
    scope.AccountIds = {"long": [account_id]}
    scope.Campaigns = None
    report_request.Scope = scope

    report_columns = reporting_service.factory.create(
        "ArrayOfCampaignPerformanceReportColumn"
    )
    report_columns.CampaignPerformanceReportColumn.append(
        [
            "TimePeriod",
            "CampaignId",
            "CampaignName",
            "DeviceType",
            "Network",
            "Impressions",
            "Clicks",
            "Spend",
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
            "TimePeriod",
            "AccountId",
            "AccountName",
            "AdGroupId",
            "AverageCpc",
            "CampaignId",
            "City",
            "Clicks",
            "Country",
            "County",
            "Ctr",
            "DeviceType",
            "Impressions",
            "LocationId",
            "MetroArea",
            "Network",
            "PostalCode",
            "Spend",
            "State",
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
    scope = reporting_service.factory.create("AccountThroughAdGroupReportScope")
    scope.AccountIds = {"long": [account_id]}
    scope.Campaigns = None
    report_request.Scope = scope

    report_columns = reporting_service.factory.create(
        "ArrayOfProfessionalDemographicsAudienceReportColumn"
    )
    report_columns.ProfessionalDemographicsAudienceReportColumn.append(
        [
            "AccountName",
            "AdGroupName",
            "CampaignId",
            "CompanyName",
            "Impressions",
            "IndustryName",
            "JobFunctionName",
            "TimePeriod",
            "Spend",
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

    authenticate(authorization_data)

    main(authorization_data)
