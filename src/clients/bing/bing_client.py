import os
import time
from datetime import datetime

# Unusual import for preventing circular import.
# Import module instead of file.
import src.clients.s3 as s3
from bingads.v13.reporting.reporting_download_parameters import (
    ReportingDownloadParameters,
)
from bingads.v13.reporting.reporting_service_manager import (
    ReportingServiceManager,
)
from src.clients.bing.auth_helper import (
    DEVELOPER_TOKEN,
    AuthorizationData,
    ServiceClient,
    authenticate,
    output_status_message,
)
from src.commons.client import Client
from src.utils.various_utils import nested_get, recursive_asdict

# The report file extension type.
REPORT_FILE_FORMAT = "Csv"

# The directory for the report files.
FILE_DIRECTORY = "/tmp"

TIMEOUT_IN_MILLISECONDS = 3600000


class BingAdsClient(Client):
    def __init__(self, task=None, env=None):
        super().__init__(env)

        self.authorization_data = AuthorizationData(
            account_id=None,
            customer_id=None,
            developer_token=DEVELOPER_TOKEN,
            authentication=None,
        )

        authenticate(self.authorization_data)

        self.output_status_message = output_status_message
        self.task = task
        self.report_request = None
        self.model = None

    def get_kwargs(self, query_params):
        dynamics_params = self.get_dynamics_params(query_params)
        statics_params = self.get_statics_params(query_params)
        return dynamics_params + statics_params

    def get(self, **_ignored):

        params = self.task.params
        params = params.get("url", None)
        query_params = params.get("params", None)

        db_params = self.get_db_params(self.task)
        kwargs = self.get_kwargs(query_params)

        result = []
        if self.task.name in [
            "daily_user_location_metrics_update",
            "daily_demographic_metrics_update",
            "daily_geographic_metrics_update",
        ]:
            request = ReportRequest(
                authorization_data=self.authorization_data,
                service_name=self.task.params["url"]["service_name"],
                task=self.task,
                kwargs=kwargs,
            )
        else:
            request = ServiceRequest(
                authorization_data=self.authorization_data,
                service_name=self.task.params["url"]["service_name"],
                task=self.task,
                kwargs=kwargs,
            )
        if db_params:
            for param in db_params:
                request.param = param[0]
                tmp = request.get()
                if isinstance(self.task.destination, s3.s3_client.S3Client):
                    result.append(tmp)
                else:
                    if tmp:
                        for t in tmp:
                            for i in param[0]:
                                for k, v in i.items():
                                    t[k] = v
                            result.append(t)
        else:
            result = request.get()

        tmp = []
        for r in result:
            tmp.append(
                {"datas": r, "authorization_data": self.authorization_data}
            )

        return tmp


class ServiceRequest:
    def __init__(
        self,
        authorization_data=None,
        aggregation=None,
        service_name=None,
        task=None,
        param=None,
        kwargs=None,
    ):

        self.authorization_data = authorization_data
        self.task = task
        self.service_name = service_name
        self._service = None
        self.param = param
        self.kwargs = kwargs

    @property
    def service(self):
        if not self._service:
            self._service = ServiceClient(
                service=self.service_name,
                version=13,
                authorization_data=self.authorization_data,
                environment="production",
            )
        return self._service

    def get(self):
        if self.task.name == "daily_accounts_update":
            result = self.service.GetAccountsInfo()
            result = recursive_asdict(result)
            result = nested_get(
                result, self.task.params["url"]["response_data_key"]
            )
            return result

        if self.task.name == "daily_campaigns_update":

            result = self.service.GetCampaignsByAccountId(
                **self.kwargs[0],
                **self.param[0],
            )

            result = recursive_asdict(result)
            result = nested_get(
                result, self.task.params["url"]["response_data_key"]
            )
            return result

        if self.task.name == "daily_adgroups_update":

            # TODO: Generic way of managing params here
            result = self.service.GetAdGroupsByCampaignId(
                **self.param[0],
            )
            result = recursive_asdict(result)
            result = nested_get(
                result, self.task.params["url"]["response_data_key"]
            )
            return result

        if self.task.name == "daily_ads_update":

            adTypes = self.service.factory.create("ArrayOfAdType")
            adTypes.AdType.append("AppInstall")
            adTypes.AdType.append("DynamicSearch")
            adTypes.AdType.append("ExpandedText")
            adTypes.AdType.append("Image")
            adTypes.AdType.append("Product")
            adTypes.AdType.append("ResponsiveAd")
            adTypes.AdType.append("ResponsiveSearch")
            adTypes.AdType.append("Text")

            # TODO: Generic way of managing params here
            result = self.service.GetAdsByAdGroupId(
                **self.param[0], AdTypes=adTypes
            )
            result = recursive_asdict(result)
            result = nested_get(
                result, self.task.params["url"]["response_data_key"]
            )
            return result

        if self.task.name == "daily_medias_update":

            result = self.service.GetMediaMetaDataByAccountId(
                **self.kwargs[0],
            )

            result = recursive_asdict(result)
            result = nested_get(
                result, self.task.params["url"]["response_data_key"]
            )
            return result

        if self.task.name == "daily_media_associations_update":

            result = self.service.GetMediaAssociations(
                MediaEnabledEntities="ResponsiveAd ImageAdExtension",
                **{
                    list(self.param[0].keys())[0]: {
                        "long": [list(self.param[0].values())[0]]
                    }
                }
                # setattr(scope, list(p.keys())[0], {"long": [list(p.values())[0]]})
                # **self.param[0],
            )

            result = recursive_asdict(result)
            result = nested_get(
                result, self.task.params["url"]["response_data_key"]
            )
            return result

        raise ValueError("Unknown task name")

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


class ReportManager:
    def __init__(self, report_request=None, authorization_data=None):
        self.authorization_data = authorization_data
        self.reporting_service_manager = ReportingServiceManager(
            authorization_data=self.authorization_data,
            poll_interval_in_milliseconds=5000,
            environment="Production",
        )
        self.report_request = report_request

    def get_reporting_download_parameters(self):
        reporting_download_parameters = ReportingDownloadParameters(
            report_request=self.report_request,
            result_file_directory=FILE_DIRECTORY,
            result_file_name="staging.csv",
            overwrite_result_file=True,  # Set this value true if you want to overwrite the same file.
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS,  # You may optionally cancel the download after a specified time interval.
        )
        return reporting_download_parameters

    def background_completion(self, reporting_download_parameters):
        """You can submit a download request and the ReportingServiceManager will automatically
        return results. The ReportingServiceManager abstracts the details of checking for result file
        completion, and you don't have to write any code for results polling."""

        result_file_path = self.reporting_service_manager.download_file(
            reporting_download_parameters
        )
        return result_file_path

    def get_result_file_name(self, report_name):
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

    def submit_and_download(self):
        """Submit the download request and then use the ReportingDownloadOperation result to
        track status until the report is complete e.g. either using
        ReportingDownloadOperation.track() or ReportingDownloadOperation.get_status()."""
        reporting_download_operation = (
            self.reporting_service_manager.submit_download(self.report_request)
        )

        # You may optionally cancel the track() operation after a specified time interval.
        # reporting_operation_status = self.reporting_download_operation.track(
        #     timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS
        # )

        # You can use ReportingDownloadOperation.track() to poll until complete as shown above,
        # or use custom polling logic with get_status() as shown below.
        for i in range(10):
            time.sleep(
                self.reporting_service_manager.poll_interval_in_milliseconds
                / 1000.0
            )

            download_status = reporting_download_operation.get_status()

            if download_status.status == "Success":
                break

        result_file_path = reporting_download_operation.download_result_file(
            result_file_directory=FILE_DIRECTORY,
            result_file_name="staging.csv",
            decompress=True,
            overwrite=True,  # Set this value true if you want to overwrite the same file.
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS,  # You may optionally cancel the download after a specified time interval.
        )

        return result_file_path


class ReportRequest:
    def __init__(
        self,
        authorization_data=None,
        aggregation="Daily",
        service_name=None,
        task=None,
        param=None,
        kwargs=None,
    ):
        self._service = None
        self.service_name = service_name

        self.authorization_data = authorization_data
        self.aggregation = aggregation
        self.task = task
        self.service_name = service_name
        self._service = None
        self.param = param
        self.kwargs = kwargs

        self.exclude_column_headers = False
        self.exclude_report_header = True
        self.exclude_report_footer = True
        self.report_file_format = "Csv"
        self.return_only_complete_data = True

        self.time = None

    @property
    def service(self):
        if not self._service:
            self._service = ServiceClient(
                service=self.service_name,
                version=13,
                authorization_data=self.authorization_data,
                environment="production",
            )
        return self._service

    def get_user_location_performance_report_request(
        self,
    ):

        report_request = self.service.factory.create(
            "UserLocationPerformanceReportRequest"
        )
        report_request.Aggregation = self.aggregation
        report_request.ExcludeColumnHeaders = self.exclude_column_headers
        report_request.ExcludeReportFooter = self.exclude_report_footer
        report_request.ExcludeReportHeader = self.exclude_report_header
        report_request.Format = self.report_file_format
        report_request.ReturnOnlyCompleteData = self.return_only_complete_data
        report_request.Time = self.get_custom_date_range()
        report_request.ReportName = "My User Location Performance Report"

        scope = self.service.factory.create("AccountThroughAdGroupReportScope")

        for p in self.param:
            setattr(scope, list(p.keys())[0], {"long": [list(p.values())[0]]})
        scope.Campaigns = None
        scope.AdGroups = None
        report_request.Scope = scope

        report_columns = self.service.factory.create(
            "ArrayOfUserLocationPerformanceReportColumn"
        )
        report_columns.UserLocationPerformanceReportColumn.append(
            [
                "TimePeriod",
                "AccountId",
                "AccountName",
                "AdGroupId",
                "AdGroupName",
                "CampaignId",
                "Country",
                "County",
                "State",
                "City",
                "PostalCode",
                "MetroArea",
                "Impressions",
                "Clicks",
                "Spend",
            ]
        )
        report_request.Columns = report_columns

        return report_request

    def professional_demographics_audience_report_request(
        self,
    ):

        report_request = self.service.factory.create(
            "ProfessionalDemographicsAudienceReportRequest"
        )
        report_request.Aggregation = self.aggregation
        report_request.ExcludeColumnHeaders = self.exclude_column_headers
        report_request.ExcludeReportFooter = self.exclude_report_footer
        report_request.ExcludeReportHeader = self.exclude_report_header
        report_request.Format = self.report_file_format
        report_request.ReturnOnlyCompleteData = self.return_only_complete_data
        report_request.Time = self.get_custom_date_range()
        report_request.ReportName = "My User Location Performance Report"
        scope = self.service.factory.create("AccountThroughAdGroupReportScope")
        for p in self.param:
            setattr(scope, list(p.keys())[0], {"long": [list(p.values())[0]]})
        scope.Campaigns = None
        report_request.Scope = scope

        report_columns = self.service.factory.create(
            "ArrayOfProfessionalDemographicsAudienceReportColumn"
        )
        report_columns.ProfessionalDemographicsAudienceReportColumn.append(
            [
                "TimePeriod",
                "AccountId",
                "AccountName",
                "CampaignId",
                "AdGroupId",
                "AdGroupName",
                "CompanyName",
                "IndustryName",
                "JobFunctionName",
                "Impressions",
                "Clicks",
                "Spend",
            ]
        )
        report_request.Columns = report_columns

        return report_request

    def get_geographic_performance_report_request(
        self,
    ):

        report_request = self.service.factory.create(
            "GeographicPerformanceReportRequest"
        )
        report_request.Aggregation = self.aggregation
        report_request.ExcludeColumnHeaders = self.exclude_column_headers
        report_request.ExcludeReportFooter = self.exclude_report_footer
        report_request.ExcludeReportHeader = self.exclude_report_header
        report_request.Format = self.report_file_format
        report_request.ReturnOnlyCompleteData = self.return_only_complete_data
        report_request.Time = self.get_custom_date_range()
        report_request.ReportName = "My User Location Performance Report"
        scope = self.service.factory.create("AccountThroughAdGroupReportScope")
        for p in self.param:
            setattr(scope, list(p.keys())[0], {"long": [list(p.values())[0]]})
        # scope.Accounts = None
        report_request.Scope = scope

        report_columns = self.service.factory.create(
            "ArrayOfGeographicPerformanceReportColumn"
        )
        report_columns.GeographicPerformanceReportColumn.append(
            [
                "TimePeriod",
                "AccountId",
                "AccountName",
                "AdGroupId",
                "AdGroupName",
                "CampaignId",
                "Country",
                "State",
                "County",
                "City",
                "PostalCode",
                "MetroArea",
                "Impressions",
                "Clicks",
                "Spend",
            ]
        )
        report_request.Columns = report_columns

        return report_request

    def get(self):
        if self.task.name == "daily_user_location_metrics_update":
            return self.get_user_location_performance_report_request()
        if self.task.name == "daily_demographic_metrics_update":
            return self.professional_demographics_audience_report_request()
        if self.task.name == "daily_geographic_metrics_update":
            return self.get_geographic_performance_report_request()

        # if self.task.name == "daily_campaigns_update":

        #     result = self.service.GetCampaignsByAccountId(
        #         **self.kwargs[0],
        #         **self.param[0],
        #     )

        #     result = recursive_asdict(result)
        #     result = nested_get(result, self.task.params["url"]["response_data_key"])
        #     return result

        raise ValueError("Unknown task name")

    def get_custom_date_range(self):

        date_range = self.kwargs
        time = self.service.factory.create("ReportTime")

        start_date = self.service.factory.create("Date")
        start_date.Day = date_range[0]["dateRange.start.day"]
        start_date.Month = date_range[1]["dateRange.start.month"]
        start_date.Year = date_range[2]["dateRange.start.year"]
        time.CustomDateRangeStart = start_date

        end_date = self.service.factory.create("Date")
        end_date.Day = date_range[3]["dateRange.end.day"]
        end_date.Month = date_range[4]["dateRange.end.month"]
        end_date.Year = date_range[5]["dateRange.end.year"]
        time.CustomDateRangeEnd = end_date

        # You can either use a custom date range or predefined time.
        # time.PredefinedTime = "Yesterday"
        time.ReportTimeZone = "BrusselsCopenhagenMadridParis"
        time.CustomDateRangeStart = start_date
        time.CustomDateRangeEnd = end_date

        return time
