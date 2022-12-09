import copy
import csv
import time
import uuid
from datetime import datetime

from bingads.v13.bulk import *
from bingads.v13.reporting.reporting_service_manager import \
    ReportingServiceManager

from src.clients.bing.auth_helper import (ServiceClient, authenticate,
                                          get_authorization_data,
                                          output_status_message)
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

        self.output_status_message = output_status_message
        self.task = task
        self.report_request = None
        self.model = None

    def get_kwargs(self, query_params):
        dynamics_params = self.get_dynamics_params(query_params)
        statics_params = self.get_statics_params(query_params)
        return dynamics_params + statics_params

    def get_data_from_file_path_list(self, result_file_path_list):
        result = []
        for f in result_file_path_list:
            with open(f, newline="", encoding="utf-8-sig") as csv_file:
                tmp = list(csv.DictReader(csv_file))
                tmp = [
                    i
                    for i in tmp
                    if i["Type"] not in ["Image", "Format Version", "Account"]
                ]
                result.extend(list(tmp))
        return result

    def get(self, task_params):

        db_params = self.get_request_params()
        print(f"Number of requests to run: {len(db_params)}")

        result = []

        if self.task.name in ["daily_ads_update", "daily_adgroups_update"]:
            final_result = []
            if db_params:
                original_db_params = {key: copy.deepcopy(
                    value) for key, value in db_params.items()}
                result_file_path_list = []
                for k, v in db_params.items():
                    self.authorization_data = get_authorization_data(
                        v["AccountId"])

                    bulk_service_manager = BulkServiceManager(
                        authorization_data=self.authorization_data,
                        poll_interval_in_milliseconds=5000,
                        environment="production",
                    )

                    authenticate(self.authorization_data)
                    # Download all campaigns, ad groups, and ads in the account.
                    task_entities = {
                        "daily_ads_update": ["Ads"],
                        "daily_adgroups_update": ["AdGroups"],
                    }
                    entities = task_entities[self.task.name]

                    # DownloadParameters is used for Option A below.

                    download_parameters = DownloadParameters(
                        # campaign_ids={"Int64": v["id"].split(",")},
                        campaign_ids=None,
                        data_scope=["EntityData"],
                        download_entities=entities,
                        file_type="Csv",
                        last_sync_time_in_utc=None,
                        result_file_directory=FILE_DIRECTORY,
                        result_file_name="ingest_" + str(uuid.uuid4()) + ".csv",
                        # Set this value true if you want to overwrite the same file.
                        overwrite_result_file=True,
                        # You may optionally cancel the download after a specified time interval.
                        timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS,
                    )

                    output_status_message(
                        "-----\nAwaiting Background Completion...")
                    result_file_path = bulk_service_manager.download_file(
                        download_parameters
                    )
                    result_file_path_list.append(result_file_path)
                    output_status_message(
                        "Download result file: {0}".format(result_file_path)
                    )

                # We aggregate all results in files into one list of dicts for insertion.
                result = self.get_data_from_file_path_list(
                    result_file_path_list
                )  # noqa501
                return result

        if self.task.name in [
            "daily_user_location_metrics_update",
            "daily_demographic_metrics_update",
            "daily_geographic_metrics_update",
            "daily_ad_metrics_update",
        ]:
            if db_params:
                original_db_params = {key: copy.deepcopy(
                    value) for key, value in db_params.items()}
                for k, v in db_params.items():
                    self.authorization_data = get_authorization_data()
                    authenticate(self.authorization_data)
                    request = ReportRequest(
                        authorization_data=self.authorization_data,
                        service_name=self.task.params["query"]["service_name"],
                        task=self.task,
                        kwargs=v,
                    ).get()
                    report_manager = ReportManager(
                        report_request=request,
                        authorization_data=self.authorization_data,
                    )
                    result_file_path = report_manager.submit_and_download()
                    # api_datas = {k: request.submit_and_download()}
                    return result_file_path
        if self.task.name == "daily_accounts_update":
            self.authorization_data = get_authorization_data()
            authenticate(self.authorization_data)
            request = ServiceRequest(
                authorization_data=self.authorization_data,
                service_name=self.task.params["query"]["service_name"],
                task=self.task,
            )
            final_result = request.get()
        else:
            final_result = []
            if db_params:
                original_db_params = {key: copy.deepcopy(
                    value) for key, value in db_params.items()}
                counter = 0
                counter_total = len(db_params)
                print(f"Number of requests to run: {counter_total}")
                result = []
                for k, v in db_params.items():
                    self.authorization_data = get_authorization_data(
                        v["AccountId"])
                    authenticate(self.authorization_data)
                    # Delete entries which cannot be passed to the service for the request. used for AccountId which has to be in authentication only.
                    if not self.task.name == "daily_campaigns_update":
                        del v["AccountId"]

                    authenticate(self.authorization_data)
                    request = ServiceRequest(
                        authorization_data=self.authorization_data,
                        service_name=self.task.params["query"]["service_name"],
                        task=self.task,
                        kwargs=v
                    )
                    api_datas = {k: request.get()}
                    result.append(api_datas)
                    counter += 1
                    if counter % 10 == 0:
                        print(f"Requests runned so far: {counter}", end=" ")
                    # Add data to the API response
                final_result = self.add_request_params_to_api_call_result(
                    result, task_params, original_db_params
                )

        return final_result


class ServiceRequest:
    def __init__(
        self,
        authorization_data=None,
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
                result, self.task.params["query"]["response_datas_key"])
            return result

        if self.task.name == "daily_campaigns_update":

            result = self.service.GetCampaignsByAccountId(**self.kwargs)

            result = recursive_asdict(result)
            result = nested_get(
                result, self.task.params["query"]["response_datas_key"])
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
                **self.kwargs, AdTypes=adTypes)
            result = recursive_asdict(result)
            result = nested_get(
                result, self.task.params["query"]["response_datas_key"])
            return result

        if self.task.name == "daily_medias_update":

            result = self.service.GetMediaMetaDataByAccountId(
                **self.kwargs,
            )

            result = recursive_asdict(result)
            result = nested_get(
                result, self.task.params["query"]["response_datas_key"])
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
        reporting_download_operation = self.reporting_service_manager.submit_download(
            self.report_request)

        # You may optionally cancel the track() operation after a specified time interval.
        # reporting_operation_status = self.reporting_download_operation.track(
        #     timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS
        # )

        # You can use ReportingDownloadOperation.track() to poll until complete as shown above,
        # or use custom polling logic with get_status() as shown below.
        for i in range(10):
            time.sleep(
                self.reporting_service_manager.poll_interval_in_milliseconds /
                1000.0)

            download_status = reporting_download_operation.get_status()

            if download_status.status == "Success":
                break

        result_file_path = reporting_download_operation.download_result_file(
            result_file_directory=FILE_DIRECTORY,
            # result_file_name="staging.csv",
            result_file_name=self.get_result_file_name(
                self.report_request.ReportName),
            decompress=True,
            # Set this value true if you want to overwrite the same file.
            overwrite=True,
            # You may optionally cancel the download after a specified time interval.
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS,
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
        report_request.ReportName = "user_location_performance_report"

        scope = self.service.factory.create("AccountThroughAdGroupReportScope")

        setattr(
            scope,
            "AccountIds",
            {"long": self.kwargs["id"].split(",")},
        )
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
        report_request.ReportName = "professional_demographics_audience_report"
        scope = self.service.factory.create("AccountThroughAdGroupReportScope")
        setattr(
            scope,
            "AccountIds",
            {"long": self.kwargs["id"].split(",")},
        )
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
        report_request.ReportName = "geographic_performance_report"
        scope = self.service.factory.create("AccountThroughAdGroupReportScope")
        setattr(
            scope,
            "AccountIds",
            {"long": self.kwargs["id"].split(",")},
        )
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

    def get_ad_performance_report_request(
        self,
    ):

        report_request = self.service.factory.create(
            "AdPerformanceReportRequest")
        report_request.Aggregation = self.aggregation
        report_request.ExcludeColumnHeaders = self.exclude_column_headers
        report_request.ExcludeReportFooter = self.exclude_report_footer
        report_request.ExcludeReportHeader = self.exclude_report_header
        report_request.Format = self.report_file_format
        report_request.ReturnOnlyCompleteData = self.return_only_complete_data
        report_request.Time = self.get_custom_date_range()
        report_request.ReportName = "ad_performance_report"
        scope = self.service.factory.create("AccountThroughAdGroupReportScope")
        setattr(
            scope,
            "AccountIds",
            {"long": self.kwargs["id"].split(",")},
        )
        report_request.Scope = scope

        report_columns = self.service.factory.create(
            "ArrayOfAdPerformanceReportColumn")
        report_columns.AdPerformanceReportColumn.append(
            [
                "TimePeriod",
                "AccountId",
                "CampaignId",
                "AdGroupId",
                "AdId",
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
        if self.task.name == "daily_ad_metrics_update":
            return self.get_ad_performance_report_request()

        raise ValueError("Unknown task name")

    def get_custom_date_range(self):

        date_range = self.kwargs
        time = self.service.factory.create("ReportTime")

        start_date = self.service.factory.create("Date")
        start_date.Day = date_range["dateRange_start_day"]
        start_date.Month = date_range["dateRange_start_month"]
        start_date.Year = date_range["dateRange_start_year"]

        end_date = self.service.factory.create("Date")
        end_date.Day = date_range["dateRange_end_day"]
        end_date.Month = date_range["dateRange_end_month"]
        end_date.Year = date_range["dateRange_end_year"]

        # You can either use a custom date range or predefined time.
        # time.PredefinedTime = "Yesterday"
        time.ReportTimeZone = "BrusselsCopenhagenMadridParis"
        time.CustomDateRangeStart = start_date
        time.CustomDateRangeEnd = end_date

        return time
