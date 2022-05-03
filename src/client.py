import logging
import os
import time
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import List, Tuple

import boto3
from bing.src.auth_helper import ServiceClient
from botocore.exceptions import ClientError

from src.constants import ENVS_LIST
from src.model import Model
from src.s3wrappers import BucketWrapper, ObjectWrapper
from src.utils.various_utils import nested_get, recursive_asdict


class Client:
    def __init__(self, env):
        if self.check_env(env):
            self.env = env

    @staticmethod
    def check_env(env):
        if env not in ENVS_LIST:
            raise AttributeError(
                "Argument 'env' has to be:\n{}\nGot '{}' instead".format(
                    "\n".join(["- " + i for i in ENVS_LIST]), env
                )
            )
        else:
            return True

    def get_dynamics_param(self, name, params, value):
        if params["value_type"] == "date":
            return {
                name: params["url_query_parameter_value"].format(
                    day=value.day,
                    month=value.month,
                    year=value.year,
                ),
            }
        # if params["value_type"] == "db":
        #     return (name, params["url_query_parameter_value"].format(value))

        return None

    def get_dynamics_group_params(self, params):
        url_params = params["url_params"]
        today = datetime.today()
        result = []
        if params["offset_unity"] == "days":
            tmp = {params["offset_unity"]: int(params["offset_value"])}
            start_date = today - timedelta(**tmp)
            # end_date = today - timedelta(**tmp)
            for k, v in url_params.items():
                result.append(self.get_dynamics_param(k, v, start_date))

        if params["offset_unity"] == "months":
            last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
            start_day_of_prev_month = date.today().replace(day=1) - timedelta(
                days=last_day_of_prev_month.day
            )
            # tmp = {params["offset_unity"]: int(params["offset_value"])}
            # if params["offset_range_position"] == "start_day":

            # if params["offset_range_position"] == "start_day":

            # start_date = today - timedelta(**tmp)
            # # end_date = today - timedelta(**tmp)
            for k, v in url_params.items():
                result.append(
                    self.get_dynamics_param(
                        k,
                        v,
                        start_day_of_prev_month
                        if params["offset_range_position"] == "start_day"
                        else last_day_of_prev_month,
                    )
                )

        return result

    def get_kwargs_list(self, kwargs_fields=[], sql_datas=[], urlencode=False):
        result = []
        for d in sql_datas:
            tmp_result = []
            for f in kwargs_fields:
                r = {f[2]: f[1].format(d[f[0]])}
                tmp_result.append(r)
            result.append(tmp_result)

        return result

    def get_args_list(self, args_fields=[], sql_datas=[], urlencode=False):
        result = []
        if not args_fields:
            return args_fields
        for d in sql_datas:
            tmp_result = []
            for f in args_fields:
                tmp_result.append(d[f])
            result.append(tmp_result)

        return result

    def get_sql_list(self, db_fields=[], sql_datas=[]):
        result = []
        if not db_fields:
            return db_fields
        for d in sql_datas:
            tmp_result = []
            for f in db_fields:
                tmp_result.append({f[1]: d[f[0]]})
            result.append(tmp_result)

        return result

    def get_filter_values_from_db(self, destination=None, params=None, channel=None):
        if not params:
            return [], [], []

        for v in params.values():
            if "rawsql" in v["type"]:
                tmp = Model.get_from_raw_sql(destination, v["raw_sql"])
            else:
                model = Model(v["filter_model"], db_engine=destination, channel=channel)
                tmp = model.get_all(fields=v["all_fields"])

            kwargs_list, args_list, sql_list = [], [], []

            if v.get("kwargs_fields", None):
                kwargs_list = self.get_kwargs_list(v["kwargs_fields"], tmp)

            if v.get("args_fields", None):
                args_list = self.get_args_list(v["args_fields"], tmp)

            if v.get("db_fields", None):
                sql_list = self.get_sql_list(v["db_fields"], tmp)

        return kwargs_list, args_list, sql_list

    def get_dynamics_params(self, params):
        result = []
        if params:
            dynamics_params = params.get("dynamics", {})
            for n, p in dynamics_params.items():
                if p["type"] == "group":
                    result += self.get_dynamics_group_params(p)
                else:
                    result += self.get_dynamics_param(n, p)

        return result

    def get_statics_params(self, params):
        result = []
        if params:
            result = [{k: v} for k, v in params.get("statics", {}).items()]
        return result

    def get_db_params(self, task):
        params = task.params
        params = params.get("url", None)
        params = params.get("params", None)
        if params:
            kwargs_list, args_list, sql_list = (
                self.get_filter_values_from_db(
                    destination=task.destination,
                    params=params.get("db", None),
                    channel=task.channel,
                )
                if params
                else ([], [], [])
            )

            from itertools import zip_longest

            zip_data = list(zip_longest(sql_list, kwargs_list, args_list))
            # zip_datas = list(zip_longest(sql_list, kwargs_list, args_list))[0:2]
            return zip_data
        return None


class S3Client(Client):
    def __init__(
        self,
        application=None,
        task=None,
        bucket_name=None,
        env="development",
        date=None,
    ):
        super().__init__(env)
        self.client = boto3.client("s3")
        self.resource = boto3.resource("s3")
        self.application = application
        self.task = task
        self.bucket_name = "linkedin-ingest-dev-staging"
        self.date = date
        self._task_full_path = None
        self._bucket = None
        self.tmp_objects_list = []

    @property
    def bucket(self):
        if not self._bucket:
            self._bucket = self.resource.Bucket(self.bucket_name)
        return self._bucket

    def get_object_key_path_elements(self) -> List[Tuple]:
        """Build list of tuples allowing to build the path.



        Args:
            date: Optionnal. A datetime object.
            Default to None

        Returns:
            A list of tuples: ((data_name, data_value), ...)
        """
        result = []

        # Add elements
        result.append(("application", self.application.name))
        result.append(("environment", self.env))
        result.append(("task", self.task.name))

        if self.date:
            result.append(("year", f"{self.date:%Y}"))
            result.append(("month", f"{self.date:%m}"))
            result.append(("day", f"{self.date:%d}"))

        return result

    def get_object_key(self) -> str:
        """Build a string representing the S3 path after the bucket_name. It contains the filename.

        Directories are in Hive format: /key=value/

        Returns:
            A string: "key=value/key1=value1/..."
        """
        data = self.get_object_key_path_elements()
        result = os.path.join(*["{}={}/".format(d[0], d[1]) for d in data])
        result += "--".join([d[1] for d in data])
        result += ".csv"

        return result

    def get_data_in_file_like_obj_format(self, data):
        fileObj = BytesIO()
        fileObj.write(str.encode(data))

        # Put the cursor at the beginning of the file like obj. Without that, the write file will be empty. # noqa : E501
        fileObj.seek(0)
        return fileObj

    def upload_data(self, data):
        """Upload a file to an S3 bucket

        :param data: Data to upload
        :return: True if file was uploaded, else False
        """
        file_obj = self.get_data_in_file_like_obj_format(data)

        object_key = self.get_object_key()

        try:
            response = self.client.upload_fileobj(
                file_obj, self.bucket_name, object_key
            )
            self.tmp_objects_list.append(ObjectWrapper(self.bucket.Object(object_key)))
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def copy_from_tmp_objects_list_to_final_dest(self):
        for obj in self.tmp_objects_list:
            dest_obj = ObjectWrapper(self.bucket.Object(f"{obj.key[:-4]}"))
            dest_obj.copy_from(CopySource={"Bucket": self.bucket_name, "Key": obj.key})
            dest_obj.wait_until_exists()

    def move_object(self, old_key, new_key):
        """Move an S3 object by copying and deleting after.

        Args:
            old_key: str
                The key of the object to "move"
            new_key:
                The object new key

        Returns:
            True if success, else False
        """
        # Copy object A as object B
        # copy_source = {
        #     "Bucket": "linkedin-ingest-dev-staging",
        #     "Key": old_key,
        # }
        # self.resource.meta.client.copy(
        #     copy_source, "linkedin-ingest-dev-staging", new_key
        # )

        object_key = "doc-example-object"
        obj_wrapper = ObjectWrapper(self.bucket.Object(object_key))
        obj_wrapper.put(__file__)
        print(f"Put file object with key {object_key} in bucket {self.bucket.name}.")

        self.client.copy_object(
            Bucket="linkedin-ingest-dev-staging",
            CopySource=f"{self.bucket_name}/{old_key}",
            Key=new_key,
        )
        # Delete the former object A
        response = self.client.delete_object(
            Bucket="linkedin-ingest-dev-staging", Key=old_key
        )

    def rollback(self):
        for o in self.tmp_objects_list:
            o.delete()
        print("All tmp files deleted")

    def success_write(self):
        print("Task succefully runned)")


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

TIMEOUT_IN_MILLISECONDS = 3600000


class BingAdsClient(Client):
    def __init__(self, task=None, env=None):
        super().__init__(env)
        from bingads.v13.reporting import ReportingServiceManager

        if env == "elopment":
            from bing.src.sandbox_auth_helper import (
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

        self.reporting_service = ServiceClient(
            service="ReportingService",
            version=13,
            authorization_data=self.authorization_data,
            environment="production",
        )

        self.customer_service = ServiceClient(
            service="CustomerManagementService",
            version=13,
            authorization_data=self.authorization_data,
            environment=ENVIRONMENT,
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
        # db_params, kwargs = self.get_zipped_data(self.task) or ([], [])

        result = []
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
                if tmp:
                    for t in tmp:
                        for i in param[0]:
                            for k, v in i.items():
                                t[k] = v
                        result.append(t)
        else:
            result = request.get()

        # task_name = self.task.name
        # self.report_request = self.get_report_request()
        # self.submit_and_download()
        tmp = []
        for r in result:
            tmp.append({"datas": r})

        return tmp

    def get_report_request(self):

        aggregation = "Daily"
        exclude_column_headers = False
        exclude_report_footer = True
        exclude_report_header = True
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

        # You can either use a custom date range or predefined time.
        # time.PredefinedTime = "Yesterday"
        time.ReportTimeZone = "BrusselsCopenhagenMadridParis"
        time.CustomDateRangeStart = start_date
        time.CustomDateRangeEnd = end_date
        return_only_complete_data = True

        if self.task.name == "daily_accounts_update":
            result_report = self.get_account_performance_report_request(
                account_id=self.authorization_data.account_id,
                aggregation=aggregation,
                exclude_column_headers=exclude_column_headers,
                exclude_report_footer=exclude_report_footer,
                exclude_report_header=exclude_report_header,
                report_file_format=REPORT_FILE_FORMAT,
                return_only_complete_data=return_only_complete_data,
                time=time,
            )

        # if self.task_name == "user_location_performance_report":
        #     result_report = get_user_location_performance_report_request(
        #         account_id=self.account_id,
        #         aggregation=aggregation,
        #         exclude_column_headers=exclude_column_headers,
        #         exclude_report_footer=exclude_report_footer,
        #         exclude_report_header=exclude_report_header,
        #         report_file_format=REPORT_FILE_FORMAT,
        #         return_only_complete_data=return_only_complete_data,
        #         time=time,
        #     )

        # if report_name == "professional_demographics_audience_report":
        #     result_report = get_professional_demographics_audience_report_request(
        #         account_id=account_id,
        #         aggregation=aggregation,
        #         exclude_column_headers=exclude_column_headers,
        #         exclude_report_footer=exclude_report_footer,
        #         exclude_report_header=exclude_report_header,
        #         report_file_format=REPORT_FILE_FORMAT,
        #         return_only_complete_data=return_only_complete_data,
        #         time=time,
        #     )

        # if report_name == "campaign_performance_report":
        #     result_report = get_campaign_performance_report_request(
        #         account_id=account_id,
        #         aggregation=aggregation,
        #         exclude_column_headers=exclude_column_headers,
        #         exclude_report_footer=exclude_report_footer,
        #         exclude_report_header=exclude_report_header,
        #         report_file_format=REPORT_FILE_FORMAT,
        #         return_only_complete_data=return_only_complete_data,
        #         time=time,
        #     )

        # if report_name == "adgroup_performance_report_request":
        #     result_report = get_adgroup_performance_report_request(
        #         account_id=account_id,
        #         aggregation=aggregation,
        #         exclude_column_headers=exclude_column_headers,
        #         exclude_report_footer=exclude_report_footer,
        #         exclude_report_header=exclude_report_header,
        #         report_file_format=REPORT_FILE_FORMAT,
        #         return_only_complete_data=return_only_complete_data,
        #         time=time,
        #     )

        # if report_name == "ad_performance_report_request":
        #     result_report = get_ad_performance_report_request(
        #         account_id=account_id,
        #         aggregation=aggregation,
        #         exclude_column_headers=exclude_column_headers,
        #         exclude_report_footer=exclude_report_footer,
        #         exclude_report_header=exclude_report_header,
        #         report_file_format=REPORT_FILE_FORMAT,
        #         return_only_complete_data=return_only_complete_data,
        #         time=time,
        #     )

        # if report_name == "professional_demographic_audience_report_request":
        #     result_report = get_professional_demographic_audience_report_request(
        #         account_id=account_id,
        #         aggregation=aggregation,
        #         exclude_column_headers=exclude_column_headers,
        #         exclude_report_footer=exclude_report_footer,
        #         exclude_report_header=exclude_report_header,
        #         report_file_format=REPORT_FILE_FORMAT,
        #         return_only_complete_data=return_only_complete_data,
        #         time=time,
        #     )

        return result_report

    def submit_and_download(self):
        """Submit the download request and then use the ReportingDownloadOperation result to
        track status until the report is complete e.g. either using
        ReportingDownloadOperation.track() or ReportingDownloadOperation.get_status()."""

        # global reporting_service_manager
        reporting_download_operation = self.reporting_service_manager.submit_download(
            self.get_report_request()
        )

        # You may optionally cancel the track() operation after a specified time interval.
        # reporting_operation_status = reporting_download_operation.track(
        #     timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS
        # )

        # You can use ReportingDownloadOperation.track() to poll until complete as shown above,
        # or use custom polling logic with get_status() as shown below.
        for i in range(10):
            time.sleep(
                self.reporting_service_manager.poll_interval_in_milliseconds / 1000.0
            )

            download_status = reporting_download_operation.get_status()

            if download_status.status == "Success":
                break

        result_file_path = reporting_download_operation.download_result_file(
            result_file_directory=FILE_DIRECTORY,
            result_file_name=self.get_result_file_name(self.task.name),
            decompress=True,
            overwrite=True,  # Set this value true if you want to overwrite the same file.
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS,  # You may optionally cancel the download after a specified time interval.
        )

        self.output_status_message("Download result file: {0}".format(result_file_path))

    def get_account_performance_report_request(
        self,
        account_id,
        aggregation,
        exclude_column_headers,
        exclude_report_footer,
        exclude_report_header,
        report_file_format,
        return_only_complete_data,
        time,
    ):

        report_request = self.reporting_service.factory.create(
            "AccountPerformanceReportRequest"
        )
        report_request.Aggregation = aggregation
        report_request.ExcludeColumnHeaders = exclude_column_headers
        report_request.ExcludeReportFooter = exclude_report_footer
        report_request.ExcludeReportHeader = exclude_report_header
        report_request.Format = report_file_format
        report_request.ReturnOnlyCompleteData = return_only_complete_data
        report_request.Time = time
        report_request.ReportName = "Accounts Performance Report"

        scope = self.reporting_service.factory.create("AccountReportScope")
        scope.AccountIds = {"long": [account_id]}

        report_request.Scope = scope

        report_columns = self.reporting_service.factory.create(
            "ArrayOfAccountPerformanceReportColumn"
        )
        report_columns.AccountPerformanceReportColumn.append(
            [
                "AccountName",
                "AccountNumber",
                "AccountId",
                "AccountStatus",
                # "Impressions",
                # "Clicks",
                # "Ctr",
                # "AverageCpc",
                # "Spend",
                # "TimePeriod",
            ]
        )
        report_request.Columns = report_columns

        return report_request

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
            result = nested_get(result, self.task.params["url"]["response_data_key"])
            return result

        if self.task.name == "daily_campaigns_update":

            result = self.service.GetCampaignsByAccountId(
                **self.kwargs[0],
                **self.param[0],
            )

            result = recursive_asdict(result)
            result = nested_get(result, self.task.params["url"]["response_data_key"])
            return result

        if self.task.name == "daily_adgroups_update":

            # TODO: Generic way of managing params here
            result = self.service.GetAdGroupsByCampaignId(
                **self.param[0],
            )
            result = recursive_asdict(result)
            result = nested_get(result, self.task.params["url"]["response_data_key"])
            return result

        if self.task.name == "daily_ads_update":
            # import logging

            # logging.basicConfig(level=logging.INFO)
            # logging.getLogger("suds.client").setLevel(logging.DEBUG)
            # logging.getLogger("suds.transport.http").setLevel(logging.DEBUG)

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
            result = self.service.GetAdsByAdGroupId(AdTypes=adTypes, **self.param[0])
            result = recursive_asdict(result)
            result = nested_get(result, self.task.params["url"]["response_data_key"])
            print(result)
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


# class S3ClientBuilder:
#     def __init__(self):
#         self._instance = None

#     def __call__(self, application, task, env, **_ignored):
#         if not self._instance:
#             self._instance = S3Client(application, task, env)
#         return self._instance


# if __name__ == "__main__":
#     from client_factory import ClientFactory

#     factory = ClientFactory()
#     factory.register_builder("S3", S3ClientBuilder())
#     print("dudu")
