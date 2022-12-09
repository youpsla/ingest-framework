import uuid
from collections import ChainMap
from datetime import datetime

import pendulum
import pytz

from src.commons.model import Model
from src.constants import ENVS_LIST
from src.utils.custom_logger import logger
from src.utils.various_utils import (get_chunks, run_in_threads_pool,
                                     zip_longest_repeat_value)


class Client:
    """
    # noqa: E501
    # This class contains functions allowing:
        - getting a list of all endpoints to be qeried
        - doing http call to the provider endpoint
    """

    def __init__(self, env):
        if self.check_env(env):
            self.env = env

    @staticmethod
    def check_env(env):
        """Check if the running env is a good one.
        TODO: Remove or move at another place

        parameters:
            env: The running env
            str

        Returns:
            Bool: True

        Raises:
            Attribute Error is env does not exist.

        """
        if env not in ENVS_LIST:
            raise AttributeError(
                "Argument 'env' has to be:\n{}\nGot '{}' instead".format(
                    "\n".join(["- " + i for i in ENVS_LIST]), env
                )
            )
        else:
            logger.info(f"Running environement: {env}")
            return True

    def get_request_parameters_lists(self):
        """Construct the list dict. Each dict contains parameters for one endpoint.

        Returns:
            list: A list of dicts

        """

        # Retrieve the "query" part of the task json definition
        # If not params, returns an empty list
        query = self.task.params.get("query", None)
        if not query:
            return []

        # Get the params of the query
        logger.info(
            f"Get query parameters from json file:{self.task.get_params_json_file_path()}"
        )
        query_params = query.get("params")
        if not query_params:
            logger.info(
                f"Task {self.task.name} does not have query parameters. Should be a destination only task. Often just a Db request."
            )

        # Get each param and add it ti the result_lists (list of list)
        result_lists = []
        if query_params:
            logger.info("Start query_params parsing.")
            for param in query_params:
                tmp_result = []

                # Case when param is a constant
                if param["type"] == "constant":
                    tmp_result = [{param["name"]: param["value"]}]

                # Case when param is of type "db". A request of data source is done
                if param["type"] == "db":
                    db_result = Model.get_from_raw_sql(
                        self.task.db_connection,
                        self.task.params["data_source"]["raw_sql"],
                    )
                    # Transform OrderedDict into dict
                    db_result = [dict(r) for r in db_result]

                    # Split the list returned by db in slice of chunck_size. # noqa: E501
                    # This is done when we want to send more than one id to the API endpoint. # noqa: E501
                    # For linkedin, it allow to retrieve organizations names by batch. Then we are not faced to the 10 000 queries daily limit. # noqa: E501
                    if param.get("chunck"):
                        for d in get_chunks(db_result, param.get(
                                "chunck_size", 20)):
                            key = str(list(d[0].keys())[0])
                            local_result = {
                                key: ",".join(
                                    map(lambda d: str(list(d.values())[0]), d)
                                )
                            }
                            tmp_result.append(local_result)
                    else:
                        tmp_result = [
                            {param["name"]: tr[param["source_key"]]}
                            for tr in db_result  # noqa: E501
                        ]

                # Case when param is of type "timestamp_from_epoch"
                # Used for buiding date range params.

                if param["type"] == "timestamp_from_epoch_exact_time":
                    target_day = self.get_day_relative_to_today_from_params(
                        day_params=param,
                        offset_unity=param["offset_unity"],
                    )

                    # Transform to timestamp
                    target_timestamp = int(
                        datetime.timestamp(target_day)) * 1000

                    tmp_result = [{param["name"]: target_timestamp}]

                if param["type"] == "timestamp_from_epoch":
                    target_day = self.get_day_relative_to_today_from_params(
                        day_params=param,
                        offset_unity=param["offset_unity"],
                    )
                    target_datetime = datetime.fromordinal(
                        target_day.toordinal()
                    )  # noqa: E501
                    if param["position"] == "start":
                        target_datetime = target_datetime.replace(
                            hour=0, minute=0, second=0, tzinfo=pytz.UTC
                        )
                        target_timestamp = (
                            int(datetime.timestamp(target_datetime)) * 1000
                        )

                        tmp_result = [{param["name"]: target_timestamp}]

                    if param["position"] == "end":
                        target_datetime = target_datetime.replace(
                            hour=23, minute=59, second=59, tzinfo=pytz.UTC
                        )
                        target_timestamp = (
                            int(datetime.timestamp(target_datetime)) * 1000
                        )

                        tmp_result = [{param["name"]: target_timestamp}]

                if param["type"] == "part_of_date":
                    target_day = self.get_day_relative_to_today_from_params(
                        day_params=param,
                        offset_unity=param["offset_unity"],
                    )
                    if param["part_of_date_name"] == "day":
                        result = datetime.strftime(target_day, "%d")
                        tmp_result = [{param["name"]: result}]
                    if param["part_of_date_name"] == "month":
                        result = datetime.strftime(target_day, "%m")
                        tmp_result = [{param["name"]: result}]
                    if param["part_of_date_name"] == "year":
                        result = datetime.strftime(target_day, "%Y")
                        tmp_result = [{param["name"]: result}]

                if param["type"] == "part_of_date_month":
                    target_month = pendulum.now().subtract(
                        **{param["offset_unity"]: param["offset_value"]}
                    )
                    if param["position"] == "start":
                        target_day = target_month.first_of("month")
                    if param["position"] == "end":
                        target_day = target_month.last_of("month")

                    if param["part_of_date_name"] == "day":
                        result = datetime.strftime(target_day, "%d")
                        tmp_result = [{param["name"]: result}]
                    if param["part_of_date_name"] == "month":
                        result = datetime.strftime(target_day, "%m")
                        tmp_result = [{param["name"]: result}]
                    if param["part_of_date_name"] == "year":
                        result = datetime.strftime(target_day, "%Y")
                        tmp_result = [{param["name"]: result}]

                if param["type"] == "as_YYYY-MM-DD":
                    target_day = self.get_day_relative_to_today_from_params(
                        day_params=param,
                        offset_unity=param["offset_unity"],
                    )

                    target_day = target_day.strftime("%Y-%m-%d")

                    tmp_result = [{param["name"]: target_day}]

                result_lists.append(tmp_result)
        # As zip_longest_repeat_value needs only non empty lists. We check that here. # noqa: E501
        # Could be the case when we use parameters from Db and the select statement returns no value. # noqa: E501
        # In this case, we return an empty list. Then no request will be done to the endpoint. # noqa: E501
        for r in result_lists:
            if len(r) == 0:
                return []
        tmp_result = zip_longest_repeat_value(*result_lists)
        tmp_result = [list(a) for a in tmp_result]

        final_result = []

        for tmp in tmp_result:
            final_result.append(dict(ChainMap(*tmp)))

        return final_result

    def get_sql_list(self, db_fields=[], sql_datas=[]):
        result = []
        if not db_fields:
            return db_fields
        for d in sql_datas:
            tmp_result = []
            for f in db_fields:
                tmp_result.append({f["destination_key"]: d[f["origin_key"]]})
            result.append(tmp_result)

        return result

    def get_day_relative_to_today_from_params(self, day_params, offset_unity):

        today = pendulum.now()
        tmp = {offset_unity: int(day_params["offset_value"])}
        day = today.subtract(**tmp)
        return day

    def get_request_params(self):
        query_params = self.task.params.get("query", None)
        if not query_params:
            return []

        zip_data = (
            self.get_request_parameters_lists()
            if self.task.params
            else ([], [], [])  # noqa: E501
        )

        # Add uuid to each record
        final_result = {}
        for z in zip_data:
            final_result[uuid.uuid4()] = z
        return final_result

    def add_request_params_to_api_call_result(
        self,
        futures_results,
        task_params,
        db_params,
    ):
        """This method allow to add datas to the data received from source.
        This allow data used to build queries to be reused to be inserted in destination (Redshift).
        """
        result = []
        for fr in futures_results:
            local_result = []
            for k, v in fr.items():

                # When result from source is None, go to next record.
                if v is None:
                    continue
                for r in v:
                    data_to_add_to_results = []
                    if task_params.get(
                        "fields_to_add_to_api_result", None
                    ):  # noqa: E501
                        data_to_add_to_results = [
                            {e["destination_key"]: db_params[k][e["origin_key"]]}
                            for e in task_params[
                                "fields_to_add_to_api_result"
                            ]  # noqa: E501
                        ]
                    # Sometimes, values returned by the source
                    api_data = (r if isinstance(r, dict) else {
                        task_params["key_for_values"]: r})
                    local_result.append(
                        dict(ChainMap(*data_to_add_to_results, api_data))  # noqa: E501
                    )
            result.extend(local_result)
        return result

    def do_requests(self, task_params, endpoint_list, pagination_function):
        futures_results = []
        endpoint_list_list = get_chunks(endpoint_list, chunk_size=50)
        for lst in endpoint_list_list:
            chunks_result_list = run_in_threads_pool(
                request_params_list=lst,
                source_function=self.do_get_query,
                result_key=task_params["query"]["response_datas_key"],
                pagination_function=pagination_function,
            )
            futures_results.extend(chunks_result_list)
        return futures_results
