import calendar
import uuid
from collections import ChainMap
from datetime import date, datetime, timedelta

import dateutil
import pytz
from src.commons.model import Model
from src.constants import ENVS_LIST
from src.utils.endpoint_utils import Endpoint
from src.utils.various_utils import (
    get_chunks,
    run_in_threads_pool,
    zip_longest_repeat_value,
)


class Client:
    """
    # noqa: E501
    # This class manage lots of things. Some of them are not directly connected to linkedin and should be extract to a Client class.
    TODO: Create Client class

    - Retrieving task parameters from json file
    - Creating the url to request and all associated operation (requesting Db for filter values, building args and kwargs, )
    - Retrieving request result from source
    """

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

    def get_month_from_offset(self, date):
        pass

    def get_month_day_range(date):
        """
        For a date 'date' returns the start and end date for the month of 'date'. # noqa: E501

        Month with 31 days:
        >>> date = datetime.date(2011, 7, 27)
        >>> get_month_day_range(date)
        (datetime.date(2011, 7, 1), datetime.date(2011, 7, 31))

        Month with 28 days:
        >>> date = datetime.date(2011, 2, 15)
        >>> get_month_day_range(date)
        (datetime.date(2011, 2, 1), datetime.date(2011, 2, 28))
        """
        first_day = date.replace(day=1)
        last_day = date.replace(
            day=calendar.monthrange(date.year, date.month)[1]
        )  # noqa: E501
        return first_day, last_day

    def get_kwargs_list(self, kwargs_fields=[], sql_datas=[], statics={}):
        # TODO Alain: Check if can be deleted.
        result = []
        for d in sql_datas:
            tmp_result = []
            for f in kwargs_fields:
                r = {f[2]: f[1].format(d[f[0]])}
                tmp_result.append({**r})
            tmp_result.append({**statics})
            result.append(tmp_result)

        return result

    def get_args_list(self, args_fields=[], sql_datas=[]):
        # TODO Alain: Check if can be deleted.
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
                tmp_result.append({f["destination_key"]: d[f["origin_key"]]})
            result.append(tmp_result)

        return result

    def get_request_parameters_lists(self):
        result = [], [], []
        query = self.task.params.get("query", None)
        if not query:
            return result

        query_params = query.get("params", None)

        result_lists = []
        if query_params:
            for param in query_params:
                tmp_result = []
                if param["type"] == "constant":
                    tmp_result = [{param["name"]: param["value"]}]
                if param["type"] == "db":
                    tmp_result = Model.get_from_raw_sql(
                        self.task.db_connection,
                        self.task.params["data_source"]["raw_sql"],
                    )
                    tmp_result = [dict(r) for r in tmp_result]
                    tmp_result = [
                        {param["name"]: tr[param["source_key"]]}
                        for tr in tmp_result  # noqa: E501
                    ]

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

    def get_part_of_date(self, date_object, part_type):
        """Return digit correspondign to days, month or year from datetime object.

        parameters:
            date_object: The source of value
            datetime
            part_type: The part to return
            str

        Returns:
            int: The digits extracted from source.

        """

    def get_date_params(self, url_params, value):
        # TODO Alain: Check if can be deleted.
        result = []
        if url_params:
            for n, p in url_params.items():
                result.append(self.get_date_param(n, p, value))

        return result

    def get_date_param(self, name, params, value):
        if params["value_type"] == "date":
            return {
                name: params["url_query_parameter_value"].format(
                    day=value.day,
                    month=value.month,
                    year=value.year,
                ),
            }

        return None

    def get_day_relative_to_today_from_params(self, day_params, offset_unity):

        today = datetime.date(datetime.now())
        tmp = {offset_unity: int(day_params["offset_value"])}
        day = today - timedelta(**tmp)
        return day

    def get_start_date(self, start_date_params=None, offset_unity=None):
        # Use max(field) to retrieve start_date.
        # If source table is empty, then the "offset_value" is used.

        if start_date_params.get("use_max_from_field", None):
            tmp_date = Model(
                start_date_params["use_max_from_field"]["model"],
                db_connection=self.task.db_connection,
                channel=self.task.channel,
            ).get_max_for_date_field_plus_one_day(
                start_date_params["use_max_from_field"]["field"]
            )[
                0
            ][
                "max_date"
            ]
            if tmp_date:
                start_date = tmp_date.date()
        else:
            if start_date_params["offset_value"] is not None:
                start_date = self.get_day_relative_to_today_from_params(
                    start_date_params, offset_unity
                )

        return start_date

    def get_end_date(self, end_date_params=None, offset_unity=None):
        end_date = self.get_day_relative_to_today_from_params(
            end_date_params, offset_unity
        )
        return end_date

    def get_day_ranges_list(self, date_range_params):

        offset_unity = date_range_params["offset_unity"]

        start_date = self.get_start_date(
            start_date_params=date_range_params["start_date"],
            offset_unity=offset_unity,
        )

        end_date = self.get_end_date(
            end_date_params=date_range_params["end_date"],
            offset_unity=offset_unity,  # noqa: E501
        )

        if end_date == datetime.date(datetime.today()):
            raise ValueError(
                "end_date can't be today. Must be previous day or older one."
            )

        result = []

        if (
            start_date != end_date
            and date_range_params["split_allowed"] is True  # noqa: E501
        ):  # noqa: E501
            delta = end_date - start_date
            days = [
                start_date + timedelta(days=i) for i in range(delta.days + 1)
            ]  # noqa: E501
            for d in days:
                tmp_result = []
                tmp_result.append(
                    self.get_date_params(
                        date_range_params["start_date"]["url_params"], d
                    )
                )
                tmp_result.append(
                    self.get_date_params(
                        date_range_params["end_date"]["url_params"], d
                    )  # noqa: E501
                )
                result.append(tmp_result)
        else:
            tmp_result = []
            tmp_result.append(
                self.get_date_params(
                    date_range_params["start_date"]["url_params"], start_date
                )
            )
            tmp_result.append(
                self.get_date_params(
                    date_range_params["end_date"]["url_params"], end_date
                )
            )
            result.append(tmp_result)

        return result

    def get_month_ranges_list(self, date_range_params):
        offset_unity = date_range_params["offset_unity"]
        start_date = self.get_day_relative_to_today_from_params(
            date_range_params["start_date"], offset_unity
        )

        end_date = self.get_day_relative_to_today_from_params(
            date_range_params["end_date"], offset_unity
        )
        year = start_date.year
        month = start_date.month
        result = []
        while (year, month) <= (end_date.year, end_date.month):
            tmp_result = []
            sd = date(year, month, 1)
            last_day_of_month_number = calendar.monthrange(year, month)[1]
            ed = date(year, month, last_day_of_month_number)

            tmp_result.append(
                self.get_date_params(
                    date_range_params["start_date"]["url_params"], sd
                )  # noqa: E501
            )

            tmp_result.append(
                self.get_date_params(
                    date_range_params["end_date"]["url_params"], ed
                )  # noqa: E501
            )

            result.append(tmp_result)

            if month == 12:
                month = 1
                year += 1
            else:
                month += 1
        return result

    def get_date_ranges_list(self, params):

        result = []
        if params["offset_unity"] == "days":
            result = self.get_day_ranges_list(params)
        if params["offset_unity"] == "months":
            result = self.get_month_ranges_list(params)

        return result

    def get_dynamics_params(self, params):
        result = []
        if params:
            dynamics_params = params.get("dynamics", {})
            for n, p in dynamics_params.items():
                result += self.get_dynamics_param(n, p)

        return result

    def get_statics_params(self, params):
        result = []
        if params:
            result = [{k: v} for k, v in params.get("statics", {}).items()]
        return result

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
        result = []
        for fr in futures_results:
            local_result = []
            for k, v in fr.items():
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
                    api_data = (
                        r if isinstance(r, dict) else {task_params["key_for_values"]: r}
                    )
                    local_result.append(
                        dict(ChainMap(*data_to_add_to_results, api_data))  # noqa: E501
                    )
            result.extend(local_result)
        return result

    def get_endpoint_list(self, task_params, db_params):
        endpoint_list = [
            (
                Endpoint(
                    params=v,
                    url_template=task_params["query"]["template"],
                    query_params=task_params["query"]["params"],
                ),
                k,
            )
            for k, v in db_params.items()
        ]
        return endpoint_list

    def do_requests(self, task_params, headers, endpoint_list, pagination_function):
        futures_results = []
        endpoint_list_list = get_chunks(endpoint_list, chunk_size=50)
        for lst in endpoint_list_list[0:2]:
            chunks_result_list = run_in_threads_pool(
                request_params_list=lst,
                source_function=self.do_get_query,
                headers=headers,
                result_key=task_params["query"]["response_datas_key"],
                pagination_function=pagination_function,
            )
            futures_results.extend(chunks_result_list)
        return futures_results
