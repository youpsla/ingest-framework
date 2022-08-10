import calendar
import uuid
from collections import ChainMap
from datetime import date, datetime, timedelta

import dateutil
import pytz
from dateutil.relativedelta import relativedelta
from src.commons.model import Model
from src.constants import ENVS_LIST
from src.utils.various_utils import zip_longest_repeat_value


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

    def get_month_from_offset(self, date):
        pass

    def get_month_day_range(date):
        """
        For a date 'date' returns the start and end date for the month of 'date'.

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
        last_day = date.replace(day=calendar.monthrange(date.year, date.month)[1])
        return first_day, last_day

    def get_dynamics_group_params(self, params):
        url_params = params["url_params"]
        today = datetime.today()
        result = []
        if params["offset_unity"] == "days":
            tmp = {params["offset_unity"]: int(params["offset_value"])}
            start_date = today - timedelta(**tmp)
            for k, v in url_params.items():
                result.append(self.get_dynamics_param(k, v, start_date))

        if params["offset_unity"] == "months":

            today = date.today()
            last_day_of_prev_month = (
                today
                - dateutil.relativedelta.relativedelta(
                    months=int(params["offset_value"]) - 1
                )
            ).replace(day=1) - timedelta(days=1)

            start_day_of_prev_month = last_day_of_prev_month.replace(day=1)

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

    def get_kwargs_list(self, kwargs_fields=[], sql_datas=[], statics={}):
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
                        {param["name"]: tr[param["source_key"]]} for tr in tmp_result
                    ]
                    # for tr in tmp_result:
                    #     final_result.append({param["name"]: tr[param["source_key"]]})

                if param["type"] == "timestamp_from_epoch":
                    target_day = self.get_day_relative_to_today_from_params(
                        day_params=param,
                        offset_unity=param["offset_unity"],
                    )
                    target_datetime = datetime.fromordinal(target_day.toordinal())
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

                result_lists.append(tmp_result)
        # As zip_longest_repeat_value needs only non empty lists. We check that here.
        # Could be the case when we use parameters from Db and the select statement returns no value.
        # In this case, we return an empty list. Then no request will be done to the endpoint.
        for r in result_lists:
            if len(r) == 0:
                return []
        tmp_result = zip_longest_repeat_value(*result_lists)
        tmp_result = [list(a) for a in tmp_result]

        final_result = []

        for tmp in tmp_result:
            final_result.append(dict(ChainMap(*tmp)))

        return final_result

    def get_date_params(self, url_params, value):
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
            end_date_params=date_range_params["end_date"], offset_unity=offset_unity
        )

        if end_date == datetime.date(datetime.today()):
            raise ValueError(
                "end_date can't be today. Must be previous day or older one."
            )

        result = []

        if start_date != end_date and date_range_params["split_allowed"] is True:
            delta = end_date - start_date
            days = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
            for d in days:
                tmp_result = []
                tmp_result.append(
                    self.get_date_params(
                        date_range_params["start_date"]["url_params"], d
                    )
                )
                tmp_result.append(
                    self.get_date_params(date_range_params["end_date"]["url_params"], d)
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
                self.get_date_params(date_range_params["start_date"]["url_params"], sd)
            )

            tmp_result.append(
                self.get_date_params(date_range_params["end_date"]["url_params"], ed)
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
            self.get_request_parameters_lists() if self.task.params else ([], [], [])
        )

        # date_range = params.get("date_range", None)
        # if not date_range:
        #     result = zip_data
        # else:
        #     result = []
        #     for idx, zd in enumerate(zip_data):
        #         date_range_list = self.get_date_ranges_list(params["date_range"])
        #         for dr in date_range_list:
        #             tmp = copy.deepcopy(zd)
        #             for d in dr:
        #                 tmp[1].extend(d)
        #             result.append(tmp)

        # return result

        # Add uuid to each record
        final_result = {}
        for z in zip_data:
            final_result[uuid.uuid4()] = z
        return final_result
