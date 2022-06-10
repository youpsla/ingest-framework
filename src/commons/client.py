import calendar
import copy
from datetime import date, datetime, timedelta
from itertools import zip_longest

import dateutil
from dateutil.relativedelta import relativedelta
from src.commons.model import Model
from src.constants import ENVS_LIST


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

    def get_months_list(self, start_month_params, end_month_params):
        start_month = self.get_first_month_of_range(start_month_params)
        end_month = ""

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
            # end_date = today - timedelta(**tmp)
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

    def get_kwargs_list(self, kwargs_fields=[], sql_datas=[]):
        result = []
        for d in sql_datas:
            tmp_result = []
            for f in kwargs_fields:
                r = {f[2]: f[1].format(d[f[0]])}
                tmp_result.append(r)
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
                # tmp_result.append({f[1]: d[f[0]]})
                tmp_result.append({f["destination_key"]: d[f["origin_key"]]})
            result.append(tmp_result)

        return result

    def get_request_parameters_lists(self, params=None, db_connection=None):
        if not params:
            return [], [], []

        for v in params.values():
            if "rawsql" in v["type"]:
                tmp = Model.get_from_raw_sql(db_connection, v["raw_sql"])
            else:
                model = Model(
                    v["filter_model"],
                    db_connection=db_connection,
                    channel=self.task.channel,
                )
                tmp = model.get_all(fields=v["all_fields"])

            kwargs_list, args_list, sql_list = [], [], []

            # from collections import OrderedDict

            # tmp = []
            # data = OrderedDict()
            # data["id"] = 507206911
            # tmp.append(data)

            if v.get("kwargs_fields", None):
                kwargs_list = self.get_kwargs_list(v["kwargs_fields"], tmp)

            if v.get("args_fields", None):
                args_list = self.get_args_list(v["args_fields"], tmp)

            if v.get("db_fields", None):
                sql_list = self.get_sql_list(v["db_fields"], tmp)

        return kwargs_list, args_list, sql_list

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
        day = today - relativedelta(**tmp)
        # day = today - timedelta(**tmp)
        return day

    def get_day_ranges_list(self, date_range_params):

        # Use max(field) to retrieve start_date.
        # If source table is empty, then the "offset_value" is used.
        if date_range_params.get("start_date", {}).get("use_max_from_field", None):
            tmp_date = Model(
                date_range_params["start_date"]["use_max_from_field"]["model"],
                db_connection=self.task.db_connection,
                channel=self.task.channel,
            ).get_max_for_date_field_plus_one_day(
                date_range_params["start_date"]["use_max_from_field"]["field"]
            )[
                0
            ][
                "max_date"
            ]
            if tmp_date:
                start_date = tmp_date.date()

        if date_range_params["start_date"]["offset_value"] is not None:
            offset_unity = date_range_params["offset_unity"]
            start_date = self.get_day_relative_to_today_from_params(
                date_range_params["start_date"], offset_unity
            )

        end_date = self.get_day_relative_to_today_from_params(
            date_range_params["end_date"], date_range_params["end_date"]["offset_unity"]
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

        # if start_date != end_date and date_range_params["split_allowed"] is True:
        #     delta = end_date - start_date
        #     days = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
        #     for d in days:
        #         tmp_result = []
        #         tmp_result.append(
        #             self.get_date_params(
        #                 date_range_params["start_date"]["url_params"], d
        #             )
        #         )
        #         tmp_result.append(
        #             self.get_date_params(date_range_params["end_date"]["url_params"], d)
        #         )
        #         result.append(tmp_result)
        # else:
        #     tmp_result = []
        #     tmp_result.append(
        #         self.get_date_params(
        #             date_range_params["start_date"]["url_params"], start_date
        #         )
        #     )
        #     tmp_result.append(
        #         self.get_date_params(
        #             date_range_params["end_date"]["url_params"], end_date
        #         )
        #     )
        #     result.append(tmp_result)

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

    def get_request_params(self, task):
        params = task.params
        params = params.get("url", None)
        params = params.get("params", None)
        if params:
            kwargs_list, args_list, sql_list = (
                self.get_request_parameters_lists(
                    params=params.get("db", None),
                    db_connection=self.task.db_connection,
                )
                if params
                else ([], [], [])
            )

            zip_data = list(zip_longest(sql_list, kwargs_list, args_list, fillvalue=[]))

            date_range = params.get("date_range", None)
            if date_range:
                result = []
                for zd in zip_data:
                    for dr in self.get_date_ranges_list(params["date_range"]):
                        tmp = copy.deepcopy(zd)
                        for d in dr:
                            tmp[1].extend(d)
                        result.append(tmp)
                return result

            return zip_data
        return None
