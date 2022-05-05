from datetime import date, datetime, timedelta

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

    def get_filter_values_from_db(
        self, destination=None, params=None, channel=None, db_connection=None
    ):
        if not params:
            return [], [], []

        for v in params.values():
            if "rawsql" in v["type"]:
                tmp = Model.get_from_raw_sql(db_connection, v["raw_sql"])
            else:
                model = Model(
                    v["filter_model"], db_connection=db_connection, channel=channel
                )
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
                    destination=task.request_data_source,
                    params=params.get("db", None),
                    channel=task.channel,
                    db_connection=self.task.db_connection,
                )
                if params
                else ([], [], [])
            )

            from itertools import zip_longest

            zip_data = list(zip_longest(sql_list, kwargs_list, args_list))
            return zip_data
        return None
