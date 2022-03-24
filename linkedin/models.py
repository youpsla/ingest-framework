"""
Model and Fields definitions.

"""
import json
import os
from datetime import datetime

from sql_utils import SqlQuery
from utils.various_utils import nested_get


class Model:
    def __init__(self, model_name, destination=None):
        self.model_name = model_name
        self.destination = destination
        self._fields_list = None
        self.set_fields()

    @property
    def params(self):
        __location__ = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__))
        )
        with open(os.path.join(__location__, "models.json"), "r") as f:
            f = json.load(f)
        for model_definition in f:
            if self.model_name in model_definition:
                return model_definition[self.model_name]

    def set_field(self, name, params):
        field = Field(name, params, self)
        setattr(self, name, field)

    def set_fields(self):
        for k, v in self.params.items():
            self.set_field(k, v)

    def populate_values(self, source_values):
        for f in self.fields_list:
            if f.type == "constant":
                f.value = f.field_params["value"]
                continue
            v = nested_get(source_values, f.source_path)
            f.value = v

    def get_db_values_tuple(self):
        result = []
        for f in self.get_db_fields_list():
            result.append(f.db_value)
        return tuple(result)

    def get_db_fields_list(self):
        result = []
        for f in self.fields_list:
            if getattr(f, "exclude", False):
                continue
            result.append(f)
        return result

    @property
    def fields_list(self):
        if not self._fields_list:
            res = []
            for i in vars(self).values():
                if isinstance(i, Field):
                    res.append(i)
            self._fields_list = res
            return res
        else:
            return self._fields_list

    def get_all(self, fields=None):
        q = SqlQuery(self.destination, "select", model=self, fields=fields)
        res = q.run()
        return res

    def get_max_for_field(self, field):
        q = SqlQuery(self.destination, "select_max", max_field=field, model=self)
        res = q.run()
        return res

    @staticmethod
    def get_from_raw_sql(destination, sql):
        q = SqlQuery(destination, "raw_sql", raw_sql=sql)
        res = q.run()
        return res

    def get_last_month_ids(
        self,
        range_type="previous_month",
        range_filter_field="start_date",
        id_field="id",
    ):
        today = datetime.today()
        if range_type == "previous_month":
            from dateutil.relativedelta import relativedelta

            today - relativedelta(months=1)
            where = (
                f" {range_filter_field} between"
                f" '{today - relativedelta(day=31)}' and"
                f" '{today + relativedelta(day=31)}'"
            )
            q = SqlQuery(
                self.destination, "select", model=self, where=where, fields=(id_field,)
            )
            res = q.run()
            return res


class Field:
    def __init__(self, field_name, field_params, model):
        self.name = field_name
        self.field_params = field_params
        self.model = model
        self.set_attributes_from_params()
        self.value = None

    def set_attributes_from_params(self):
        for k, v in self.field_params.items():
            setattr(self, k, v)

    @property
    def db_value(self, model=None):
        result = None
        if self.type in ["raw", "constant"]:
            return self.value
        if self.type == "composite":
            format_dict = {
                # TODO: self.model is uggly hack IMHO. Should find better design. See model.get_fields_value_tuple_for_sql # noqa: E501
                v: getattr(self.model, k).value
                for k, v in self.composite_fields.items()
            }
            result = self.composite_pattern.format(**format_dict)
        if self.type == "function":
            if self.transform_function["type"] == "lstrip":
                if not isinstance(self.value, str):
                    result = ""

                else:
                    result = self.value.lstrip(
                        self.transform_function["string_to_strip"]
                    )
            if (
                self.transform_function["type"]
                == "datetime_from_timestamp_in_milliseconds"
            ):
                result = datetime.fromtimestamp(self.value / 1000.0)
            if self.transform_function["type"] == "split":
                result = self.value.split(self.transform_function["split_caracter"])

                # Sometimes Bing geo API returns only 2 locations instead of 3.
                # This is an issue because we don't know which location is  not returned. # noqa: E501
                # For example, it as returned "Belgrad, Serbia". It looks like region has been omitted. # noqa: E501
                try:
                    result = result[self.transform_function["split_position"]].strip()
                except IndexError:
                    result = ""

        if self.type == "datetime_today":
            # return datetime.today()
            result = datetime.today()

        return self.get_sql_escaped(result)

    def get_sql_escaped(self, value):
        result = value
        return result.replace("'", "''") if isinstance(result, str) else result

    # @value.setter
    # def value(self, value):
    #     if not self.field_params["source_path"]:
    #         self._value = self.field_params["constant_value"]
    #     if self.field_params["transform_function"] is None:
    #         self._value = value
    #     else:
    #         f = self.field_params["transform_function"]
    #         if f["type"] == "lstrip":
    #             self._value = value.lstrip(f["string_to_strip"])
    #         elif f["type"] == "composite":
    #             pass


if __name__ == "__main__":
    s = Model("accounts")
    for f in s._fields_name:
        if f == "last_modified_date":
            s.last_modified_date.value = datetime.now().timestamp() * 1000
            print(s.last_modified_date.value)
