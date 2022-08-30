"""
Model and Fields definitions.

"""
import json
import os
from datetime import datetime

from src.utils.custom_logger import logger
from src.utils.sql_utils import SqlQuery
from src.utils.various_utils import nested_get


class ModelManager:
    def __init__(self):
        pass


class Model:
    """A class representing datas to be store in Db. # noqa: E501

    Each attribute of type Field represent a data collected from the source.
    This class contains some methods for querying the Db.
    Can have Fields with "exclude" attribute. Those attribute will not be saved in Db.

    Attributes:
        model_name: The name of the model. Use "<schema>_<table>" format.
        db_connection: Used to query Db (Instance of SqlQuery)
        _fields_list: List of attributes of type Field.

    """

    def __init__(
        self,
        model_name,
        db_connection=None,
        channel=None,
        model_params_dict=None,
    ):
        self.model_name = model_name
        self.db_connection = db_connection
        self.channel = channel
        self._params = model_params_dict
        self._fields_list = None
        self.set_fields()

    def get_params_json_file_path(self):
        app_home = os.environ["APPLICATION_HOME"]
        return os.path.realpath(
            os.path.join(app_home, "configs", self.channel, "models.json")
        )

    @property
    def params(self):
        """Property to access model params stored in json file. # noqa: E501
        Each time the property is accessed, the json file is read. Normally, this happens only ont time per task

        TODO: Currently, source file is hardcoded.


        Returns: dict
            A dict with params

        Raises:
            IOError: An error occurred accessing the smalltable.

        """

        # TODO: Add cache here because retrieved each time Model is instantiated. # noqa: E501
        if self._params is None:
            """Get task params from Json file. Store as property for reuse."""
            with open(self.get_params_json_file_path(), "r") as f:
                f = json.load(f)
                if len(f) == 0:
                    return None
                for model_definition in f:
                    if self.model_name in model_definition:
                        return model_definition[self.model_name]
        else:
            return self._params

    @property
    def fields_list(self):
        """Property to the field list of the model.

        Returns:
            A dict with params

        Raises:
            IOError: An error occurred accessing the smalltable.

        TODO: Currently, source file is hardcoded.
        """
        if not self._fields_list:
            res = []
            for i in vars(self).values():
                if isinstance(i, Field):
                    res.append(i)
            self._fields_list = res
            return res
        else:
            return self._fields_list

    @property
    def fields_name_list(self):
        return [m.name for m in self.fields_list]

    def set_field(self, name, params):
        """Set the attribute of type Field to self (Model).

        Args:
            name: The name of the attribute
            params: Params for creating the Field instance

        Raises:
            IOError: An error occurred accessing the smalltable.

        """
        field = Field(name, params, self)
        setattr(self, name, field)

    def set_fields(self):
        """Call set field pour each entry in jsom for the model."""
        for k, v in self.params.items():
            self.set_field(k, v)

    def populate_values(self, source_values):
        """Set value attribute toall  model attributes of type Field

        Args:
            source_values: dict
            The source API datas

        """
        for f in self.fields_list:
            if f.type == "constant":
                f.value = f.field_params["value"]
                continue
            v = nested_get(source_values, f.source_path)
            f.value = v

    def get_db_values_tuple(self):
        """Construct a tuple of fields db_value"""
        result = []
        for f in self.get_db_fields_list():
            result.append(f.db_value)
        return tuple(result)

    def get_db_fields_list(self):
        """Get a list of fields for which we will store value in db."""
        result = []
        for f in self.fields_list:
            if getattr(f, "exclude", False):
                continue
            result.append(f)
        return result

    def get_all(self, fields=None, filter_field=None):
        """Get all record for a table corresponding to the current model. # noqa: E501

        Args:
            fields: A list of fields for filtering request result fields.

        Returns:
            A list of tuples. each tuple representing a record of the query result.

        """
        q = SqlQuery(
            self.db_connection,
            "select",
            model=self,
            fields=fields,
            filter_field=filter_field,
        )
        res = q.run()
        return res

    def get_fields_value_as_string(self, fields):
        return "".join([str(getattr(self, f).db_value) for f in fields])

    def get_max_for_date_field_plus_one_day(self, field):
        """Get max value for a a date field and add one day.

        Args:
            field: The field we want the max vamlue for.

        Returns:
            A list of tuple with one tuple containing the max value.

        """
        q = SqlQuery(
            self.db_connection,
            "select_max_for_date_plus_one_day",
            max_field=field,
            model=self,
        )
        res = q.run()
        return res

    def get_max_for_field(self, field):
        """Get max value for a specific field.

        Args:
            field: The field we want the max vamlue for.

        Returns:
            A list of tuple with one tuple containing the max value.

        """
        q = SqlQuery(
            self.db_connection, "select_max", max_field=field, model=self
        )  # noqa: E501
        res = q.run()
        return res

    @staticmethod
    def get_from_raw_sql(db_connection, sql):
        """Allow to query the Db with raw sql. # noqa: E501

        Mainly used for retrieving from teh db values used to build the request endpoint.

        Args:
            db_connection:
            sql: A string repsenting the sql query.

        Returns:
            A list of tuple with one tuple containing the max value.

        """
        q = SqlQuery(db_connection, "get_from_raw_sql", raw_sql=sql)
        res = q.run()
        return res

    @staticmethod
    def run_raw_sql(db_connection, sql):
        """Allow to query the Db with raw sql. # noqa: E501

        Mainly used for retrieving from teh db values used to build the request endpoint.

        Args:
            db_connection:
            sql: A string repsenting the sql query.

        Returns:
            A list of tuple with one tuple containing the max value.

        """
        q = SqlQuery(db_connection, "raw_sql", raw_sql=sql)
        q.run()


class Field:
    """# noqa: E501
    Class used:
        - to store individual value retrieved from source,
        - to transform collected values to the adapted format for the Db.



        The set_attributes_from_params().

    Attributes:
        ## Attributes
        db_value: A property representing the value to be store in the Db.

        ## Attributes set by passing values at instanciation time.
        field_params: parameters rtrieved from model.json.
        model: The Model class
        value: The value retrieved from source API

        ## Attributes retrieved from from model.json.
        source_path: Contains a list of keys used to find the data in the source result dict.
        type: Define how to transform the retrieved sour value for inserting in Db. Can have one of the following values:
            - raw: The data collected from source will be insert as it ios in the Db. No tranformation.
            - exclude: This value will not be stored in the Db.
            - function: Specify that the data collected from source will be transformed by "transform_function".
            - constant: The db_value used for this field is set in model.json
            - composite: Used when the value to be store in Db is a composition of values retrieved from Db.
                         Use "composite_fields" and "composite_pattern" attributes (See below).
        - composite fields: A dict. The key is the name of the model Field attribute containing value to aggregate. The value is the name of the placeholder in the "composite_pattern" attribute
        - composite_pattern: A string representing the model used to format the composite field. Used with the python format() pattern.
        - transform_function: A dict with the following keys:
            - type: The name of the transformation to apply.Can have one of the following values:
                - lstrip: See t_lstrip()
                - datetime_from_timestamp_in_milliseconds: See t_datetime_from_timestamp_in_milliseconds()
                - t_split: See t_split()
            - string_to_strip: Used by t_lstrip(). String to be removed from the value retrieved from source API.
            - split_position: Define the position of the value to be returned by t_split()
            - split_caracter: Split caracter used by t_strip()
    """

    def __init__(self, field_name, field_params, model):
        self.name = field_name
        self.field_params = field_params
        self.model = model
        self.set_attributes_from_params()
        self.value = None

    def set_attributes_from_params(self):
        """Set attributes from the model.json file."""
        if self.field_params:
            for k, v in self.field_params.items():
                setattr(self, k, v)

    @staticmethod
    def t_lstrip(pattern, value):
        """Transform a string using the lstrip function

        Args:
            pattern: str
                String to be stripped from the value
            value: str
                The string being stripped.

        Returns:
            result: str
                The value stripped

        """
        if not isinstance(value, str):
            result = ""

        else:
            result = value.lstrip(pattern)

        return result

    @staticmethod
    def t_milliseconds_to_datetime(value):
        """Transform an int representing a date in milliseconds to a datetime object.

        Args:
            value: An int.

        Returns:
            result: A datetime object

        """
        result = datetime.fromtimestamp(
            value / 1000.0 if isinstance(value, int) else int(value) / 1000.0
        )

        return result

    @staticmethod
    def t_split(value, caracter, position):
        """# Noqa :E501
            Used when the API returned values need to be split in separate values corresponding to the model table.
           Used for the LinkedIn "County" aggreation field which returns a string like "Country, Region, City".
           In the Db, they are 3 corresponding columns: "country", "region", "city".

           IMPORTANT: Sometimes the retrieved values contains only two fields: "country" and "city".
           This can affect data accuracy. I have no simple solution at the moment.

        Args:
            value:
            caracter:
            position:

        Returns:
            result: A string. can be empty.

        Raises:
            IndexError: Raise if split function fail. This could be the case when we try to retrieve at position x when split result doesn't contains enough elements.
                        In this case, we return an empty string


        """
        try:
            result = value.split(caracter)
        except IndexError as e:
            logger.info(
                f"Error while splitting '{value}' with caracter {caracter} at position"  # noqa: E501
                f" {position}"
            )
            logger.info(e)
            return ""

        # Sometimes Bing geo API returns only 2 locations instead of 3.
        # This is an issue because we don't know which location is  not returned. # noqa: E501
        # For example, if "Belgrad, Serbia" is returned, it looks like region has been omitted. # noqa: E501
        try:
            result = result[position].strip()
        except IndexError:
            result = ""

        return result

    @property
    def db_value(self, model=None):
        """# Noqa :E501
           Retrieve the value to store in Db applying the "type" tranform to the "value" attribute

        Args:
            model: The model the field belongs too.

        Returns:
            result: A sql escaped string.

        """
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
                result = self.t_lstrip(
                    self.transform_function["string_to_strip"], self.value
                )
            if (
                self.transform_function["type"]
                == "datetime_from_timestamp_in_milliseconds"
            ):
                result = self.t_milliseconds_to_datetime(self.value)
            if self.transform_function["type"] == "split":
                result = self.t_split(
                    self.value,
                    self.transform_function["split_caracter"],
                    self.transform_function["split_position"],
                )

        return self.get_sql_escaped(result)

    def get_sql_escaped(self, value):
        """Could be "'" caracter in values to insert in Db. Need to be double in this case. # noqa: E501

        Args:
            value: The value to escape

        Returns:
            result: A sql escaped string.

        """
        result = value
        return result.replace("'", "''") if isinstance(result, str) else result
