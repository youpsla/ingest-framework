import csv
import json
import os
import time

from src.clients.bing.bing_client import ReportManager

# from src.clients.bing.bing_client import ReportManager
from src.commons.client_helper import get_client
from src.commons.model import Model
from src.utils.custom_logger import logger
from src.utils.sql_utils import SqlQuery
from src.utils.various_utils import (
    get_model_params_as_dict,
    get_running_env,
    nested_get,
)


class Task:
    """
    # noqa: E501
    # This class manage lots of things. Some of them are not directly connected to linkedin and should be extract to a Client class.
    TODO: Create Client class

    - Retrieving task parameters from json file
    - Creating the url to request and all associated operation (requesting Db for filter values, building args and kwargs, )
    - Retrieving request result from source
    """

    _source = None
    _destination = None

    def __init__(self, channel, name, db_connection) -> None:
        self.channel = channel
        self.name = name
        self.running_env = get_running_env()
        self.db_connection = db_connection
        self._params = None
        self._request_data_source = None
        self._source = None

    @property
    def source(self):
        if not self._source:
            source_name = self.params.get("source")
            if source_name:
                self._source = get_client(
                    self.running_env,
                    source_name,
                    self,
                    db_connection=self.db_connection,
                )
        return self._source

    @property
    def destination(self):
        if not self._destination:
            destination_name = self.params["destination"]
            self._destination = get_client(
                self.running_env, destination_name, self
            )  # noqa: E501
        return self._destination

    @property
    def request_data_source(self):
        if not self._request_data_source:
            request_data_source_name = self.params.get(
                "request_data_source", None
            )  # noqa: E501
            self._request_data_source = get_client(
                self.running_env, request_data_source_name, self
            )
        return self._request_data_source

    def get_params_json_file_path(self):
        app_home = os.environ["APPLICATION_HOME"]
        return os.path.realpath(
            os.path.join(app_home, "configs", self.channel, "tasks.json")
        )

    @property
    def params(self):
        """Get task params from Json file. Store as property for reuse."""
        if self._params is None:
            with open(self.get_params_json_file_path(), "r") as f:
                f = json.load(f)
                if len(f) == 0:
                    return None
            for task_definition in f:
                if self.name in task_definition:
                    self._params = task_definition[self.name]
                    return self._params
        else:
            return self._params

    @property
    def model(self):
        return Model(
            self.params["model"],
            db_connection=self.db_connection,
            channel=self.channel,
        )

    @property
    def actions(self):
        return self.params["actions"]

    def get_data_from_source(self):
        """Retrieve data by calling source API"""

        result = None

        if self.params is not None:
            # TODO: Remove header as it is not used by bing. Pass other params as group. # noqa: E501
            result = self.source.get(
                task_params=self.params,
            )

        else:
            raise RuntimeError(f"No available params for task {self.name}.")

        if not result:
            logger.info(
                f"{self.name}: No data from source." " Running next task."
            )  # noqa: E501

        return result

    def run(self):
        logger.info(f"Channel: {self.channel} - Task: {self.name} - Start")
        if "copy" not in self.actions:
            if "raw_sql" not in self.actions:
                source_data = self.get_data_from_source()

        if "download" in self.actions:
            for d in source_data:
                elem = d["datas"]
                result_file_path = ReportManager(
                    authorization_data=d["authorization_data"],
                    report_request=elem,
                ).submit_and_download()
            source_data = []
            with open(result_file_path, newline="", encoding="utf-8-sig") as csv_file:
                tmp = csv.DictReader(csv_file)
                for t in tmp:
                    source_data.append({"datas": t})

        if "transfer" in self.actions:
            self.destination.upload_and_delete_source()

        if "copy" in self.actions:
            self.source.copy_to_redshift_and_delete()

        # Insert datas in destination
        if "insert" in self.actions:
            data_objs = self.get_data_objs(source_data)
            datas_values = []
            # Search for new records and insert them.
            if data_objs:
                if self.params.get("exclude_existing_in_db"):
                    # if getattr(self.params, "exclude_existing_in_db", None):
                    existing_ids = [
                        "".join(
                            str(r[e])
                            for e in [
                                d
                                for d in self.params[
                                    "destination_unique_keys"
                                ]  # noqa: E501
                            ]  # noqa: E501
                        )
                        for r in self.model.get_all()
                    ]
                    data_objs = [
                        r
                        for r in data_objs
                        if r.get_fields_value_as_string(
                            self.params["destination_unique_keys"]
                        )
                        not in existing_ids
                    ]
                datas_values = [r.get_db_values_tuple() for r in data_objs]

                logger.info(
                    f"{self.name} - {self.model.model_name}:"
                    f" {len(datas_values)} record(s) will be inserted"
                )
                self.insert(datas_values)

        if "update" in self.actions:
            data_objs = self.get_data_objs(source_data)

            values_dicts_list = [
                {f.name: f.db_value for f in obj.get_db_fields_list()}
                for obj in data_objs  # noqa: E501
            ]

            # TODO: Manage the case when no quries are done to source because of prefiltering from DB. This is the case for sponsored_video_update # noqa: E501
            if values_dicts_list:
                self.update(values_dicts_list, self.params["update_keys"])

        if "partial_update" in self.actions:
            for d in source_data:
                # m = Model(
                #     self.model.model_name,
                #     self.db_connection,
                #     channel=self.channel,  # noqa: E501
                # )
                # m.set_field(
                #     self.params["db_query"]["fields"][0],
                #     m.params[self.params["db_query"]["fields"][0]],
                # )
                # m.set_field(
                #     d["where_field"],
                #     m.params[d["where_field"]],
                # )
                # m.populate_values(d["datas"])
                # setattr(getattr(m, d["where_field"]), "value", d["where_value"]) # noqa: E501

                where_dicts_list = []
                for v in self.params["db_query"]["keys"]:
                    where_dicts_list.append(
                        {
                            v["table_field_name_used_as_key"]: nested_get(
                                d, v["api_result_path"]
                            )
                        }
                    )

                values_dicts_list = []
                for v in self.params["db_query"]["fields"]:
                    values_dicts_list.append(
                        {v["table_field_name"]: nested_get(d, v["api_result_path"])}
                    )

                self.partial_update(
                    values_dicts_list,
                    where_dicts_list=where_dicts_list,
                )

        if "raw_sql" in self.actions:
            try:
                query = self.params["raw_sql_to_run"]
            except KeyError:
                raise (
                    "A task with raw_sql in actions must have a raw_sql_to_run key"  # noqa: E501
                )  # noqa: E501

            Model.run_raw_sql(db_connection=self.db_connection, sql=query)

        return True, self.destination

    def get_data_objs(self, source_data):
        data_objs = []
        start = time.time()
        model_params = get_model_params_as_dict(
            self.channel, self.params["model"]
        )  # noqa: E501
        for elem in source_data:
            if elem is not None:
                m = Model(
                    self.params["model"],
                    db_connection=self.db_connection,
                    channel=self.channel,
                    model_params_dict=model_params,
                )
                m.populate_values(elem)
                data_objs.append(m)
        end = time.time()

        logger.debug(f"{self.name}: Generation of data_objs took: {end-start}")
        return data_objs

    def insert(self, data):

        sql_query = SqlQuery(
            self.db_connection,
            "insert",
            fields=[f.name for f in self.model.get_db_fields_list()],
            values=data,
            model=self.model,
        )
        sql_query.run()

    def partial_update(self, values_dicts_list, where_dicts_list=None):
        sql_query = SqlQuery(
            self.db_connection,
            "partial_update",
            fields=[f["table_field_name"] for f in self.params["db_query"]["fields"]],
            values=values_dicts_list,
            model=self.model,
            where=where_dicts_list,
        )
        sql_query.run()

    def update(self, values_dicts_list, update_keys):
        sql_query = SqlQuery(
            self.db_connection,
            "update",
            values=values_dicts_list,
            model=self.model,
            update_keys=update_keys,
        )
        sql_query.run()
