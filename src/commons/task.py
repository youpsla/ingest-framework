import json
import logging
import os

from src.clients.bing.bing_client import ReportManager

# from src.clients.bing.bing_client import ReportManager
from src.commons.client_helper import get_client
from src.commons.model import Model
from src.utils.custom_logger import logger
from src.utils.sql_utils import SqlQuery
from src.utils.various_utils import get_running_env


class Task:
    """This class manage lots of things. Some of them are not directly connected to linkedin and should be extract to a Client class.
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

    @property
    def source(self):
        if not self._source:
            source_name = self.params["source"]
            self._source = get_client(self.running_env, source_name, self)
        return self._source

    @property
    def destination(self):
        if not self._destination:
            destination_name = self.params["destination"]
            self._destination = get_client(self.running_env, destination_name, self)
        return self._destination

    @property
    def request_data_source(self):
        if not self._request_data_source:
            request_data_source_name = self.params.get("request_data_source", None)
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
            # TODO: Remove header as it is not used by bing. Pass other params as group.
            result = self.source.get(
                task_params=self.params,
            )

        else:
            raise RuntimeError(
                f"No available params for task {self.name}. Running next task."
            )

        if not result:
            logger.info(f"{self.name}: No data from source." " Running next task.")

        return result

    def run(self):
        # try:
        logger.info(f"Channel: {self.channel} - Task: {self.name} - Start")
        # with open("fixtures.json", "r") as f:
        #     fix = json.load(f)
        #     datas_from_source = fix["social_metrics"]["elements"]

        # Retrieving datas from source
        if "copy" not in self.actions:
            source_data = self.get_data_from_source()

        if "download" in self.actions:
            for d in source_data:
                elem = d["datas"]
                ReportManager(
                    authorization_data=d["authorization_data"],
                    report_request=elem,
                ).submit_and_download()

        if "transfer" in self.actions:
            self.destination.upload_and_delete_source()

        if "copy" in self.actions:
            self.source.copy_to_redshift_and_delete()

        # Insert datas in destination
        if "insert" in self.actions:
            data_objs = self.get_data_objs(source_data)
            datas_values = []
            # Search for new records and insert them.
            if self.params["exclude_existing_in_db"]:

                existing_ids = [
                    "".join(
                        str(r[e])
                        for e in [d for d in self.params["destination_unique_keys"]]
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
            else:
                datas_values = [r.get_db_values_tuple() for r in data_objs]

            logger.info(
                f"{self.name} - {self.model.model_name}:"
                f" {len(datas_values)} record(s) will be inserted"
            )
            self.insert(datas_values)

        if "update" in self.actions:
            data_objs = self.get_data_objs(source_data)

            values_dicts_list = [
                {f.name: f.db_value for f in obj.fields_list} for obj in data_objs
            ]

            # TODO: Manage the case when no quries are done to source because of prefiltering from DB. This is the case for sponsored_video_update
            if values_dicts_list:
                self.update(values_dicts_list, self.params["update_key"])
            else:
                pass

        if "partial_update" in self.actions:
            for d in source_data:
                if d["datas"] is not None:
                    m = Model(
                        self.model.model_name, self.db_connection, channel=self.channel
                    )
                    # m.set_field(
                    #     self.params["db_query"]["fields"][0],
                    #     m.params[self.params["db_query"]["fields"][0]],
                    # )
                    # m.set_field(
                    #     d["where_field"],
                    #     m.params[d["where_field"]],
                    # )
                    m.populate_values(d["datas"])
                    # setattr(getattr(m, d["where_field"]), "value", d["where_value"])

                    where_dicts_list = []
                    for v in self.params["db_query"]["keys"]:
                        where_dicts_list.append({v: d["datas"][v]})

                    values_dicts_list = []
                    for v in self.params["db_query"]["fields"]:
                        values_dicts_list.append({v[0]: d["datas"][v[1]]})

                    self.partial_update(
                        values_dicts_list,
                        where_dicts_list=where_dicts_list,
                    )

        return True, self.destination

    def get_data_objs(self, source_data):
        data_objs = []
        for d in source_data:
            elem = d["datas"]
            if elem is not None:
                m = Model(
                    self.model.model_name,
                    db_connection=self.db_connection,
                    channel=self.channel,
                )
                m.populate_values(elem)
                data_objs.append(m)
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
            fields=self.params["db_query"]["fields"],
            values=values_dicts_list,
            model=self.model,
            where=where_dicts_list,
        )
        sql_query.run()

    def update(self, values_dicts_list, update_key):
        sql_query = SqlQuery(
            self.db_connection,
            "update",
            values=values_dicts_list,
            model=self.model,
            update_key=update_key,
        )
        sql_query.run()
