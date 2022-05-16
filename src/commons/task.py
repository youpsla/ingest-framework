import json
import logging
import os

from src.clients.bing.bing_client import ReportManager

# from src.clients.bing.bing_client import ReportManager
from src.commons.client_helper import get_client
from src.commons.model import Model
from src.utils.custom_logger import logger
from src.utils.sql_utils import SqlQuery


class Task:
    """This class manage lots of things. Some of them are not directly connected to linkedin and should be extract to a Client class.
    TODO: Create Client class

    - Retrieving task parameters from json file
    - Creating the url to request and all associated operation (requesting Db for filter values, building args and kwargs, )
    - Retrieving request result from source
    """

    _source = None
    _destination = None

    def __init__(self, channel, name, running_env, db_connection) -> None:
        self.channel = channel
        self.name = name
        self.running_env = running_env
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
            self._destination = get_client(
                self.running_env, destination_name, self
            )
        return self._destination

    @property
    def request_data_source(self):
        if not self._request_data_source:
            request_data_source_name = self.params.get(
                "request_data_source", None
            )
            self._request_data_source = get_client(
                self.running_env, request_data_source_name, self
            )
        return self._request_data_source

    @property
    def params(self):
        if self._params is None:
            __location__ = os.path.realpath(
                os.path.join(
                    os.getcwd(), os.path.dirname(__file__), "../../", "tasks"
                )
            )
            with open(
                os.path.join(__location__, f"{self.channel}.json"), "r"
            ) as f:
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
            logger.info(
                f"{self.name}: No data from source." " Running next task."
            )

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
            datas_obj = []
            for d in source_data:
                elem = d["datas"]
                if elem is not None:
                    m = Model(
                        self.model.model_name,
                        db_connection=self.db_connection,
                        channel=self.channel,
                    )
                    m.populate_values(elem)
                    datas_obj.append(m)
            datas_values = []
            # Search for new records and insert them.
            if self.params["exclude_existing_in_db"]:
                # existing_ids = [
                #     str(r[self.params["exclude_key"]]) for r in self.model.get_all()
                # ]
                existing_ids = [
                    "".join(
                        str(r[e])
                        for e in [
                            d for d in self.params["destination_unique_keys"]
                        ]
                    )
                    for r in self.model.get_all()
                ]

                # api_ids = [
                #     "".join([str(r[e]) for e in self.params['exclude_keys']) for r in api_data
                # ]
                # existing_ids = [
                #     str(r[e]) for r in self.model.get_all() for e in self.params['exclude_keys']
                # ]
                datas_obj = [
                    r
                    for r in datas_obj
                    if r.get_fields_value_as_string(
                        self.params["destination_unique_keys"]
                    )
                    not in existing_ids
                ]
                datas_values = [r.get_db_values_tuple() for r in datas_obj]
            else:
                datas_values = [r.get_db_values_tuple() for r in datas_obj]

            logger.info(
                f"{self.name} - {self.model.model_name}:"
                f" {len(datas_values)} record(s) will be inserted"
            )
            self.insert(datas_values)

        if "update" in self.actions:
            datas_obj = []
            for d in source_data:
                if d["datas"] is not None:
                    m = Model(
                        self.model.model_name,
                        db_connection=self.db_connection,
                        channel=self.channel,
                    )
                    # m.set_field(
                    #     self.params["db_query"]["fields"][0],
                    #     m.params[self.params["db_query"]["fields"][0]],
                    # )
                    # m.set_field(
                    #     d["where_field"],
                    #     m.params[d["where_field"]],
                    # )
                    m.set_fields()
                    m.populate_values(d["datas"])
                    # setattr(getattr(m, d["where_field"]), "value", d["where_value"])
                    datas_obj.append(m)

            data_values = [r.get_db_values_tuple() for r in datas_obj]

            # where_dicts_list = []
            # for v in self.params["db_query"]["keys"]:
            #     where_dicts_list.append({v: d["datas"][v]})

            model_fields_list = self.model.fields_list
            values_dicts_list = [
                {model_fields_list[values.index(v)].name: v for v in values}
                for values in data_values
            ]

            self.update(values_dicts_list, self.params["update_key"])
        return True, self.destination
        # except Exception as e:
        #     logger.error(
        #         f"ERROR occured while running task {self.name}:\n{e}\n\n Cancel all"
        #         " tasks runned so far.\nRollback Db transaction.\n## ENDING LAMBDA"
        #     )
        #     self.destination.rollback()
        #     raise Exception(
        #         "Current running task has failed. All write operation(s) have been"
        #         " rollbacked."
        #     )
        # TODO: Update linkedin client to implemùent rollback method

        return "error"

    def insert(self, data):
        sql_query = SqlQuery(
            self.db_connection,
            "insert",
            fields=[f.name for f in self.model.get_db_fields_list()],
            values=data,
            model=self.model,
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
