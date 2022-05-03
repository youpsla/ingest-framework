import json
import logging
import os

from src.client_helper import get_client
from src.utils.sql_utils import SqlQuery

from .model import Model

logger = logging.getLogger(__name__)


class Task:
    """This class manage lots of things. Some of them are not directly connected to linkedin and should be extract to a Client class.
    TODO: Create Client class

    - Retrieving task parameters from json file
    - Creating the url to request and all associated operation (requesting Db for filter values, building args and kwargs, )
    - Retrieving request result from source
    """

    _source = None
    _destination = None

    def __init__(self, channel, name, running_env) -> None:
        self.channel = channel
        self.name = name
        self.running_env = running_env
        self._params = None

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
    def params(self):
        if self._params is None:
            __location__ = os.path.realpath(
                os.path.join(os.getcwd(), os.path.dirname(__file__), "../", "tasks")
            )
            with open(os.path.join(__location__, f"{self.channel}.json"), "r") as f:
                f = json.load(f)
                if len(f) == 0:
                    return None
            for task_definition in f:
                if self.name in task_definition:
                    self._params = task_definition[self.name]
                    return task_definition[self.name]
        else:
            return self._params

    @property
    def model(self):
        return Model(
            self.params["model"], db_engine=self.destination, channel=self.channel
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
            print(f"No available params for task {self.name}. Running next task.")
            return "error"

        # TODO: Manage result returned by get() method. Remove "runniong next task as we raise error now for interrupting workflow"
        # if len(result) == 0:
        #     print(f"{self.name}: No new datas from source. Running next task.")
        #     return "success"

        if not result:
            print(
                f"{self.name}: Failed retrieving datas from source. Skipping."
                " Running next task."
            )
            return "error"

        return result

    def run(self):
        # try:
        print(f"Starting task: {self.name}")
        # with open("fixtures.json", "r") as f:
        #     fix = json.load(f)
        #     datas_from_source = fix["social_metrics"]["elements"]

        # Retrieving datas from source
        source_data = self.get_data_from_source()

        if "download" in self.actions:
            model = Model(self.model.model_name, self.destination, self.channel)
            self.source.params = self.params
            result = self.source.submit_and_download()

        # Insert datas in destination
        if "insert" in self.actions:
            datas_obj = []
            for d in source_data:
                elem = d["datas"]
                if elem is not None:
                    m = Model(
                        self.model.model_name, self.destination, channel=self.channel
                    )
                    m.populate_values(elem)
                    datas_obj.append(m)
            datas_values = []
            # Search for new records and insert them.
            if self.params["exclude_existing_in_db"]:
                existing_ids = [
                    str(r[self.params["exclude_key"]]) for r in self.model.get_all()
                ]
                datas_obj = [
                    r
                    for r in datas_obj
                    if getattr(
                        r, self.params["destination_unique_key"]["value"]
                    ).db_value
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
                        self.model.model_name, self.destination, channel=self.channel
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
            self.destination,
            "insert",
            fields=[f.name for f in self.model.get_db_fields_list()],
            values=data,
            model=self.model,
        )
        sql_query.run()

    def update(self, values_dicts_list, update_key):
        sql_query = SqlQuery(
            self.destination,
            "update",
            values=values_dicts_list,
            model=self.model,
            update_key=update_key,
        )
        sql_query.run()
