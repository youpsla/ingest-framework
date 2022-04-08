import json
import logging
import os

from models import Model
from sql_utils import SqlQuery

logger = logging.getLogger(__name__)


class Task:
    def __init__(self, name, source, destination) -> None:
        self.name = name
        self.source = source
        self.destination = destination
        self._params = None

    @property
    def params(self):
        if self._params is None:
            __location__ = os.path.realpath(
                os.path.join(os.getcwd(), os.path.dirname(__file__))
            )
            with open(os.path.join(__location__, "tasks.json"), "r") as f:
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
        return Model(self.params["model"], db_engine=self.destination)

    @property
    def actions(self):
        return self.params["actions"]

    def run(self):
        try:
            print(f"Starting task: {self.name}")
            # with open("fixtures.json", "r") as f:
            #     fix = json.load(f)
            #     datas_from_source = fix["social_metrics"]["elements"]

            # Retrieving datas from source
            if self.params is not None:
                datas_from_source = self.source.get(
                    task_params=self.params,
                    header=self.params["request"]["header"],
                )
            else:
                print(f"No available params for task {self.name}. Running next task.")
                return "error"

            if len(datas_from_source) == 0:
                print(f"{self.name}: No new datas from source. Running next task.")
                return "success"

            if not datas_from_source:
                print(
                    f"{self.name}: Failed retrieving datas from source. Skipping."
                    " Running next task."
                )
                return "error"

            # Insert datas in destination
            if "insert" in self.actions:
                datas_obj = []
                for d in datas_from_source:
                    elem = d["datas"]
                    if elem is not None:
                        m = Model(self.model.model_name, self.destination)
                        m.populate_values(elem)
                        datas_obj.append(m)
                        # del m
                datas_values = []
                # Search for new records and insert them.
                if self.params["exclude_existing_in_db"]:
                    dede = self.model.get_all()
                    existing_ids = [
                        r[self.params["exclude_key"]] for r in self.model.get_all()
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

                print(
                    f"{self.name} - {self.model.model_name}:"
                    f" {len(datas_values)} record(s) will be inserted"
                )
                self.insert(datas_values)

            if "update" in self.actions:
                datas_obj = []
                for d in datas_from_source:
                    if d["datas"] is not None:
                        m = Model(self.model.model_name, self.destination)
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

                        where_dicts_list = []
                        for v in self.params["db_query"]["keys"]:
                            where_dicts_list.append({v: d["datas"][v]})

                        values_dicts_list = []
                        for v in self.params["db_query"]["fields"]:
                            values_dicts_list.append({v[0]: d["datas"][v[1]]})

                        self.update(
                            values_dicts_list,
                            where_dicts_list=where_dicts_list,
                        )
                        del m
            return "success"
        except Exception as e:
            logger.error(
                f"ERROR occured while running task {self.name}:\n{e}\n\n Cancel all"
                " tasks runned so far.\nRollback Db transaction.\n## ENDING LAMBDA"
            )
            self.destination.write_results_db_connection.rollback()

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

    def update(self, values_dicts_list, where_dicts_list=None):
        sql_query = SqlQuery(
            self.destination,
            "update",
            fields=self.params["db_query"]["fields"],
            values=values_dicts_list,
            model=self.model,
            where=where_dicts_list,
        )
        sql_query.run()
