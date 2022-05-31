import os

from psycopg2 import ProgrammingError, extras
from psycopg2.extras import RealDictCursor
from src.utils.custom_logger import logger
from src.utils.various_utils import get_schema_name


class SqlQuery:
    def __init__(
        self,
        db_connection,
        qtype,
        fields=None,
        values=None,
        model=None,
        max_field=None,
        raw_sql=None,
        where=None,
        update_key=None,
    ):
        self.qtype = qtype
        self.fields = fields
        self.values = values
        self.db_connection = db_connection
        self.model = model
        self.max_field = max_field
        self._raw_sql = raw_sql
        self.sql = None
        self.where = where
        self.update_key = update_key
        self._values_list = None

    @property
    def schema_name(self):
        channel = os.environ["INGEST_CURRENT_CHANNEL"]
        return get_schema_name(channel)

    @property
    def raw_sql(self):
        try:
            result = self._raw_sql.format(schema=self.schema_name)
        except Exception:
            result = self._raw_sql
        return result

    @property
    def schema_table(self):
        return "{}.{}".format(self.schema_name, self.model.model_name)

    @property
    def stage_table_name(self):
        return "{}.{}".format(self.schema_name, "staging")

    @property
    def values_list(self):
        if not self._values_list:
            self._values_list = [[i for i in v.values()] for v in self.values]
        return self._values_list

    def copy_from_s3(self):
        pass

    def run(self):

        # write_results_db_connection = self.destination.write_results_db_connection
        # write_cur = write_results_db_connection.cursor()

        # db_connection = self.destination.db_connection
        # cursor = db_connection.cursor()
        with self.db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
            try:
                if self.qtype == "select":
                    self.sql = self.get_sql_select()
                    cursor.execute(self.sql)
                elif self.qtype == "insert":
                    self.sql = self.get_sql_insert(self.schema_table)
                    extras.execute_values(
                        cursor,
                        self.sql,
                        self.values,
                    )
                elif self.qtype == "select_max":
                    self.sql = self.get_sql_select_max()
                    cursor.execute(self.sql, self.values)
                elif self.qtype == "raw_sql":
                    cursor.execute(self.raw_sql)
                    return cursor.fetchall()
                elif self.qtype == "write_raw_sql":
                    cursor.execute(self.raw_sql)
                elif self.qtype == "update":
                    # 1) Creation of the tmp(stage) table
                    create_sql = self.create_tmp_stage_table()
                    cursor.execute(create_sql)
                    # 2) Insert self.values in tmp table
                    sql_insert = self.get_sql_insert(self.stage_table_name)
                    extras.execute_values(
                        cursor,
                        sql_insert,
                        self.values_list,
                    )
                    # 3) Do the update
                    self.sql = self.get_sql_update()
                    cursor.execute(self.sql)
                elif self.qtype == "partial_update":
                    self.sql = self.get_sql_partial_update()
                    cursor.execute(self.sql)

                result = []
                try:
                    result = cursor.fetchall()
                except ProgrammingError:
                    pass

            except Exception as e:
                logger.error(
                    f"Issue while executing the following sql query:\n{self.sql}.\nThe"
                    f" following error occur: {e}"
                )
                raise RuntimeError
            return result

    def get_sql_select(self):
        sql = (
            f"SELECT {'*' if not self.fields else ','.join(self.fields)}"
            f" from {self.schema_table}"
            f" {'where '+self.where if self.where else ''}"
        )
        return sql

    def get_sql_select_max(self):
        sql = f"SELECT MAX({self.max_field}) FROM {self.schema_table}"
        return sql

    def get_sql_insert(self, schema_table):
        sql = """INSERT INTO {} ({}) VALUES %s"""
        sql = sql.format(
            schema_table, ", ".join([f.name for f in self.model.get_db_fields_list()])
        )
        return sql

    def create_tmp_stage_table(self):
        sql_create_tmp_table = (
            "DROP TABLE IF EXISTS {stage_table_name}; create table IF NOT EXISTS {stage_table_name}"
            " (like {schema_table});".format(
                schema_table=self.schema_table,
                stage_table_name=self.stage_table_name,
            )
        )
        return sql_create_tmp_table

    def get_sql_update(self):
        set_fields = [f.name for f in self.model.fields_list]
        set_fields.remove(self.update_key)

        def update_get_set(self):
            set_data = " , ".join(
                [
                    "=".join(
                        (
                            "{}".format(str(f)),
                            "{stage_table_name}.{table_name}".format(
                                stage_table_name=self.stage_table_name,
                                table_name=f,
                            ),
                        )
                    )
                    for f in set_fields
                ]
            )
            return set_data

        def update_get_where_primary_key(self, target):
            result = "{target}.{pk} = {stage_table_name}.{pk}".format(
                target=target,
                stage_table_name=self.stage_table_name,
                pk=self.update_key,
            )
            return result

        def update_get_and_where(self):
            comparaison_fields = list(self.values[0].keys())
            comparaison_fields.remove(self.update_key)
            where_data = " or ".join(
                [
                    " != ".join(
                        (
                            "{}.{}".format(self.model.model_name, str(f)),
                            "{}.{}".format(self.stage_table_name, str(f)),
                        )
                    )
                    for f in comparaison_fields
                ]
            )
            return where_data

        target = self.model.model_name

        sql = (
            "update {schema_table} set {set_part} from {stage_table_name} where"
            " {where_primary_key} and ({and_where})".format(
                schema_table=self.schema_table,
                set_part=update_get_set(self),
                stage_table_name=self.stage_table_name,
                where_primary_key=update_get_where_primary_key(self, target),
                and_where=update_get_and_where(self),
            )
        )
        return sql

    def get_sql_partial_update(self):
        sql = """UPDATE {} SET {} WHERE {}"""

        where = " ".join([f" {k}='{v}'" for d in self.where for k, v in d.items()])

        # set_data = ' , '.join(['='.join((str(a[0]),str(a[1]))) for a in zip(fields,values)]) # noqa: E501
        set_data = " , ".join(
            [
                "=".join((str(k), "'{}'".format(v.replace("'", "''"))))
                for d in self.values
                for k, v in d.items()
            ]
        )
        sql = sql.format(self.schema_table, set_data, where)
        return sql
