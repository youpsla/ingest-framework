import logging

from linkedin.CONFIG import SCHEMA_NAME
from psycopg2 import extras
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class SqlQuery:
    def __init__(
        self,
        destination,
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
        self.destination = destination
        self.model = model
        self.max_field = max_field
        self._raw_sql = raw_sql
        self.sql = None
        self.where = where
        self.update_key = update_key
        self._values_list = None

        write_results_db_connection = self.destination.write_results_db_connection
        self.write_cur = write_results_db_connection.cursor()

    @property
    def raw_sql(self):
        return self._raw_sql.format(schema=SCHEMA_NAME)

    @property
    def schema_table(self):
        return "{}.{}".format(SCHEMA_NAME, self.model.model_name)

    @property
    def stage_table_name(self):
        return "{}.{}".format(SCHEMA_NAME, "staging")

    @property
    def values_list(self):
        if not self._values_list:
            self._values_list = [[i for i in v.values()] for v in self.values]
        return self._values_list

    def run(self):

        write_results_db_connection = self.destination.write_results_db_connection
        write_cur = write_results_db_connection.cursor()

        db_connection = self.destination.db_connection
        # cursor = db_connection.cursor()

        try:
            if self.qtype == "select":
                with db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    self.sql = self.get_sql_select()
                    cursor.execute(self.sql)
                    return cursor.fetchall()
            elif self.qtype == "insert":
                with db_connection.cursor() as cursor:
                    self.sql = self.get_sql_insert(self.schema_table)
                    extras.execute_values(
                        cursor,
                        self.sql,
                        self.values,
                    )
                    db_connection.commit()
            elif self.qtype == "select_max":
                self.sql = self.get_sql_select_max()
                with db_connection.cursor() as cursor:
                    cursor.execute(self.sql, self.values)
                    db_connection.commit()
                return cursor.fetchall()
            elif self.qtype == "raw_sql":
                with db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(self.raw_sql)
                    db_connection.commit()
                    return cursor.fetchall()
            elif self.qtype == "update":
                with db_connection.cursor() as cursor:
                    # 1) Creation of the tmp(stage) table
                    create_sql = self.create_tmp_stage_table()
                    cursor.execute(create_sql)

                    # 2) Insert self.values in tmp table
                    sql_insert = self.get_sql_insert("bing.staging")
                    extras.execute_values(
                        cursor,
                        sql_insert,
                        self.values_list,
                    )
                    db_connection.commit()
                    # 3) Do the update
                    self.sql = self.get_sql_update()
                    cursor.execute(self.sql)

                    db_connection.commit()

        except Exception as e:
            print(e)
            logger.error(
                f"Issue while executing the following sql query:\n{self.sql}.\nThe"
                f" following error occur: {e}"
            )
            return "Error"
        return "Success"

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
            schema_table, ", ".join([f.name for f in self.model.fields_list])
        )
        return sql

    def create_tmp_stage_table(self):
        sql_create_tmp_table = (
            "DROP TABLE bing.staging; create table IF NOT EXISTS bing.staging"
            " (like {schema_table});".format(schema_table=self.schema_table)
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
