import os

from psycopg2 import ProgrammingError, extras
from psycopg2.extras import RealDictCursor

from configs.globals import PROVIDER
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
        raw_sql=None,
        where=None,
        update_keys=None
    ):
        self.qtype = qtype
        self.fields = fields
        self.values = values
        self.db_connection = db_connection
        self.model = model
        self._raw_sql = raw_sql
        self.sql = None
        self.where = where
        self.update_keys = update_keys
        self._values_list = None

    @property
    def schema_name(self):
        return get_schema_name(PROVIDER)

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
        with self.db_connection.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:  # noqa: E501
            try:
                if self.qtype == "select":
                    self.sql = self.get_sql_select()
                    cursor.execute(self.sql)
                elif self.qtype == "insert":
                    self.sql = self.get_sql_insert(self.schema_table)
                    extras.execute_values(
                        cursor, self.sql, self.values, page_size=500
                    )  # noqa: E501
                elif self.qtype == "get_from_raw_sql":
                    cursor.execute(self.raw_sql)
                    return cursor.fetchall()
                elif self.qtype == "raw_sql":
                    cursor.execute(self.raw_sql)
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
                    # 3) Count number of fileds to be updated
                    cursor.execute(self.get_count_sql_update())
                    count_update_result = cursor.fetchone()
                    logger.info(
                        f"Number of records to be updated: {count_update_result['count']}"  # noqa: E501
                    )
                    # 4) Do the update
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
                raise RuntimeError(f"The following error occur: {e}")
            return result

    def get_sql_select(self):
        sql = (
            f"SELECT {'*' if not self.fields else ','.join(self.fields)}"
            f" from {self.schema_table}"
        )
        return sql

    def get_sql_insert(self, schema_table):
        sql = """INSERT INTO {} ({}) VALUES %s"""
        sql = sql.format(
            schema_table,
            ", ".join([f.name for f in self.model.get_db_fields_list()]),  # noqa: E501
        )
        return sql

    def create_tmp_stage_table(self):
        sql_create_tmp_table = (
            "DROP TABLE IF EXISTS {stage_table_name}; create table IF NOT EXISTS {stage_table_name}"  # noqa: E501
            " (like {schema_table});".format(
                schema_table=self.schema_table,
                stage_table_name=self.stage_table_name,
            )
        )
        return sql_create_tmp_table

    def get_sql_update(self):
        set_fields = set(self.values[0].keys()) - set(self.update_keys)
        # set_fields_1 = [f.name for f in self.model.fields_list]
        # set_fields_1.remove(self.update_keys)

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
            result = " AND ".join(
                [
                    "{target}.{pk} = {stage_table_name}.{pk}".format(
                        target=target,
                        stage_table_name=self.stage_table_name,
                        pk=pk,
                    )
                    for pk in self.update_keys
                ]
            )
            # result = "{target}.{pk} = {stage_table_name}.{pk}".format(
            #     target=target,
            #     stage_table_name=self.stage_table_name,
            #     pk=self.update_key,
            # )
            return result

        def update_get_and_where(self):
            comparaison_fields = set(self.values[0].keys()) - set(
                self.update_keys
            )  # noqa: E501

            # Solution managing case of null value in redshift.
            where_data = " or ".join(
                [
                    "("
                    + "("
                    + " != ".join(
                        (
                            "{}.{}".format(self.model.model_name, str(f)),
                            "{}.{}".format(self.stage_table_name, str(f)),
                        )
                    )
                    + ")"
                    + " or "
                    + "("
                    + "{}.{}".format(self.stage_table_name, str(f))
                    + " is not null "
                    + " and "
                    + "{}.{}".format(self.model.model_name, str(f))
                    + " is null "
                    + ")"
                    + ")"
                    for f in comparaison_fields
                ]
            )

            return where_data

        target = self.model.model_name

        sql = (
            "update {schema_table} set {set_part} from {stage_table_name} where"  # noqa: E501
            " {where_primary_key} and ({and_where})".format(
                schema_table=self.schema_table,
                set_part=update_get_set(self),
                stage_table_name=self.stage_table_name,
                where_primary_key=update_get_where_primary_key(self, target),
                and_where=update_get_and_where(self),
            )
        )
        return sql

    def get_count_sql_update(self):
        def update_get_where_primary_key(self, target):
            result = " AND ".join(
                [
                    "{target}.{pk} = {stage_table_name}.{pk}".format(
                        target=target,
                        stage_table_name=self.stage_table_name,
                        pk=pk,
                    )
                    for pk in self.update_keys
                ]
            )
            # result = "{target}.{pk} = {stage_table_name}.{pk}".format(
            #     target=target,
            #     stage_table_name=self.stage_table_name,
            #     pk=self.update_key,
            # )
            return result

        def update_get_and_where(self):
            comparaison_fields = set(self.values[0].keys()) - set(
                self.update_keys
            )  # noqa: E501

            # Solution managing case of null value in redshift.
            where_data = " or ".join(
                [
                    "("
                    + "("
                    + " != ".join(
                        (
                            "{}.{}".format(self.model.model_name, str(f)),
                            "{}.{}".format(self.stage_table_name, str(f)),
                        )
                    )
                    + ")"
                    + " or "
                    + "("
                    + "{}.{}".format(self.stage_table_name, str(f))
                    + " is not null "
                    + " and "
                    + "{}.{}".format(self.model.model_name, str(f))
                    + " is null "
                    + ")"
                    + ")"
                    for f in comparaison_fields
                ]
            )

            return where_data

        target = self.model.model_name

        sql = (
            "select count(*) from {stage_table_name}, {source_table} where"
            " {where_primary_key} and ({and_where})".format(
                stage_table_name=self.stage_table_name,
                source_table=self.schema_table,
                where_primary_key=update_get_where_primary_key(self, target),
                and_where=update_get_and_where(self),
            )
        )
        return sql

    def get_sql_partial_update(self):
        sql = """UPDATE {} SET {} WHERE {}"""

        where = " ".join(
            [f" {k}='{v}'" for d in self.where for k, v in d.items()]
        )  # noqa: E501

        # set_data = ' , '.join(['='.join((str(a[0]),str(a[1]))) for a in zip(fields,values)]) # noqa: E501
        set_data = " , ".join(
            [
                "=".join(
                    (
                        str(k),
                        "'{}'".format(v.replace("'", "''") if v else None),
                    )  # noqa: E501
                )  # noqa: E501
                for d in self.values
                for k, v in d.items()
            ]
        )
        sql = sql.format(self.schema_table, set_data, where)
        return sql
