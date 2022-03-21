from psycopg2 import extras
from psycopg2.extras import RealDictCursor

from CONFIG import SCHEMA_NAME


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

    @property
    def raw_sql(self):
        return self._raw_sql.format(schema=SCHEMA_NAME)

    @property
    def schema_table(self):
        return "{}.{}".format(SCHEMA_NAME, self.model.model_name)

    def run(self):

        db_connection = self.destination.db_connection
        if self.qtype == "select":
            self.sql = self.get_sql_select()
            with db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(self.sql)
                db_connection.commit()
                res = cursor.fetchall()
                return res
        elif self.qtype == "insert":
            with db_connection.cursor() as cursor:
                self.sql = self.get_sql_insert()
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
                res = cursor.fetchall()
                return res
        elif self.qtype == "raw_sql":
            with db_connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(self.raw_sql)
                db_connection.commit()
                res = cursor.fetchall()
                return res
        elif self.qtype == "update":
            with db_connection.cursor() as cursor:
                self.sql = self.get_sql_update()
                cursor.execute(self.sql)
                db_connection.commit()
        return None

    def get_sql_select(self):
        sql = (
            f"SELECT {'*' if not self.fields else ','.join(self.fields)}"
            f" from {self.schema_table}"
            f" {'where '+self.where if self.where else ''}"
        )
        return sql

    def get_sql_select_max(self):
        # if len(self.fields) > 1:
        #     raise ValueError("Only one field can be passed has argument for slect_max") # noqa: E501
        sql = f"SELECT MAX({self.max_field}) FROM {self.schema_table}"
        return sql

    def get_sql_insert(self):
        sql = """INSERT INTO {} ({}) VALUES %s"""
        sql = sql.format(self.schema_table, ", ".join(self.fields))
        return sql

    def get_sql_update(self):
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
