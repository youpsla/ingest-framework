import json
import os

import boto3
import psycopg2
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

# TODO: Use another schema for tmp table for rpeventing writing in prod schema.
# TODO: Currently, the delete is done randomly on duplicates. Could be nice if the older one is keeped. That's an issue for tables without jab_id field.


sentry_sdk.init(
    dsn=os.environ["SENTRY_DSN"],
    integrations=[AwsLambdaIntegration(timeout_warning=True)],
    traces_sample_rate=1.0,
)

"""
Script which detects duplicates in Redshift tables;
Comparaison between records is based on all fields except those spcified in "COLUMNS_TO_EXCLUDE"
"""


DB_SECRET_NAMES = {
    "redshift": {
        "dev": {
            "readonly": "jabmo/db/redshift/web-ingest/dev/ro",
            "readwrite": "jabmo/db/redshift/web-ingest/dev/rw",
        },
        "prod": {
            "readonly": "jabmo/db/redshift/web-ingest/prod",
            "readwrite": "jabmo/db/redshift/web-ingest/prod/rw",
        },
    },
}


SQL_CREATE_TMP_TABLE = """create table {schema}.tmp as select {all_columns} row_num from (
 select {all_columns},
 row_number() over (partition by {data_columns}) row_num
 from {schema}.{table}
)
where row_num = 1 {partition_order_by_field}
"""

SQL_COUNT_SOURCE_TABLE = "select count(*) from {source_schema_table}"
SQL_COUNT_TMP_TABLE = "select count(*) from {schema}.tmp"
SQL_DROP_TMP_TABLE = "drop table {schema}.tmp"
SQL_GET_COLUMNS = "SELECT * FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name   = '{source_table}'"
SQL_GET_BASE_TABLES_FROM_SCHEMA = (
    "SELECT * FROM information_schema.tables WHERE table_schema = '{schema}'"
)
COLUMNS_TO_EXCLUDE = ("jab_id", "jab_created_at", "entry_date")


def get_db_connection(env, mode):
    """Must set the following variables:
    user = xxx
    pwd = xxx
    host = xxx
    port = xxx
    dbname = xxx
    """
    client = boto3.client("secretsmanager")
    if env == "development" or env.startswith("if-dev-"):
        local_env = "dev"
    else:
        local_env = "prod"
    secret_values = client.get_secret_value(
        SecretId=DB_SECRET_NAMES["redshift"][local_env][mode],
    )
    secret_string = json.loads(secret_values["SecretString"])

    connection = psycopg2.connect(
        user=secret_string["username"],
        host=secret_string["host"],
        port=int(secret_string["port"]),
        password=secret_string["password"],
        database=secret_string["database"],
    )

    return connection


def get_all_column_list(cursor, schema, source_table):
    """Retrieve all the columns for a table on a list format

    Args:
        cursor: cursor
        schema: str
        source_table: str

    Returns:
        result: list
        A list of columns name.
    """
    cursor.execute(SQL_GET_COLUMNS.format(schema=schema, source_table=source_table))
    result = cursor.fetchall()
    result = [r[3] for r in result]
    return result


def get_all_columns(cursor, schema, source_table):
    """Retrieve all the columns for a table on a str format

    Args:
        cursor: cursor
        schema: str
        source_table: str

    Returns:
        result: str
        A string representing tables name separate by comma.
    """
    cl = get_all_column_list(cursor, schema, source_table)
    result = ", ".join(cl)

    # print("Columns used in uniqueness count:\n- {}".format("\n- ".join(r_list)))
    return result


def get_data_columns(cursor, schema, source_table):
    """Retrieve all the columns excluding COLUMNS_TO_EXCLUDE

    COLUMNS_TO_EXCLUDE contains columns that are not relevant for detecting duplicates.
    Could be "jab_id", "entry_date". Those are fields which will have differents values when running the script at differents momens but retrieving exactly same data values.

    Args:
        cursor: cursor
        schema: str
        source_table: str

    Returns:
        result: str
        A list of str.
    """
    cl = get_all_column_list(cursor, schema, source_table)
    r_list = [c for c in cl if c not in COLUMNS_TO_EXCLUDE]

    result = ", ".join(r_list)

    # print("Columns used in uniqueness count:\n- {}".format("\n- ".join(r_list)))
    return result


def schema_get_base_tables(cursor, schema):
    """Retrieve all the tables "BASE TABLE" for a schema

    Args:
        cursor: cursor
        schema: str

    Returns:
        result: list
        A list of str
    """
    cursor.execute(SQL_GET_BASE_TABLES_FROM_SCHEMA.format(schema=schema))
    result = cursor.fetchall()
    r_list = [r[2] for r in result if r[3] == "BASE TABLE"]
    # print("Tables in schema {}:\n- {}".format(schema, "\n- ".join(r_list)))
    return r_list


def delete_duplicates(cursor, schema, table):
    """Delete duplicates in table

    First, compare "partition_order_by_field" values in both source and tmp table.
    Ids in tmp table are unique ids. We keep them but delete the others.

    Args:
        cursor: cursor
        schema: str
        table: str

    """
    SQL_ID_OF_UNIQUES_RECORDS = """select jab_id from {schema}.tmp"""
    SQL_FOR_EXISTINGS_ID = """select jab_id from {schema}.{table}"""

    SQL_DELETE_IDS_FROM_ORIGIN_TABLE = (
        """delete from {schema}.{table} where jab_id in ({ids_to_delete})"""
    )

    cursor.execute(SQL_FOR_EXISTINGS_ID.format(schema=schema, table=table))
    existings_ids = [str(r[0]) for r in cursor.fetchall()]

    cursor.execute(SQL_ID_OF_UNIQUES_RECORDS.format(schema=schema))
    ids_of_unique_records = [str(r[0]) for r in cursor.fetchall()]

    ids_to_delete = list(set(existings_ids) - set(ids_of_unique_records))

    if ids_to_delete:
        cursor.execute(
            SQL_DELETE_IDS_FROM_ORIGIN_TABLE.format(
                schema=schema, table=table, ids_to_delete=", ".join(ids_to_delete)
            )
        )


def lambda_handler(event, context):

    schemas = event.get("schemas", None)
    tables = event.get("tables", None)
    do_delete_duplicates = event.get("do_delete_duplicates", None)
    partition_order_by_field = event.get("partition_order_by_field", None)
    env = event.get("env", "development")
    mode = event.get("mode", "readonly")

    main(schemas, tables, do_delete_duplicates, partition_order_by_field, env, mode)


def get_number_of_duplicates():
    pass


def main(
    schemas=None,
    tables=None,
    do_delete_duplicates=False,
    partition_order_by_field=None,
    env="development",
    mode="readonly",
):
    """Set the attribute of type Field to self (Model).

    Args:
        schema: str
        tables: iterable
        Contains tables name. If no tables is specified, the run is done on all "BASE TABLE" of the schema.
    """
    # Get connection to the Db
    connection = get_db_connection(env, mode)

    with connection.cursor() as cursor:
        for schema in schemas:

            # If no "tables" arg is passed, we retrieve all tables of the schema.
            if not tables:
                tables = schema_get_base_tables(cursor, schema)

            # For each table, we create a tmp table with unique records.
            # Count the number of records in each table.
            for table in tables:
                print(f"##### Running on {schema }.{table} #####")
                all_columns = get_all_columns(cursor, schema, table)
                data_columns = get_data_columns(cursor, schema, table)

                sql = SQL_CREATE_TMP_TABLE.format(
                    schema=schema,
                    all_columns=all_columns,
                    data_columns=data_columns,
                    table=table,
                    partition_order_by_field=f" order by {partition_order_by_field} asc"
                    if partition_order_by_field
                    else "",
                )
                cursor.execute(sql)
                sql = SQL_COUNT_SOURCE_TABLE.format(
                    source_schema_table=".".join((schema, table))
                )
                cursor.execute(sql)
                count_source = cursor.fetchone()[0]

                cursor.execute(SQL_COUNT_TMP_TABLE.format(schema=schema))
                count_tmp = cursor.fetchone()[0]

                nb_duplicates = count_source - count_tmp
                if nb_duplicates != 0:
                    print(
                        f"{'.'.join((schema, table)).ljust(40)}: {str(nb_duplicates).ljust(8)} duplicates"
                    )
                else:
                    print("No duplicate found")

                if do_delete_duplicates is True:
                    delete_duplicates(cursor, schema, table)

                cursor.execute(SQL_DROP_TMP_TABLE.format(schema=schema))
                tables = None

        if do_delete_duplicates is True:
            connection.commit()
        else:
            connection.rollback()
    connection.close()


if __name__ == "__main__":
    event = {
        "schemas": ["new_linkedin"],
        "tables": [],  # If empty, all tables are selected. Otherwise, onbly table names as str.
        "do_delete_duplicates": True,  # If False, only output logs.
        "partition_order_by_field": "jab_id",  # Do not touch !!!! Has to be an UNIQUE.
        "env": "production",
        "mode": "readwrite",  # Do not touch.
    }
    lambda_handler(event, "")
