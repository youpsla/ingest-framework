import os

import psycopg2

from src.envs import ENV_KEYS
from src.tools.aws_tools import Secret


class RedshiftClient:
    """Class managing:
    - Getting envs variables for dev or prod
    - Creating the db connection

    inputs:
    secret_env: Should be:
    WEB_INGEST_PROD_RW_SECRET_NAME
    WEB_INGEST_PROD_RO_SECRET_NAME
    WEB_INGEST_DEV_RW_SECRET_NAME
    WEB_INGEST_DEV_RO_SECRET_NAME

    """

    def __init__(self, auth_type="dev", mode="readwrite", dbname="snowplow", **_unused):
        self.auth_type = auth_type
        self.mode = mode
        self.dbname = dbname
        self._db_connection = None
        self._write_results_db_connection = None

    @property
    def secret_name_env_key(self):
        return ENV_KEYS["redshift"][self.auth_type][self.mode]

    @property
    def secret_name(self):
        name = os.getenv(self.secret_name_env_key)
        if not name:
            raise Exception(
                f"{self.secret_name_env_key} doesn't exists or is None. Please set"
                f" {self.secret_name_env_key} with the Redshift secret name storing db"
                " credentials."
            )
        return name

    @property
    def db_connection(self):
        if not self._db_connection:
            credentials = Secret(self.secret_name).get_value()
            user = credentials["username"]
            pwd = credentials["password"]
            host = credentials["host"]
            port = credentials["port"]
            dbname = self.dbname

            connection = psycopg2.connect(
                user=user, host=host, port=int(port), password=pwd, database=dbname
            )
            self._db_connection = connection

        return self._db_connection

    @property
    def write_results_db_connection(self):
        if not self._write_results_db_connection:
            credentials = Secret(self.secret_name).get_value()
            user = credentials["username"]
            pwd = credentials["password"]
            host = credentials["host"]
            port = credentials["port"]
            dbname = self.dbname

            connection = psycopg2.connect(
                user=user, host=host, port=int(port), password=pwd, database=dbname
            )
            self._write_results_db_connection = connection

        return self._write_results_db_connection
