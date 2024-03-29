import logging

import psycopg2

from src.clients.aws.aws_tools import Secret
from src.constants import DB_SECRET_NAMES
from src.utils.various_utils import get_running_env

logger = logging.getLogger(__name__)


if logging.getLogger().hasHandlers():
    # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


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

    def __init__(self, mode="readwrite", dbname="snowplow", **_unused):
        self.mode = mode
        self.dbname = dbname
        self._db_connection = None
        self._write_results_db_connection = None

    @property
    def secret_name(self):
        name = DB_SECRET_NAMES["redshift"][get_running_env()][self.mode]
        return name

    @property
    def db_connection(self):
        if not self._db_connection:
            logger.info(f"Secret name: {self.secret_name}")
            credentials = Secret(self.secret_name).get_value()
            user = credentials["username"]
            pwd = credentials["password"]
            host = credentials["host"]
            port = credentials["port"]
            dbname = self.dbname

            connection = psycopg2.connect(
                user=user,
                host=host,
                port=int(port),
                password=pwd,
                database=dbname,
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
                user=user,
                host=host,
                port=int(port),
                password=pwd,
                database=dbname,
            )
            self._write_results_db_connection = connection

        return self._write_results_db_connection
