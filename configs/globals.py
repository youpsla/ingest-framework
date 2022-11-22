import os
import sys

from src.utils.various_utils import get_schema_name

# User defined variables #
PROVIDER = "hubspot"
###############################
os.environ["INGEST_CURRENT_PROVIDER"] = PROVIDER

SCHEMA_NAME = get_schema_name(PROVIDER)


if os.environ.get("AWS_EXECUTION_ENV") is None:
    local_home = "/Users/Alain/Projects/jabmo/ingest"
    APPLICATION_HOME = local_home
    os.environ["APPLICATION_HOME"] = APPLICATION_HOME
    sys.path.append(os.path.join(local_home))
else:
    lambda_home = "/var/task"
    APPLICATION_HOME = lambda_home
    os.environ["APPLICATION_HOME"] = APPLICATION_HOME
    sys.path.append(os.path.join(APPLICATION_HOME))
