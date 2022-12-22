import os
import sys

PROVIDER = os.environ["PROVIDER"]
SENTRY_DSN = os.environ["SENTRY_DSN"]
TASK_GROUP = os.environ["TASK_GROUP"]
APP_VERSION = os.environ.get("APP_VERSION", "local")

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
