import os
import sys

RUNNING_ON_LAMBDA = False
RUNNING_ENV = "development"
CHANNEL = "bing"

ROOT_DIR = ""

CONFIGS_DIRECTORY_PATH = "configs"


def get_running_env():
    running_env = os.environ.get("RUNNING_ENV")
    if not running_env:
        raise ValueError("RUNNING_ENV cannot be None.")
    return running_env


RUNNING_ENV = get_running_env()

# if RUNNING_ENV == 'development':


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
