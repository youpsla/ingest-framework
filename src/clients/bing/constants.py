import os

API_CREDENTIALS_SECRET_NAME = f"ingest-framework/{os.environ['RUNNING_ENV']}/bing/api-credentials"
DEVELOPER_TOKEN_SECRET_NAME = f"ingest-framework/{os.environ['RUNNING_ENV']}/bing/developer-token"
REFRESH_TOKEN_SECRET_NAME = f"ingest-framework/{os.environ['RUNNING_ENV']}/bing/refresh-token"

BING_ENV = "production"
