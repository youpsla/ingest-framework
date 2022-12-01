from src.clients.aws.aws_tools import Secret, SecretsManager

API_CREDENTIALS_SECRET_NAME = "ingest-framework/prod/bing/api-credentials"
DEVELOPER_TOKEN_SECRET_NAME = "ingest-framework/prod/bing/developer-token"
REFRESH_TOKEN_SECRET_NAME = "ingest-framework/prod/bing/refresh-token"


class BingSecretsManager(SecretsManager):
    def __init__(self):
        super().__init__()

    def get_api_credentials(self):
        credentials = self.get_secret(API_CREDENTIALS_SECRET_NAME)
        return credentials

    def get_developer_token(self):
        developer_token = self.get_secret(DEVELOPER_TOKEN_SECRET_NAME)
        return developer_token

    def get_env(self):
        credentials = self.get_api_credentials()
        env = credentials.get("env")
        return env

    def get_refresh_token(self):
        refresh_token_secret = self.get_secret(REFRESH_TOKEN_SECRET_NAME)
        return True

    def put_refresh_token(self, refresh_token):
        secret_name = "ingest-framework/prod/bing/refresh-token"
        refresh_token_secret = Secret(secret_name)
        refresh_token_secret.put_value("refresh_token", refresh_token)
        return True
