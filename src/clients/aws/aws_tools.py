import base64
import json
import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class SecretManagerClient:
    def __init__(self, region_name="eu-west-1"):
        self.client = self.get_client(region_name)

    def get_client(self, region_name):
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name="secretsmanager", region_name=region_name)
        return client


def search_secrets_by_prefix(prefix):
    client = SecretManagerClient().client
    response = client.list_secrets(
        MaxResults=100,
        Filters=[
            {
                "Key": "name",
                "Values": [
                    prefix,
                ],
            },
        ],
    )
    return response


class Secret:
    def __init__(self, name):
        self.secretsmanager = SecretsManager()
        self.name = name

    def get_value(self):

        if self.name is None:
            raise ValueError

        try:
            get_secret_value_response = self.secretsmanager.client.get_secret_value(
                SecretId=self.name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "DecryptionFailureException":
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InternalServiceErrorException":
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if "SecretString" in get_secret_value_response:
                secret = get_secret_value_response["SecretString"]
                values = json.loads(secret)
                return values
            else:
                decoded_binary_secret = base64.b64decode(
                    get_secret_value_response["SecretBinary"]
                )
                return decoded_binary_secret

    def create(self, name=None, stage=None):
        """
        Gets the value of a secret.

        :param stage: The stage of the secret to retrieve. If this is None, the
                      current stage is retrieved.
        :return: The value of the secret. When the secret is a string, the value is
                 contained in the `SecretString` field. When the secret is bytes,
                 it is contained in the `SecretBinary` field.
        """
        if name is None:
            raise ValueError

        try:
            kwargs = {"SecretId": name}
            if stage is not None:
                kwargs["VersionStage"] = stage
            response = self.secretsmanager_client.get_secret_value(**kwargs)
            logger.info("Got value for secret %s.", name)
        except ClientError:
            logger.exception("Couldn't get value for secret %s.", name)
            raise
        else:
            return response

    def put_value(self, secret_value, stages=None):
        """
        Puts a value into an existing secret. When no stages are specified, the
        value is set as the current ('AWSCURRENT') stage and the previous value is
        moved to the 'AWSPREVIOUS' stage. When a stage is specified that already
        exists, the stage is associated with the new value and removed from the old
        value.

        :param secret_value: The value to add to the secret.
        :param stages: The stages to associate with the secret.
        :return: Metadata about the secret.
        """
        if self.name is None:
            raise ValueError

        try:
            kwargs = {"SecretId": self.name}
            if isinstance(secret_value, str):
                kwargs["SecretString"] = secret_value
            elif isinstance(secret_value, bytes):
                kwargs["SecretBinary"] = secret_value
            if stages is not None:
                kwargs["VersionStages"] = stages

            secret_manager_client = SecretManagerClient().client
            response = secret_manager_client.put_secret_value(**kwargs)
            # response = self.secretsmanager_client.put_secret_value(**kwargs)
            logger.info("Value put in secret %s.", self.name)
        except ClientError:
            logger.exception("Couldn't put value in secret %s.", self.name)
            raise
        else:
            return response


class SecretsManager:
    def __init__(self):
        self.client = boto3.client("secretsmanager")
        self.secret_name = None

    def get_secret(self, secret_name):
        """
        Retrieve the credentials registered in the aws secret manager service
        for a secret_name given in parameter.

        Raises
        ------
        ClientError: if one of these errors happend and throwable parameter is
                    set to True raise the error otherwise, return an empty dict:
                    DecryptionFailureException, InternalServiceErrorException,
                    InvalidParameterException, InvalidRequestException,
                    ResourceNotFoundException

        Parameters
        ----------
        secret_name: str
            Name of the secret.

        Returns
        -------
        _ : dict (or bytes)
            Containing credentials.
        """
        try:
            get_secret_value_response = self.client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in [
                "DecryptionFailureException",
                "InternalServiceErrorException",
                "InvalidParameterException",
                "InvalidRequestException",
                "ResourceNotFoundException",
            ]:
                logger.error(e)
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of
            # these fields will be populated.
            if "SecretString" in get_secret_value_response:
                secret = get_secret_value_response["SecretString"]
                credentials = json.loads(secret)
                return credentials
            else:
                decoded_binary_secret = base64.b64decode(
                    get_secret_value_response["SecretBinary"]
                )
                return decoded_binary_secret
