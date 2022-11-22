import logging
import os
import sys

import boto3
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

SENTRY_DSN = os.environ["SENTRY_DSN"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

ecs = boto3.client("ecs")

sentry_sdk.init(
    dsn=SENTRY_DSN,
    integrations=[AwsLambdaIntegration(timeout_warning=True)],
    traces_sample_rate=1.0
)


def lambda_handler(event, context):
    """
    Lambda function to process providers.
    Start ECR tasks:
        - hubspot

    parameters
    ----------
    event: dict
        env: str
            Environment name ("production", "staging" or "development").
    context: dict
        empty
    """
    try:
        environment = event.get("env")
        task_parameters = event.get("task_parameters")
        logger.info(
                        f"Running task {environment} environment:\n"
                        f"{task_parameters}"
                    )
        accepted_environment = ["production", "staging", "development"]
        if environment not in ["production", "staging", "development"]:
            raise ValueError(
                        f"'env' variable should be in {accepted_environment}"
                    )
        elif not task_parameters:
            raise ValueError("'task_parameters' parameter is not valid")
        else:
            response = ecs.run_task(**task_parameters)
            if (
                response
                .get("ResponseMetadata", {})
                .get("HTTPStatusCode") != 200
            ):
                tasks = list(map(lambda e: e["taskArn"], response["tasks"]))
                failures = response['failures']
                raise ValueError(
                                    f"Tasks '{tasks}' didn't run correctly:\n"
                                    f"{failures}"
                                )
    except Exception as e:
        logger.error(e)
    else:
        logger.info("Finished running task.")
