#!/bin/bash

# Script for deploying the ingest-framework to AWS.
#
# Steps:
#   1 - Checks if the provided environment is valid, and if not, it prints a help message and exits.
#   2 - Deploy or update the infrastructure using the sam deploy command,
#       specifying the ci/infrastructure.yaml template file and setting the Environment,
#       SentryDsn, and StackVersion parameters.
#   3 - Log in to the AWS Docker repository using the aws ecr get-login-password and docker login commands.
#   4 - Build a Docker image using the docker build command and tags it with the current environment and timestamp.
#   5 - Push the Docker image to the AWS Docker repository using the docker push command.
#
# Help:
#
#   AWS_PROFILE=<profile> ./deploy_infra.sh <environment>
#
#   <environment>: The environment to deploy to. Must be one of [development or production].
#
# Examples:
#
#   Deploy to the development environment:
#
#     AWS_PROFILE=jabmo ./deploy_infra.sh development

# Terminate script on error
set -e
set -o pipefail


help() {
    echo -e "HELP:\n"
    echo -e "    $0 <environment>\n"
    echo "    environment: The environment for which to deploy ingest-framework."
    echo "    Must be one of [production, development]"
    echo ""
    echo "    /!\ Docker is required /!\\"
    echo ""
    echo "Examples:"
    echo ""
    echo -e "   Deploy to the development environment:\n"
    echo "     AWS_PROFILE=jabmo ./deploy_infra.sh development"
    exit 1
}

# Check if environment name was provided and is valid.
if [[ ! "$1" =~ ^(development|production)$ ]]; then
    echo -e "\n /!\ ERROR: VALID PARAMETER ENVIRONMENT REQUIRED. /!\ \n"
    help
    exit 1
fi

# Check if Docker is running
if ! which docker &> /dev/null || ! docker info &> /dev/null; then
    # Docker is running, launch the script
    echo -e "\n /!\ Error: Docker is not installed or running /!\\ \n"
    help
    exit 1
fi



# Define variables.
CALLER_IDENTITY=$(aws sts get-caller-identity --query "Account" --output text)
BUCKET_NAME=jabmo-ingest
REPOSITORY_NAME=ingest-framework-repository
TS=$(date +"%s")
STACK_VERSION=$(git describe)-$1-$TS
if [ "$1" == "development" ]; then
    TAG=latest
else
    TAG=$STACK_VERSION
fi

echo "==> Deploying for $1 environment <=="
read -p "$(echo -e 'Continue?\n[Enter] → Yes\n[Ctrl]+[C] → No.\n ')"
############################################################################################

echo "==> Check if '$BUCKET_NAME' bucket exists <=="
if [[ ! -z $(aws s3api list-buckets --query "Buckets[?Name=='$BUCKET_NAME']" --output text) ]]; then
    echo "==> Bucket '$BUCKET_NAME' already exists <=="
else
    echo "==> Beginning of the creation of the bucket '$BUCKET_NAME' <=="
    aws s3 mb s3://$BUCKET_NAME --region eu-west-1
    echo "==> Bucket '$BUCKET_NAME' created <=="
fi

echo "==> Deploying/updating ECR resource <=="
sam deploy -t ci/repository.yaml \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        Environment=$1 \
        StackVersion=$STACK_VERSION \
        RepositoryName=$REPOSITORY_NAME\
    --confirm-changeset \
    --use-json \
    --s3-bucket $BUCKET_NAME \
    --stack-name jabmo-ingest-framework-repository \
    --profile $AWS_PROFILE

echo "==> Deploying/updating the infrastructure <=="
sam deploy -t ci/infrastructure.yaml \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        Environment=$1 \
        StackVersion=$STACK_VERSION \
        Tag=$TAG \
    --confirm-changeset \
    --use-json \
    --s3-bucket $BUCKET_NAME \
    --stack-name jabmo-ingest-framework-$1 \
    --profile $AWS_PROFILE


############################################################################################


echo "==> Login to AWS docker repository: $CALLER_IDENTITY.dkr.ecr.eu-west-1.amazonaws.com <=="
aws ecr get-login-password \
    --region eu-west-1 \
    --profile $AWS_PROFILE \
| docker login \
    --username AWS \
    --password-stdin $CALLER_IDENTITY.dkr.ecr.eu-west-1.amazonaws.com


############################################################################################


echo "==> building the image ingest-framework <=="
docker build -t ingest-framework -f docker/ingest-framework.dockerfile . --platform linux/amd64


############################################################################################

echo "==> Tagging the image ingest-framework <=="
# Deploy the image in ECR.
docker tag ingest-framework $CALLER_IDENTITY.dkr.ecr.eu-west-1.amazonaws.com/$REPOSITORY_NAME:$TAG

############################################################################################

echo "==> Pushing the image to $CALLER_IDENTITY.dkr.ecr.eu-west-1.amazonaws.com/ingest-framework-repository <=="
docker push $CALLER_IDENTITY.dkr.ecr.eu-west-1.amazonaws.com/ingest-framework-repository:$TAG