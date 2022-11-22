#!/bin/bash

# Parameters
# 1.) environment name

if [[ "$1" =~ ^(development|staging|production)$ ]]; then
    CALLER_IDENTITY=$(aws sts get-caller-identity --query "Account" --output text)
    BUCKET_NAME=jabmo-ingest
    TS=$(date +"%s")
    STACK_VERSION=$(git describe)-$1-$TS
    echo "==> Deploying for $1 environment <=="
    read -p "$(echo -e 'Continue?\n[Enter] → Yes\n[Ctrl]+[C] → No.\n ')"
    ############################################################################################
    echo "==> Deploying/updating the infrastructure <=="
    echo "==> Check if '$BUCKET_NAME' bucket exists <=="
    if [[ ! -z $(aws s3api list-buckets --query "Buckets[?Name=='$BUCKET_NAME']" --output text) ]]; then
        echo "==> Bucket '$BUCKET_NAME' Exists <=="
    else
        echo "==> beginning of the creation of the bucket '$BUCKET_NAME' <=="
        aws s3 mb s3://$BUCKET_NAME --region eu-west-1
        echo "==> Bucket '$BUCKET_NAME' created <=="
    fi
    sam deploy -t ci/infrastructure.yaml \
        --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
        --parameter-overrides \
            RedshiftWebIngestRw=jabmo/db/redshift/web-ingest/$1/rw \
            Environment=$1 \
            SentryDsn=https://47e2fc82fc674f428f2b3e931eddace7@o1035237.ingest.sentry.io/6094713\
            StackVersion=$STACK_VERSION \
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
    docker tag ingest-framework $CALLER_IDENTITY.dkr.ecr.eu-west-1.amazonaws.com/ingest-framework-repository-$1:$1
    ############################################################################################
    echo "==> Pushing the image to $CALLER_IDENTITY.dkr.ecr.eu-west-1.amazonaws.com/ingest-framework-repository-$1 <=="
    docker push $CALLER_IDENTITY.dkr.ecr.eu-west-1.amazonaws.com/ingest-framework-repository-$1:$1
else
    echo "##############################################"
    echo -e "'$1' parameter not valid"
    echo "Should be development, staging or production"
    echo "##############################################"
    exit
fi