#!/bin/bash

echo "#######################"
echo -e "Launching provider $PROVIDER"
echo "#######################"


case $PROVIDER in

    hubspot)
        python hubspot_lambda_handler/src/lambda_f.py 2>&1
        if [ $? -ne 0 ]; then
            ERROR_MESSAGE="Execution script for provider '$PROVIDER' failed"
            sentry-cli send-event --message "$ERROR_MESSAGE"
        fi
        ;;

    linkedin)
        python linkedin/lambda_f.py 2>&1
        if [ $? -ne 0 ]; then
            ERROR_MESSAGE="Execution script for provider '$PROVIDER' failed"
            sentry-cli send-event --message "$ERROR_MESSAGE"
        fi
        ;;

    *)
        # Use the sentry-cli tool to report the error to Sentry in case in
        # unknown provider given.
        ERROR_MESSAGE="/!\ Ingest-framework ERROR unknown provider '$PROVIDER' /!\\"
        sentry-cli send-event --message "$ERROR_MESSAGE"
        echo $ERROR_MESSAGE
        ;;
esac