#!/bin/bash

echo "#######################"
echo -e "Launching provider $PROVIDER"
echo "#######################"

send_error_to_sentry() {
    sentry-cli send-event --message "$ERROR_MESSAGE" --env=$RUNNING_ENV -t provider:$PROVIDER -t task_group:$TASK_GROUP
}

handle_error() {
    if [ $? -ne 0 ]; then
        ERROR_MESSAGE="Execution script for provider '$PROVIDER' failed"
        send_error_to_sentry
    fi
}


case $PROVIDER in

    hubspot | linkedin | bing | on24)
        python src/scripts/lambda_f.py 2>&1
        ;;

    *)
        # Use the sentry-cli tool to report the error to Sentry in case in
        # unknown provider given.
        ERROR_MESSAGE="/!\ Ingest-framework ERROR unknown provider '$PROVIDER' /!\\"
        send_error_to_sentry
        echo $ERROR_MESSAGE
        ;;
esac

# Send error to Sentry if an error occured within this script.
handle_error