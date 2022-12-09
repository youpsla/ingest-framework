#!/bin/bash

echo "#######################"
echo -e "Launching provider $PROVIDER"
echo "#######################"

handle_error() {
    if [ $? -ne 0 ]; then
        ERROR_MESSAGE="Execution script for provider '$PROVIDER' failed"
        sentry-cli send-event --message "$ERROR_MESSAGE"
    fi
}


case $PROVIDER in

    hubspot)
        python src/scripts/lambda_f.py 2>&1
        ;;

    linkedin)
        python src/scripts/lambda_f.py 2>&1
        ;;

    bing)
        python src/scripts/lambda_f.py 2>&1
        ;;

    on24)
        python src/scripts/lambda_f.py 2>&1
        ;;

    *)
        # Use the sentry-cli tool to report the error to Sentry in case in
        # unknown provider given.
        ERROR_MESSAGE="/!\ Ingest-framework ERROR unknown provider '$PROVIDER' /!\\"
        sentry-cli send-event --message "$ERROR_MESSAGE"
        echo $ERROR_MESSAGE
        ;;
esac

# Send error to Sentry if an error occured within this script.
handle_error