#!/bin/bash

echo "#######################"
echo -e "Launching provider $PROVIDER"
echo "#######################"


case $PROVIDER in

    hubspot)
        python hubspot_lambda_handler/src/lambda_f.py 2>&1
        ;;

    *)
        echo "/!\ Unknown provider /!\\"
        ;;
esac