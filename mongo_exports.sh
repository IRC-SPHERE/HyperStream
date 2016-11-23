#!/usr/bin/env bash

mongoexport -h=localhost:27018 -u=dbuser -p=rYYr7QEeD5 --authenticationDatabase=admin --authenticationMechanism=MONGODB-CR -d=hyperstream -c=meta_data --out=mongo_dumps/meta_data.txt
mongoexport -h=localhost:27018 -u=dbuser -p=rYYr7QEeD5 --authenticationDatabase=admin --authenticationMechanism=MONGODB-CR -d=hyperstream -c=plate_definitions --out=mongo_dumps/plate_definitions.txt
mongoexport -h=localhost:27018 -u=dbuser -p=rYYr7QEeD5 --authenticationDatabase=admin --authenticationMechanism=MONGODB-CR -d=hyperstream -c=workflow_definitions --out=mongo_dumps/workflow_definitions.txt
mongoexport -h=localhost:27018 -u=dbuser -p=rYYr7QEeD5 --authenticationDatabase=admin --authenticationMechanism=MONGODB-CR -d=hyperstream -c=workflow_status --out=mongo_dumps/workflow_status.txt