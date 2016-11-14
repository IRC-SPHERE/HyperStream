#!/usr/bin/env bash

mongoimport -h=localhost:27017 -d=hyperstream --drop -c=meta_data --file=mongo_dumps/meta_data.txt
mongoimport -h=localhost:27017 -d=hyperstream --drop -c=plate_definitions --file=mongo_dumps/plate_definitions.txt
mongoimport -h=localhost:27017 -d=hyperstream --drop -c=workflow_definitions --file=mongo_dumps/workflow_definitions.txt
mongoimport -h=localhost:27017 -d=hyperstream --drop -c=workflow_status --file=mongo_dumps/workflow_status.txt