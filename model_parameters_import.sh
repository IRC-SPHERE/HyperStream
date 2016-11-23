#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
    echo "Please specify exactly one house id"
fi

# Pull last model from the database
echo "House $1"
mongoimport -h=localhost:27017 -d=hyperstream -c=streams --file=mongo_dumps/model_parameters_${1}.txt
echo ""
