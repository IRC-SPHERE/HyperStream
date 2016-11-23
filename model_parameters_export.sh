#!/usr/bin/env bash

if [ "$#" -lt 1 ]; then
    echo "Please specify at least one house id"
fi

# Pull last model from the database
for house in $*; do
    echo "House $house"
    mongoexport -h=localhost:27018 -u=dbuser -p=rYYr7QEeD5 --authenticationDatabase=admin --authenticationMechanism=MONGODB-CR -d=hyperstream -c=streams --limit=1 -q="{'stream_id.name': 'location_prediction', 'stream_id.meta_data.localisation_model': 'lda', 'stream_id.meta_data.house': '$house'}" --sort="{datetime:-1}" --out=mongo_dumps/model_parameters_${house}.txt
    echo ""
done
