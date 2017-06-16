#!/bin/bash

echo "Activating virtual environment"
. ./venv/bin/activate

function stop_container
{
    NAME=$1
    if [ `docker inspect -f '{{.State.Running}}' $NAME` ]
    then
        echo "Stopping docker $NAME container"
        docker stop $NAME
    fi
    echo "Removing docker $NAME container"
    docker rm $NAME
}

function start_container
{
    NAME=$1
    ARGUMENTS=$2
    WAIT_PORT=$3
    if [ `docker ps -a --format="{{ .Names }}" | grep $NAME` ]
    then
        stop_container $NAME
    fi
    CONTAINER=`docker run --name $NAME $ARGUMENTS`
    until nc -z $(docker inspect --format='{{.NetworkSettings.IPAddress}}' $CONTAINER) $WAIT_PORT
    do
        echo "waiting for docker $NAME container..."
        sleep 0.5
    done
}

DOCKER_ARGUMENTS='-d -p 27017:27017 library/mongo'
start_container "hs_mongodb" "${DOCKER_ARGUMENTS}" 27017
DOCKER_ARGUMENTS='-d -ti -p 1883:1883 -p 9001:9001 toke/mosquitto'
start_container "hs_mqtt" "${DOCKER_ARGUMENTS}" 9001

nosetests --with-coverage

stop_container "hs_mqtt"
stop_container "hs_mongodb"
