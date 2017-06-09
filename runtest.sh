#!/bin/bash

echo "Activating virtual environment"
. ./venv/bin/activate

echo "Starting MongoDB server"
sudo service mongodb start
#
#MONGODB_CONTAINER=`docker run --name hs_mongodb -p 27017:27017 -d library/mongo`
## Wait for the MONGODB port to be available
#until nc -z $(docker inspect --format='{{.NetworkSettings.IPAddress}}' $MONGODB_CONTAINER) 27017
#do
#    echo "waiting for docker MongoDB container..."
#    sleep 0.5
#done

echo "Starting docker MQTT container"
MQTT_CONTAINER=`docker run --name hs_mqtt -d -ti -p 1883:1883 -p 9001:9001 toke/mosquitto`
# Wait for the MQTT port to be available
until nc -z $(docker inspect --format='{{.NetworkSettings.IPAddress}}' $MQTT_CONTAINER) 9001
do
    echo "waiting for docker MQTT container..."
    sleep 0.5
done
nosetests --with-coverage

echo "Stopping docker MQTT container"
docker stop hs_mqtt
echo "Removing docker MQTT container"
docker rm hs_mqtt
echo "Stopping MongoDB server"
sudo service mongodb stop
