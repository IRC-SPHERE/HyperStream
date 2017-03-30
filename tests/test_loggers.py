# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

import unittest
import logging
from subprocess import check_output
import paho.mqtt.client as mqtt
from time import sleep

from helpers import *


class MqttClient(object):
    last_messages = {}

    def __enter__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.client.connect("127.0.0.1", 1883, 60)
        # Blocking call that processes network traffic, dispatches callbacks and
        # handles reconnecting.
        # Other loop*() functions are available that give a threaded interface and a
        # manual interface.
        self.client.loop_start()
        self.client.subscribe("topics/test")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            # print exc_type, exc_value, traceback
            return False # uncomment to pass exception through
        self.client.loop_stop(force=True)
        return self

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe("$SYS/#")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self.last_messages[msg.topic] = msg.payload
        if msg.topic[:4] != "$SYS":
            print(msg.topic + " " + str(msg.payload))


# noinspection SpellCheckingInspection
def mosquitto_is_running():
    # If you get "No such file or directory on OS-X, brew install pidof
    pids = check_output(["pidof", 'mosquitto'])
    if not pids:
        raise RuntimeError("mosquitto not running")

    pids = map(int, pids.strip().split(" "))

    for pid in pids:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
    return True


# noinspection PyMethodMayBeStatic
class HyperStreamLoggingTests(unittest.TestCase):
    def test_mqtt_logger(self):
        """
        Test the MQTT logger using the standard format
        Note that mosquitto should be running first:
        $ docker run -ti -p 1883:1883 -p 9001:9001 toke/mosquitto
        or
        $ brew services start mosquitto
        
        """
        assert(mosquitto_is_running())

        with MqttClient() as client:
            # client.client.publish("topics/test", "{} ABC".format(utcnow()))
            logging.monitor("1234567890")
            sleep(1)
            print(client.last_messages["topics/test"])
            assert(client.last_messages["topics/test"][24:] == '[MON  ]  1234567890')

    def test_mqtt_logger_json(self):
        """
        Test the MQTT logger using the JSON format
        Note that mosquitto should be running first:
        $ docker run -ti -p 1883:1883 -p 9001:9001 toke/mosquitto
        or
        $ brew services start mosquitto

        """
        assert (mosquitto_is_running())

        with MqttClient() as client:
            # client.client.publish("topics/test", "{} ABC".format(utcnow()))
            logging.monitor("1234567890")
            sleep(1)
            print(client.last_messages["topics/test"])
            assert (client.last_messages["topics/test"][24:] == '[MON  ]  1234567890')


if __name__ == "__main__":
    tests = HyperStreamLoggingTests()
    tests.test_mqtt_logger()
