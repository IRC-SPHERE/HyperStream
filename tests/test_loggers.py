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
from time import sleep
import json
import mqtthandler

from hyperstream import HyperStream
from hyperstream.utils.hyperstream_logger import MON, SenMLFormatter
from helpers import *


# noinspection PyMethodMayBeStatic,SpellCheckingInspection
class HyperStreamLoggingTests(unittest.TestCase):
    def test_mqtt_logger(self):
        """
        Test the MQTT logger using the standard format
        Note that mosquitto should be running first:
        $ docker run -ti -p 1883:1883 -p 9001:9001 toke/mosquitto
        or
        $ brew services start mosquitto

        """
        # assert(mosquitto_is_running())
        logging.raiseExceptions = True

        # noinspection PyTypeChecker
        mqtt_logger = dict(host=mqtt_ip, port=1883, topic="topics/test", loglevel=MON, qos=1)

        with HyperStream(file_logger=False, console_logger=False, mqtt_logger=mqtt_logger):
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
        # assert (mosquitto_is_running())
        logging.raiseExceptions = True

        def handleError(self, record):
            raise

        mqtthandler.MQTTHandler.handleError = handleError

        # noinspection PyTypeChecker
        mqtt_logger = dict(host=mqtt_ip, port=1883, topic="topics/test", loglevel=MON, qos=1,
                           formatter=SenMLFormatter())

        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=mqtt_logger)

        with MqttClient() as client:
            # client.client.publish("topics/test", "{} ABC".format(utcnow()))
            logging.monitor("1234567890", extra=dict(n="blah"))
            sleep(1)
            # print(client.last_messages["topics/test"])
            msg = json.loads(client.last_messages["topics/test"])
            assert(msg['e'][0]['n'] == 'blah')
            assert(msg['e'][0]['v'] == '1234567890')
            assert(msg['uid'] == 'hyperstream')

            logging.monitor("1234567890")
            sleep(1)
            # print(client.last_messages["topics/test"])
            msg = json.loads(client.last_messages["topics/test"])
            assert(msg['e'][0]['n'] == 'default')
            assert(msg['e'][0]['v'] == '1234567890')
            assert(msg['uid'] == 'hyperstream')


if __name__ == "__main__":
    tests = HyperStreamLoggingTests()
    tests.test_mqtt_logger()
