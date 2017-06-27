# The MIT License (MIT) # Copyright (c) 2014-2017 University of Bristol
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
#  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
#  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
#  OR OTHER DEALINGS IN THE SOFTWARE.

import os
from datetime import datetime, timedelta
from contextlib import contextmanager
import logging

from hyperstream import HyperStream, UTC, NodeIDAbsentError
from subprocess import check_output
import paho.mqtt.client as mqtt

# os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".."))
# os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

# Various constants
mqtt_ip = "127.0.0.1"
# noinspection PyTypeChecker
t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
# noinspection PyTypeChecker
t2 = datetime(2016, 4, 29, 13, 0, 0, 0, UTC)
now = datetime(2016, 1, 1, 0, 0, 0)
hour = timedelta(hours=1)
minute = timedelta(minutes=1)
second = timedelta(seconds=1)
zero = timedelta(0)


@contextmanager
def resource_manager():
    yield HyperStream(loglevel=logging.CRITICAL)


def setup():
    hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
    for plate_id, tag in (('T', 'test'), (None, 'test_meta_data'), ('T1', 'test_plate_creation')):
        try:
            delete_meta_data(hs, tag)
        except NodeIDAbsentError:
            pass
        if plate_id:
            delete_plates(hs, plate_id)
    insert_meta_data(hs, 'test')
    create_plates(hs, 'T', 'test')


def teardown():
    hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
    delete_plates(hs, 'T')
    delete_meta_data(hs, 'test')


def insert_meta_data(hs, tag):
    for data in map(str, range(4)):
        identifier = '{}_{}'.format(tag, data)
        hs.plate_manager.meta_data_manager.insert(tag=tag, identifier=identifier, parent='root', data=data)


def delete_meta_data(hs, tag):
    for data in map(str, range(4)):
        identifier = '{}_{}'.format(tag, data)
        hs.plate_manager.meta_data_manager.delete(identifier)


def get_meta_data(hs, tag):
    return sorted(x.identifier
                  for x in hs.plate_manager.meta_data_manager.global_plate_definitions.all_nodes()
                  if "_".join(x.identifier.split('_')[:-1]) == tag)


def create_plates(hs, plate_id, tag):
    hs.plate_manager.create_plate(
        plate_id=plate_id,
        description=tag,
        meta_data_id=tag,
        values=[],
        complement=True,
        parent_plate=None)

    hs.plate_manager.create_plate(
        plate_id=plate_id + ".U",
        description="nested test",
        meta_data_id="under_" + tag,
        values=[],
        complement=True,
        parent_plate=plate_id)


def delete_plates(hs, plate_id):
    hs.plate_manager.delete_plate(plate_id)
    hs.plate_manager.delete_plate(plate_id + ".U")


class MqttClient(object):
    last_messages = {}

    def __enter__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.client.connect(mqtt_ip, 1883, 60)
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
