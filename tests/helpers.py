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

from hyperstream import UTC, StreamId
from subprocess import check_output
import paho.mqtt.client as mqtt

# os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".."))
os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))

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

# Some useful Stream IDs
environmental = StreamId('environmental', meta_data=(('house', '1'),))
clock = StreamId('clock')
aggregate = StreamId('aggregate')
every30s = StreamId('every30s')
motion_kitchen_windowed = StreamId('motion_kitchen_windowed')
env_kitchen_30_s_window = StreamId('env_kitchen_30_s_window')
kitchen = StreamId("kitchen")
kitchen_motion = StreamId('kitchen_motion')
m_kitchen_30_s_window = StreamId('m_kitchen_30_s_window')
average = StreamId('average')
count = StreamId('count')
component = StreamId('component')
component_filter = StreamId('component_filter')


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
