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

from hyperstream import HyperStreamConfig, OnlineEngine, UTC, StreamId, ChannelManager, WorkflowManager, PlateManager, \
    Client

from sphere_connector_package.sphere_connector import SphereConnector, SphereLogger
from datetime import datetime, timedelta
import logging

import os


os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))


# Various constants
t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
t2 = datetime(2016, 4, 29, 13, 0, 0, 0, UTC)
now = datetime(2016, 1, 1, 0, 0, 0)
hour = timedelta(hours=1)
minute = timedelta(minutes=1)
second = timedelta(seconds=1)
zero = timedelta(0)


# Hyperstream setup
sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.CRITICAL)
sphere_connector = SphereConnector(include_mongo=True, include_redcap=False, sphere_logger=sphere_logger)
hyperstream_config = HyperStreamConfig()
# online_engine = OnlineEngine(hyperstream_config)

client = Client(hyperstream_config.mongo)
channels = ChannelManager(hyperstream_config.tool_path)
plates = PlateManager(hyperstream_config.meta_data).plates
workflows = WorkflowManager(channel_manager=channels, plate_manager=plates)

# Various channel_manager
M = channels.memory
S = channels.sphere
T = channels.tools
D = channels.mongo


# Some useful Stream IDs
environmental = StreamId('environmental', meta_data={'house': '1'})
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
sphere_silhouette = StreamId('sphere_silhouette')
sphere = StreamId('sphere')
component = StreamId('component')
component_filter = StreamId('component_filter')


# Some tools
# t_clock = T[clock].define(stride=30 * second)
# t_env = T[sphere].define(modality='environmental')
