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

import logging
from datetime import datetime, timedelta

from hyperstream import ChannelManager, HyperStreamConfig, StreamId, Workflow, PlateManager, WorkflowManager, Client
from hyperstream.time_interval import RelativeTimeInterval
from hyperstream.modifiers import Component
from hyperstream.utils import UTC
from hyperstream.itertools2 import online_average, count as online_count, any_set
from sphere_connector_package.sphere_connector import SphereLogger

if __name__ == '__main__':
    # TODO: hyperstream needs it's own logger (can be a clone of this one)
    sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.DEBUG)
    
    hyperstream_config = HyperStreamConfig()
    client = Client(hyperstream_config.mongo)
    
    # Define some managers
    channels = ChannelManager(hyperstream_config.tool_path)
    plates = PlateManager(hyperstream_config.meta_data).plates
    
    # Various constants
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
    t2 = t1 + timedelta(minutes=10)
    second = timedelta(seconds=1)
    minute = timedelta(minutes=1)
    hour = timedelta(hours=1)
    
    # Various channels
    M = channels.memory
    S = channels.sphere
    T = channels.tools
    D = channels.mongo
    
    # A couple of parameters
    relative_window = (-30 * second, timedelta(0))
    interval = (t1, t2)

    # Define streams
    id_tool_sliding_window = StreamId('sliding_window')
    tool_sliding_window = T[id_tool_sliding_window].define(
        lower=timedelta(seconds=-30),
        upper=timedelta(seconds=0),
        increment=timedelta(seconds=10))

    id_memory_sliding_window = StreamId('id_memory_sliding_window')
    stream_memory_sliding_window = M.create_stream(stream_id=id_memory_sliding_window)
    tool_sliding_window.set_output_stream(stream_memory_sliding_window)

    tool_sliding_window.window(interval).execute()

    for kv in stream_memory_sliding_window.iteritems():
        print kv

    # # Define the motion in kitchen tool
    # id_tool_sphere = StreamId('sphere')
    # tool_sphere_environmental = T[id_tool_sphere].define(
    #     modality='environmental')
    #
    # id_memory_m_environmental = StreamId('stream_m_environmental')
    # stream_memory_environmental = S.create_stream(stream_id=id_memory_m_environmental)
    # tool_sphere_environmental.set_output_stream(stream_memory_environmental)
    #
    # # Filter the motion in kitchen
    # id_tool_component = StreamId('component')
    # tool_motion = T[id_tool_component].define(
    #     input_streams=[stream_memory_environmental],
    #     key='motion-S1_K')
    #
    # id_memory_motion = StreamId('id_memory_motion')
    # stream_memory_motion = S.create_stream(id_memory_motion)
    # tool_motion.set_output_stream(stream_memory_motion)
    
    # for kv in tool_motion.window(interval).iteritems():
    #     print kv
    #
    # exit()
