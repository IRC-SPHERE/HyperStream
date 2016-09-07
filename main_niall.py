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
from hyperstream.time_interval import RelativeTimeInterval, TimeInterval
from hyperstream.utils import UTC
from hyperstream.itertools2 import online_average, count as online_count, any_set
from sphere_connector_package.sphere_connector import SphereLogger

from hyperstream.utils.time_utils import MIN_DATE

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
    interval = TimeInterval(t1, t1 + 5 * minute)
    
    #
    #
    #
    #
    # Define sliding window stream
    stream_memory_sliding_window = M.create_stream(StreamId('stream_id_memory_sliding_window'))
    
    stream_tool_sliding_window = channels.get_tool2(
        tool="sliding_window",
        tool_parameters=dict(
            first=MIN_DATE,
            lower=timedelta(seconds=-30),
            upper=timedelta(seconds=0),
            increment=timedelta(seconds=10)
        )
    )
    
    stream_tool_sliding_window.execute(
        input_streams=None,
        interval=interval,
        writer=stream_memory_sliding_window.writer
    )
    
    #
    #
    #
    #
    # Define the motion in kitchen tool
    
    stream_sphere_environmental = S.create_stream(StreamId('stream_id_memory_environmental'))
    
    stream_tool_sphere_environmental = channels.get_tool2(
        tool='sphere',
        tool_parameters=dict(
            modality='environmental'
        )
    )
    
    stream_tool_sphere_environmental.execute(
        input_streams=None,
        interval=interval,
        writer=stream_sphere_environmental.writer
    )
    
    #
    #
    #
    #
    # Filter the motion in kitchen
    stream_memory_motion = M.create_stream(StreamId('id_memory_m_kitchen'))
    
    tool_motion = channels.get_tool2(
        tool='component',
        tool_parameters=dict(
            key='motion-S1_K'
        )
    )
    
    tool_motion.execute(
        input_streams=[stream_sphere_environmental],
        interval=interval,
        writer=stream_memory_motion.writer
    )
    
    #
    #
    #
    #
    # Aggregate over the window
    stream_memory_m_kitchen_mean = M.create_stream(StreamId('id_memory_m_kitchen_mean'))
    
    tool_sliding_apply = channels.get_tool2(
        tool='sliding_apply',
        tool_parameters=dict(
            func=online_average
        )
    )
    
    tool_sliding_apply.execute(
        input_streams=[
            stream_memory_sliding_window,
            stream_memory_motion
        ],
        interval=interval,
        writer=stream_memory_m_kitchen_mean.writer
    )
    
    #
    #
    #
    #
    # Print some data
    # for kv in stream_memory_m_kitchen_mean:
    #     print kv
    
    print stream_memory_m_kitchen_mean.values()
