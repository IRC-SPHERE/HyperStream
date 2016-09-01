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
from hyperstream.itertools2 import online_average, count as online_count
from sphere_connector_package.sphere_connector import SphereLogger

if __name__ == '__main__':
    # TODO: hyperstream needs it's own logger (can be a clone of this one)
    sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.DEBUG)
    
    hyperstream_config = HyperStreamConfig()
    client = Client(hyperstream_config.mongo)
    
    # Define some managers
    channels = ChannelManager(hyperstream_config.tool_path)
    plates = PlateManager(hyperstream_config.meta_data).plates
    workflows = WorkflowManager(channels=channels, plates=plates)
    
    # Various constants
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
    t2 = t1 + timedelta(minutes=5)
    second = timedelta(seconds=1)
    minute = timedelta(minutes=1)
    hour = timedelta(hours=1)
    
    # Various channels
    M = channels.memory
    S = channels.sphere
    T = channels.tools
    D = channels.mongo
    
    # Basic definition of a workfloa
    w = Workflow(
        channels=channels,
        plates=plates,
        workflow_id="nt_multimodal_test",
        name="Test of multimodal performance",
        owner="nt",
        description="Another test of creating workflows")
    
    # A couple of parameters
    relative_window = (-30 * second, timedelta(0))
    output_interval = (t1, t2)
    
    # Define stream IDs
    id_tool_sphere = StreamId('sphere')
    id_tool_clock = StreamId('clock')
    id_tool_aggregate = StreamId('aggregate')
    
    id_sphere_m_kitchen = StreamId('m_kitchen')
    id_sphere_m_lounge = StreamId('m_lounge')
    
    id_memory_every30s = StreamId('timer_every30s')
    id_memory_m_kitchen_mean = StreamId('m_kitchen_mean')
    id_memory_m_lounge_mean = StreamId('m_lounge_mean')
    
    # Define streams
    tool_clock_30s = T[id_tool_clock].define(stride=30 * second)
    tool_sphere_environmental = T[id_tool_sphere].define(modality='environmental')
    
    M.create_stream(stream_id=id_memory_every30s, tool_stream=tool_clock_30s)
    
    print 'Mean kitchen:'
    S.create_stream(stream_id=id_sphere_m_kitchen, tool_stream=tool_sphere_environmental) \
        .modify(Component('motion-S1_K')) \
        .relative_window(relative_window)
    
    tool_m_kitchen_avg = T[id_tool_aggregate].define(
        input_streams=[
            S[id_sphere_m_kitchen]
        ],
        timer=M[id_memory_every30s],
        func=online_average
    )
    
    M.create_stream(stream_id=id_memory_m_kitchen_mean, tool_stream=tool_m_kitchen_avg)
    
    for kk, vv in M[id_memory_m_kitchen_mean].window(output_interval).iteritems():
        print "{}: {}".format(kk, vv)
        
    print 'Mean lounge:'
    
    S.create_stream(stream_id=id_sphere_m_lounge, tool_stream=tool_sphere_environmental) \
        .modify(Component('motion-S1_L')) \
        .relative_window(relative_window)
    
    tool_m_lounge_avg = T[id_tool_aggregate].define(
        input_streams=[
            S[id_sphere_m_lounge]
        ],
        timer=M[id_memory_every30s],
        func=online_average
    )
    
    M.create_stream(stream_id=id_memory_m_lounge_mean, tool_stream=tool_m_lounge_avg)
    
    for kk, vv in M[id_memory_m_lounge_mean].window(output_interval).iteritems():
        print "{}: {}".format(kk, vv)
