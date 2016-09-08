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
from hyperstream.tool import ExplicitFactor

from hyperstream.utils.time_utils import MIN_DATE

if __name__ == '__main__':
    # TODO: hyperstream needs it's own logger (can be a clone of this one)
    sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.DEBUG)
    
    hyperstream_config = HyperStreamConfig()
    client = Client(hyperstream_config.mongo)
    
    # Define some managers
    channels = ChannelManager(hyperstream_config.tool_path)
    plates = PlateManager(hyperstream_config.meta_data).plates
    
    # Various channels
    M = channels.memory
    S = channels.sphere
    T = channels.tools
    D = channels.mongo
    
    # A couple of parameters
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
    interval = TimeInterval(t1, t1 + timedelta(minutes=2))
    
    # Define the workflow
    w = Workflow(
        channels=channels,
        plates=plates,
        workflow_id="nt_test",
        name="test",
        owner="nt",
        description="test")
    
    # Define the sliding window
    n_sliding_window = w.create_node(stream_name="sliding_window", channel=M, plate_ids=None)
    f_sliding_window = w.create_factor(
        tool_name="sliding_window",
        tool_parameters=dict(
            first=MIN_DATE,
            lower=timedelta(seconds=-30),
            upper=timedelta(seconds=0),
            increment=timedelta(seconds=10)
        ),
        source_nodes=None,
        sink_node=n_sliding_window
    )
    
    # Define the environmental data
    n_environmental_data = w.create_node(stream_name="environmental", channel=M, plate_ids=["H1"])
    f_environmental_data = w.create_factor(
        tool_name="sphere",
        tool_parameters=dict(modality="environmental"),
        source_nodes=None,
        sink_node=n_environmental_data
    )
    
    # Define the motion data
    n_motion_k = w.create_node(stream_name="motion_k", channel=M, plate_ids=["H1"])
    f_motion_k = w.create_factor(
        tool_name="component",
        tool_parameters=dict(key="motion-S1_K"),
        source_nodes=[n_environmental_data],
        sink_node=n_motion_k
    )
    
    # Define the motion data
    n_motion_l = w.create_node(stream_name="motion_l", channel=M, plate_ids=["H1"])
    f_motion_l = w.create_factor(
        tool_name="component",
        tool_parameters=dict(key="motion-S1_K"),
        source_nodes=[n_environmental_data],
        sink_node=n_motion_l
    )
    
    # Take the mean of the motion stream over a sliding window
    n_average_m_k = w.create_node(stream_name="average_m_k", channel=M, plate_ids=["H1"])
    f_average_m_k = w.create_factor(
        tool_name="sliding_apply",
        tool_parameters=dict(func=online_average),
        source_nodes=[n_sliding_window, n_motion_k],
        sink_node=n_average_m_k
    )
    
    # Take the mean of the motion stream over a sliding window
    n_average_m_l = w.create_node(stream_name="average_m_l", channel=M, plate_ids=["H1"])
    f_average_m_l = w.create_factor(
        tool_name="sliding_apply",
        tool_parameters=dict(func=online_average),
        source_nodes=[n_sliding_window, n_motion_l],
        sink_node=n_average_m_l
    )
    
    n_prod = w.create_node(stream_name='prod', channel=M, plate_ids=["H1"])
    f_prod = w.create_factor(
        tool_name='product',
        tool_parameters={},
        source_nodes=[n_average_m_k, n_average_m_l],
        sink_node=n_prod
    )
    
    # Calculate data
    w.execute(interval)
    for stream_name, stream in n_average_m_k.streams.iteritems():
        print stream_name, stream
        for kv in stream.iteritems():
            print "", kv
        print
        
    for stream_name, stream in n_average_m_l.streams.iteritems():
        print stream_name, stream
        for kv in stream.iteritems():
            print "", kv
        print
