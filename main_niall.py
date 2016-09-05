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
from math import ceil
from datetime import datetime, timedelta

from hyperstream import ChannelManager, HyperStreamConfig, StreamId, Workflow, PlateManager, WorkflowManager, Client
from hyperstream.time_interval import RelativeTimeInterval
from hyperstream.modifiers import Component, ComponentFilter
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
    
    # Various constants
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
    t2 = t1 + timedelta(minutes=10)
    relative_window = (-timedelta(seconds=30), timedelta(0))
    interval = (t1, t2)
    
    # Basic definition of a workflow
    w = Workflow(
        channels=channels,
        plates=plates,
        workflow_id="nt_multimodal_test",
        name="Test of multimodal performance",
        owner="nt",
        description="Another test of creating workflows"
    )
    
    # Define tool IDs (directory must exist in hyperstream/tools/*)
    id_tool_sphere = StreamId('sphere')
    id_tool_sliding_window = StreamId('sliding_window')
    id_tool_sliding_apply = StreamId('sliding_apply')
    id_tool_apply = StreamId('apply')
    id_tool_component = StreamId('component')
    
    # Define tool streams: these do not need to be duplicated over plates
    tool_sliding_window_30s_10s = w.channels.tools[id_tool_sliding_window].define(
        lower=timedelta(seconds=-30),
        upper=timedelta(seconds=0),
        increment=timedelta(seconds=10)
    )
    
    tool_sphere_environmental = w.channels.tools[id_tool_sphere].define(
        modality='environmental'
    )
    
    tool_apply_ceil = w.channels.tools[id_tool_apply].define(
        func=ceil
    )
    
    # Create a clock that ticks every 30 seconds
    id_memory_sliding_window = StreamId('sliding_window_30s_10s')
    w.channels.memory.create_stream(
        stream_id=id_memory_sliding_window,
        tool_stream=tool_sliding_window_30s_10s
    )
    
    # Create the environmental stream
    w.create_streams(
        channel=w.channels.sphere,
        stream_name='id_sphere_env',
        plate_ids=('H',),
        tool_stream=tool_sphere_environmental
    )
    
    # # Create a component tool to pick out the motion-S1_K sensor
    # tool_component_motion_kitchen = w.channels.tools[id_tool_component].define(
    #     input_streams=w.nodes['id_sphere_env'],
    #     key='motion-S1_K'
    # )
    #
    # # Create the motion in the kitchen stream
    # w.create_streams(
    #     channel=w.channels.memory,
    #     stream_name='id_sphere_m_kitchen',
    #     plate_ids=('H',),
    #     tool_stream=tool_component_motion_kitchen
    # )
    
    tool_m_kitchen_avg = w.create_factor(
        tooling_callback=lambda streams: \
            w.channels.tools[id_tool_component].define(input_streams=streams,
                                                       key='motion-S1_K'),
        output_channel=w.channels.memory,
        output_stream_name='id_memory_m_kitchen',
        plate_ids=('H',),
        node_names=[
            'id_sphere_env'
        ],
    )
    
    
    def aggregate_gen(streams):
        streams = [w.channels.memory[id_memory_sliding_window]] + streams
        
        return w.channels.tools[id_tool_sliding_apply].define(
            input_streams=streams,
            func=online_average
        )
    
    
    w.create_factor(
        tooling_callback=aggregate_gen,
        output_channel=w.channels.memory,
        output_stream_name='id_memory_m_kitchen_mean',
        plate_ids=('H',),
        node_names=[
            'id_memory_sliding_window_30s_10s',
            'id_memory_m_kitchen'
        ],
    )
    
    for node_name, node in w.nodes.iteritems():
        print node
        for stream in node.streams:
            print '\t', stream
            for kk, vv in stream.window(interval).head(5):
                print '\t\t', kk, vv
            print
        print
    
    exit()
    
    
    def ceil_applier(streams):
        return w.channels.tools[id_tool_apply].define(
            input_streams=streams,
            func=ceil
        )
    
    
    factor_m_kitchen_ceil = w.create_factor(
        tooling_callback=ceil_applier,
        output_channel=w.channels.memory,
        output_stream_name='id_memory_m_kitchen_ceil',
        plate_ids=('H',),
        node_names=[
            'id_memory_m_kitchen_mean'
        ]
    )
    
    w.window(interval)
    
    # for node_name, node in w.nodes.iteritems():
    #     print node
    #     for stream in node.streams:
    #         print stream
    #         for kk, vv in stream.window(interval).head(5):
    #             print '', kk, vv
    #         print
    #     print
