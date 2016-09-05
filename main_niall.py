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
    id_tool_clock = StreamId('clock')
    id_tool_aggregate = StreamId('aggregate')
    id_tool_apply = StreamId('apply')
    
    # Define tool streams: these do not need to be duplicated over plates
    tool_clock_30s = w.channels.tools[id_tool_clock].define(stride=timedelta(seconds=30))
    tool_sphere_environmental = w.channels.tools[id_tool_sphere].define(modality='environmental')
    tool_apply_ceil = w.channels.tools[id_tool_apply].define(ceil)
    
    # Create a clock that ticks every 30 seconds
    id_memory_every30s = StreamId('timer_every30s')
    w.channels.memory.create_stream(
        stream_id=id_memory_every30s,
        tool_stream=tool_clock_30s
    )
    
    w.create_streams(
        channel=w.channels.sphere,
        stream_name='id_sphere_m_kitchen',
        plate_ids=('H',),
        tool_stream=tool_sphere_environmental
    ).modify(
        Component('motion-S1_K')
    ).relative_window(
        relative_window
    )

    for stream in w['id_sphere_m_kitchen'].streams:
        print stream
        for kk, vv in stream.window(interval).iteritems():
            print '', kk, vv
        print
    print
    
    def aggregate_gen(streams):
        return w.channels.tools[id_tool_aggregate].define(
            input_streams=streams,
            timer=w.channels.memory[id_memory_every30s],
            func=online_average
        )
    
    
    tool_m_kitchen_avg = w.define_tool_and_create_streams(
        tooling_callback=aggregate_gen,
        output_channel=w.channels.memory,
        output_stream_name='id_memory_m_kitchen_mean',
        plate_ids=('H',),
        node_names=[
            'id_sphere_m_kitchen',
        ],
    )
    
    
    # def ceil_applier(streams):
    #     return w.channels.tools[id_tool_apply].define(
    #         input_streams=streams,
    #         func=ceil
    #     )
    #
    #
    # tool_m_kitchen_plus_10 = w.define_tool_and_create_streams(
    #     tooling_callback=ceil_applier,
    #     output_channel=w.channels.memory,
    #     output_stream_name='id_plus_10',
    #     plate_ids=('H',),
    #     node_names=[
    #         'id_memory_m_kitchen_mean'
    #     ]
    # )
    #
    # for node_name, node in w.nodes.iteritems():
    #     print node
    #     for stream in node.streams:
    #         print stream
    #         for kk, vv in stream.window(interval).iteritems():
    #             print '', kk, vv
    #         print
    #     print
