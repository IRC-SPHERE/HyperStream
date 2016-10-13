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

from hyperstream import ChannelManager, HyperStreamConfig, StreamId, Workflow, PlateManager, WorkflowManager, Client, \
    TimeInterval
from hyperstream.utils import UTC, MIN_DATE
from hyperstream.itertools2 import count as online_count
from sphere_connector_package.sphere_connector import SphereLogger
from sphere_helpers import PredefinedTools


if __name__ == '__main__':
    # TODO: hyperstream needs it's own logger (can be a clone of this one)
    sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.DEBUG)

    hyperstream_config = HyperStreamConfig()
    client = Client(hyperstream_config.mongo)

    # Define some managers
    channel_manager = ChannelManager(hyperstream_config.tool_path)
    plate_manager = PlateManager(hyperstream_config.meta_data).plates
    workflow_manager = WorkflowManager(channel_manager=channel_manager, plate_manager=plate_manager)

    tools = PredefinedTools(channel_manager)

    # Various constants
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
    t2 = t1 + timedelta(minutes=5)
    second = timedelta(seconds=1)
    minute = timedelta(minutes=1)
    hour = timedelta(hours=1)

    # Various channels
    M = channel_manager.memory
    S = channel_manager.sphere
    T = channel_manager.tools
    D = channel_manager.mongo

    # TODO: We could make the __getitem__ accept str and do the following, but for now it only accepts StringId
    environmental = StreamId(name='environmental', meta_data={'house': '1'})
    clock = StreamId('clock')
    aggregate = StreamId('aggregate')
    every30s = StreamId('every30s')
    motion_kitchen_windowed = StreamId('motion_kitchen_windowed')
    env_kitchen_30_s_window = StreamId('env_kitchen_30_s_window')
    m_kitchen_30_s_window = StreamId('m_kitchen_30_s_window')
    average = StreamId('averager', meta_data={'house': '1'})
    count = StreamId('counter')
    # sum_ = StreamId('sum')
    sphere = StreamId('sphere')
    component = StreamId('component')

    # Create a simple one step workflow for querying
    w = Workflow(
        channels=channel_manager,
        plates=plate_manager,
        workflow_id="simple_query_workflow",
        name="Simple query workflow",
        owner="TD",
        description="Just a test of creating workflows")

    time_interval = TimeInterval(t1, t1 + 1 * minute)

    h1 = w.plates["H1"]

    # Create some streams (collected in a node)
    node = w.create_node(stream_name="environmental", channel=S, plate_ids=["H1"])  # .window((t1, t1 + 1 * minute))

    # Create a factor to produce some data
    factor = w.create_factor(tool_name="sphere", tool_parameters=dict(modality="environmental"), source_nodes=None,
                             sink_node=node, alignment_node=None)
    # sources=None, sink=StreamView(stream=node.streams[0], time_interval=time_interval))

    # Execute the workflow
    w.execute(time_interval)

    # Check the values
    assert (node.streams[('house', '1'), ].window(time_interval).values()[:1] ==
            [{u'electricity-04063': 0.0, 'noise': None, 'door': None, 'uid': u'04063', 'electricity': None,
              'light': None, 'motion': None, 'dust': None, 'cold-water': None, 'humidity': None, 'hot-water': None,
              'temperature': None}])

    # More complex workflow
    w = Workflow(
        channels=channel_manager,
        plates=plate_manager,
        workflow_id="windowed_query_workflow",
        name="Windowed query workflow",
        owner="TD",
        description="Another test of creating workflows")

    #############################################################
    houses = w.plates["H1"]

    # Create the nodes using pre-existing streams in the database
    n_environ = w.create_node(stream_name="environmental", channel=S, plate_ids=["H1"])
    n_clock = w.create_node(stream_name="clock", channel=M, plate_ids=None)
    n_motion_kitchen = w.create_node(stream_name="m_kitchen_30_s_window", channel=M, plate_ids=["H1"])
    n_motion_kitchen_count = w.create_node(stream_name="motion_kitchen_count", channel=M, plate_ids=["H1"])

    # Create the factors
    f_timer = w.create_factor(tool_name="clock", tool_parameters=dict(first=MIN_DATE, stride=30 * second),
                              source_nodes=None, sink_node=n_clock, alignment_node=None)

    f_env = w.create_factor(tool_name="sphere", tool_parameters=dict(modality="environmental"), source_nodes=None,
                            sink_node=n_environ, alignment_node=None)

    f_motion = w.create_factor(tool_name="component", tool_parameters=dict(key="motion-S1_K"), source_nodes=[n_environ],
                               sink_node=n_motion_kitchen, alignment_node=None)

    f_kitchen_motion = w.create_factor(tool_name="aggregate", tool_parameters=dict(func=online_count),
                                       source_nodes=[n_clock, n_motion_kitchen], sink_node=n_motion_kitchen_count,
                                       alignment_node=None)

    ti = TimeInterval(t1, t1 + 5 * minute)
    w.execute(ti)

    print(n_motion_kitchen_count.streams[('house', '1'), ].window(ti).values())
