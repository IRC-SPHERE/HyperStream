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
    TimeInterval  # , StreamView
from hyperstream.utils import UTC, MIN_DATE
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

    # TODO: We could make the __getitem__ accept str and do the following, but for now it only accepts StringId
    environmental = StreamId(name='environmental', meta_data={'house': '1'})
    clock = StreamId('clock')
    aggregate = StreamId('aggregate')
    every30s = StreamId('every30s')
    motion_kitchen_windowed = StreamId('motion_kitchen_windowed')
    env_kitchen_30_s_window = StreamId('env_kitchen_30_s_window')
    m_kitchen_30_s_window = StreamId('m_kitchen_30_s_window')
    average = StreamId('average')  # , meta_data={'house': '1'})
    count = StreamId('count')
    # sum_ = StreamId('sum')
    sphere = StreamId('sphere')
    component = StreamId('component')

    # Create a simple one step workflow for querying
    w = Workflow(
        channels=channels,
        plates=plates,
        workflow_id="simple_query_workflow",
        name="Simple query workflow",
        owner="TD",
        description="Just a test of creating workflows")

    time_interval = TimeInterval(t1, t1 + 1 * minute)

    h1 = w.plates["H1"]

    # Create some streams (collected in a node)
    node = w.create_node(stream_name="environmental", channel=S, plate_ids=["H1"])  # .window((t1, t1 + 1 * minute))

    # Create a factor to produce some data
    factor = w.create_factor(tool_name="sphere", tool_parameters=dict(modality="environmental"),
                             source_nodes=None, sink_node=node)
    # sources=None, sink=StreamView(stream=node.streams[0], relative_time_interval=relative_time_interval))

    # Execute the workflow
    w.execute(time_interval)

    # Check the values
    assert (node.streams[('house', '1'), ].window(time_interval).values()[:1] ==
            [{u'electricity-04063': 0.0, 'noise': None, 'door': None, 'uid': u'04063', 'electricity': None,
              'light': None, 'motion': None, 'dust': None, 'cold-water': None, 'humidity': None, 'hot-water': None,
              'temperature': None}])

    #############################################################
    # More complex workflow
    w = Workflow(
        channels=channels,
        plates=plates,
        workflow_id="windowed_query_workflow",
        name="Windowed query workflow",
        owner="TD",
        description="Another test of creating workflows")

    houses = w.plates["H1"]

    # Create the nodes using pre-existing streams in the database
    n_environ = w.create_node(stream_name="environmental", channel=S, plate_ids=["H1"])
    n_clock = w.create_node(stream_name="clock", channel=M, plate_ids=None)
    n_motion_kitchen = w.create_node(stream_name="m_kitchen_30_s_window", channel=M, plate_ids=["H1"])
    n_motion_kitchen_count = w.create_node(stream_name="motion_kitchen_count", channel=M, plate_ids=["H1"])

    # Create the clock factor
    f_timer = w.create_factor(
        tool_name="clock",
        tool_parameters=dict(first=MIN_DATE, stride=30 * second),
        source_nodes=None,
        sink_node=n_clock
        # sink=StreamView(n_clock.streams[0])
    )

    # TODO: Something along the following lines
    # with w.for_each(houses.values) as house:
    #     pass

    # for house in houses.values:
        # for house in [[]]:
    f_env = w.create_factor(
        tool_name="sphere",
        tool_parameters=dict(modality="environmental"),
        source_nodes=None,
        sink_node=n_environ
        # sink=StreamView(n_environ.streams[0])
    )

    f_motion = w.create_factor(
        tool_name="component",
        tool_parameters=dict(key="motion-S1_K"),
        source_nodes=[n_environ],
        sink_node=n_motion_kitchen
        # sources=[StreamView(s) for s in n_environ.streams],
        # sink=StreamView(n_motion_kitchen.streams[0])
    )

    f_kitchen_motion = w.create_factor(
        tool_name="aggregate",
        tool_parameters=dict(func=online_count),
        source_nodes=[n_clock, n_motion_kitchen],
        sink_node=n_motion_kitchen_count
        # sources=[StreamView(n_clock.streams[0]), StreamView(n_motion_kitchen.streams[0])],
        # sink=StreamView(n_motion_kitchen_count.streams[0])
    )

    ti = TimeInterval(t1, t1 + 5 * minute)
    w.execute(ti)

    print(n_motion_kitchen_count.streams[('house', '1'), ].window(ti).values())

    # exit()

    #############################################################
    # Simple querying
    ti = TimeInterval(t1, t1 + minute)
    env = S[environmental]
    eid = StreamId('electricity')
    elec_tool = channels.get_tool(tool=component, tool_parameters=dict(key='electricity-04063'))
    elec = M.create_stream(stream_id=eid)

    elec_tool.execute(interval=ti, sources=[env], sink=elec)

    el = M[eid].window(ti)

    q1 = "\n".join("=".join(map(str, ee)) for ee in el)

    print(q1)
    print(el.values())

    t_clock = channels.get_tool(tool=clock, tool_parameters=dict(first=MIN_DATE, stride=30 * second))
    t_env = channels.get_tool(tool=sphere, tool_parameters=dict(modality='environmental'))

    # Windowed querying
    M.create_stream(stream_id=every30s)

    ti = TimeInterval(t1, t2)
    t_clock.execute(interval=ti, sources=None, sink=M[every30s])

    print("\n----------")
    print("M[every30s]")
    print("\n".join(map(str, M[every30s].window(ti))))
    print("----------")
    print("")

    M.clear_stream(every30s)
    t_motion = channels.get_tool(tool=component, tool_parameters=dict(key='motion-S1_K'))
    M.create_stream(stream_id=m_kitchen_30_s_window)

    t_motion.execute(interval=ti, sources=[S[environmental]], sink=M[m_kitchen_30_s_window])

    print("\n------------------------")
    print("S[m_kitchen_30_s_window]")
    print("\n".join(map(str, M[m_kitchen_30_s_window].window(ti))))
    print("------------------------")
    print("")

    # Want to be able to reuse the same aggregate tool, but with different parameters (in this case func)
    # Since define() just returns back the object, we have an issue that a new object isn't being created
    # Note that in fact T[aggregate] is returning a stream, and the items() function of the stream just returns a
    # single element, which is the tool class, pre-built with its kwargs
    # So we basically want to have a new stream being created by define, rather than the same one getting reused.
    # Since define is only used for this purpose, there shouldn't be a problem?
    # Hence it seems reasonable to simply use copy.deepcopy when returning the stream object

    t_average = channels.get_tool(tool=aggregate, tool_parameters=dict(func=online_average))
    t_count = channels.get_tool(tool=aggregate, tool_parameters=dict(func=online_count))

    # Streams to hold the data
    M.create_stream(stream_id=average)
    M.create_stream(stream_id=count)

    t_clock.execute(interval=ti, sources=None, sink=M[every30s])

    t_average.execute(
        interval=ti, sources=[
            M[every30s].relative_window((-30 * second, timedelta(0))),
            M[m_kitchen_30_s_window]],
        sink=M[average])

    t_count.execute(
        interval=ti,
        sources=[
            M[every30s].relative_window((-30 * second, timedelta(0))),
            M[m_kitchen_30_s_window]],
        sink=M[count])

    aa = M[average].window(ti).values()
    print(aa)
    assert (aa == [0.0, 0.25, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

    cc = M[count].window(ti).values()
    print(cc)
    assert (cc == [3, 4, 4, 3, 3, 3, 3, 3, 3, 3])
