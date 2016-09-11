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
import numpy as np
from scipy import integrate

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
    # TODO: Shorted time interval for now
    # t2 = datetime(2016, 4, 29, 12, 0, 0, 0, UTC)
    t2 = datetime(2016, 4, 28, 20, 1, 0, 0, UTC)
    second = timedelta(seconds=1)
    minute = timedelta(minutes=1)
    hour = timedelta(hours=1)

    key = (('house', '1'), )

    # Various channels
    M = channels.memory
    S = channels.sphere
    T = channels.tools
    D = channels.mongo

    # Create a simple one step workflow for querying
    w = Workflow(
        channels=channels,
        plates=plates,
        workflow_id="bbc_workflow",
        name="BBC workflow",
        owner="WP5",
        description="Workflow to analyse data from the BBC stay in the SPHERE house")

    time_interval = TimeInterval(t1, t2)

    # Get environmental streams from the database
    # TODO: TD Changed the plate from H1.L to H1 for now
    n_environmental = w.create_node(stream_name="environmental", channel=S, plate_ids=["H1"])
    factor = w.create_factor(tool_name="sphere", tool_parameters=dict(modality="environmental"),
                             source_nodes=None, alignment_node=None, sink_node=n_environmental).execute(time_interval)

    print("Environmental")
    for k, v in n_environmental.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    # Get wearable streams from the database
    n_wearable = w.create_node(stream_name="wearable", channel=S, plate_ids=["H1"])
    factor = w.create_factor(tool_name="sphere", alignment_node=None, tool_parameters=dict(modality="wearable"),
                             source_nodes=None, sink_node=n_wearable).execute(time_interval)

    print("Wearable")
    for k, v in n_wearable.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    # Get RSSI stream from the database
    n_rssi = w.create_node(stream_name="rssi", channel=S, plate_ids=["H1"])
    factor = w.create_factor(tool_name="sphere", tool_parameters=dict(modality="wearable", elements={"rss"}),
                             source_nodes=None, alignment_node=None, sink_node=n_rssi).execute(time_interval)

    print("RSSI")
    for k, v in n_rssi.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    # Get the clock ticks every 10s to perform sliding window averaging
    n_clock_10s = w.create_node(stream_name="clock_10s", channel=M, plate_ids=None)
    factor = w.create_factor(tool_name="clock", tool_parameters=dict(stride=10*second),
                             source_nodes=None, alignment_node=None, sink_node=n_clock_10s).execute(time_interval)

    print("Clock 10s")
    for k, v in n_clock_10s.streams[None].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    # Perform sliding window aggregation on each of the environmental streams
    environmental_aggregators = {
        'humidity': online_average,
        'dust': online_average,
        'noise': online_average,
        'temp': online_average,
        'pir': max,
        'coldwater': integrate.quad,
        'hotwater': integrate.quad,
        'electricity_tv': max,
        'electricity_total': max,
    }

    # TODO: TD Changed the plate from H1.L to H1 for now
    n_environmental_rw = w.create_node(stream_name='environmental_rw', channel=M, plate_ids=['H1'])
    w.create_factor(tool_name="relative_window",
                    tool_parameters=dict(relative_start=-10, relative_end=0),
                    source_nodes=[n_environmental],
                    alignment_node=n_clock_10s,
                    sink_node=n_environmental_rw).execute(time_interval)

    print("Environmental relative window")
    for k, v in n_environmental_rw.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    # TODO: TD Changed the plate from H1.L to H1 for now
    n_environmental_10s = w.create_node(stream_name="environmental_10s", channel=M, plate_ids=["H1"])
    w.create_factor(tool_name="relative_apply2",
                    tool_parameters=dict(func=lambda kk, vv: environmental_aggregators[kk](vv)),
                    source_nodes=[n_environmental_rw],
                    alignment_node=None,
                    sink_node=n_environmental_10s).execute(time_interval)

    print("Environmental 10s aggregates")
    for k, v in n_environmental_10s.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    # Get humidity component
    # TODO: TD Changed the plate from H1.L to H1 for now
    n_humid = w.create_node(stream_name="humid", channel=M, plate_ids=["H1"])
    w.create_factor(tool_name="component",
                    tool_parameters=dict(key="humidity"),
                    source_nodes=[n_environmental_10s],
                    alignment_node=n_clock_10s,
                    sink_node=n_humid).execute(time_interval)

    print("Humidity")
    for k, v in n_humid.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    # Create relative window over the humidity
    # TODO: TD Changed the plate from H1.L to H1 for now
    n_humid_10s = w.create_node(stream_name="humid_10s", channel=M, plate_ids=["H1"])
    w.create_factor(tool_name="relative_window",
                    tool_parameters=dict(relative_start=-10.001, relative_end=0),
                    source_nodes=[n_humid],
                    alignment_node=n_clock_10s,
                    sink_node=n_humid_10s).execute(time_interval)

    print("Humidity 10s aggregate")
    for k, v in n_humid_10s.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    # TODO: why does this difference need an alignment node?

    def func(x):
        return np.diff(x)

    # Calculate humidity differences for each of the humidity sensors
    # TODO: TD Changed the plate from H1.L to H1 for now
    n_humid_diff_10s = w.create_node(stream_name="humid_diff_10s", channel=M, plate_ids=["H1"])
    w.create_factor(tool_name="relative_apply", tool_parameters=dict(func=func),  # lambda x: np.diff(x)),
                    source_nodes=[n_humid_10s],
                    alignment_node=n_clock_10s,
                    sink_node=n_humid_diff_10s).execute(time_interval)

    print("Humidity differences")
    for k, v in n_humid_diff_10s.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    # Perform sliding window aggregation on the RSSI stream
    rssi_aggregators = {
        'kitchen': 'mean',
        'lounge': 'mean',
        'hallway': 'mean',
    }

    n_rssi_10s = w.create_node(stream_name="rssi_10s", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool_name="relative_window",
                             tool_parameters=dict(relative_start=-10, relative_end=0),
                             source_nodes=[n_rssi],
                             alignment_node=n_clock_10s, sink_node=n_rssi_10s).execute(time_interval)

    print("RSSI relative window")
    for k, v in n_rssi_10s.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    n_rssi_10s_mean = w.create_node(stream_name="rssi_10s_mean", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool_name="relative_apply",
                             tool_parameters=dict(func=lambda x: np.mean(x)),
                             source_nodes=[n_rssi_10s],
                             alignment_node=None, sink_node=n_rssi_10s_mean).execute(time_interval)

    print("RSSI 10s average")
    for k, v in n_rssi_10s_mean.streams[key].window(time_interval).head(10):
        print("{}, {}".format(k, v))

    raise Exception("TD: Executing up to here")

    # Perform localisation based on the RSSI stream
    n_localisation_10s = w.create_node(stream_name="localisation_10s", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool_name="localiser", tool_parameters=dict(),
                             source_nodes=[n_rssi_10s], alignment_node=n_clock_10s, sink_node=n_localisation_10s).execute(time_interval)

    # Perform Van Hees inactivity prediction based on the wearable stream
    n_van_hees_10s = w.create_node(stream_name="van_hees_10s", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool_name="van_hees_algorithm", tool_parameters=dict(),
                             source_nodes=[n_wearable.rel_window(-10,0)],
                             alignment_node=n_clock_10s,
                             sink_node=n_van_hees_10s).execute(time_interval)  # rel_window might have some other size

    # Apply rules to predict activities
    n_activities_10s = w.create_node(stream_name="activities_10s", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool_name="activity_recogniser", tool_parameters=dict(),
                             source_nodes=[n_localisation_10s,n_environmental_10s,n_humid_diff_10s,n_van_hees_10s],
                             alignment_node=n_clock_10s, sink_node=n_activities_10s).execute(time_interval)

    # Get the clock ticks every 5m to perform sliding window averaging
    n_clock_5m = w.create_node(stream_name="clock_5m", channel=M, plate_ids=[])
    factor = w.create_factor(tool_name="clock", tool_parameters=dict(stride=5*minute),
                             source_nodes=None, alignment_node=None, sink_node=n_clock_5m).execute(time_interval)

    # Aggregate activity predictions within 5min periods
    n_activities_5m = w.create_node(stream_name="activities_5m", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool_name="sliding_window_aggregator", tool_parameters=dict(agg_functions='mode'),
                             source_nodes=[n_activities_10s.rel_window(-5*60,0)], alignment_node=n_clock_5m,
                             sink_node=n_activities_5m).execute(time_interval)

    # Produce a summary
    n_bbc_summary = w.create_node(stream_name="bbc_summary", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool_name="bbc_summariser", tool_parameters=dict(),
                             source_nodes=[n_activities_5m.abs_window(t1,t2)],
                             alignment_node=None, sink_node=n_bbc_summary).execute(time_interval)

# dts$a_watchtv = 1*((dts$room=="Living.Room") & (dts$electv>=0.2))
# dts$a_work = 1*((dts$room=="Living.Room") & (dts$electv<0.5) & (dts$mins>=30))
# dts$a_sleep = 1*((dts$inactive>=0.2) & (dts$room=="Bedroom") & (dts$mins>300))
# dts$a_shower = 1*((dts$inactive>=0.2) & (dts$bathroom_humdiff>=10) & (dts$electot>=0.2))
# dts$a_bathe = 1*((dts$inactive>=0.2) & (dts$bathroom_humdiff>=10) & (dts$electot<0.2))
# dts$a_mealprep = 1*((dts$room=="Kitchen") & (dts$kitchen_water>=1) & (dts$electot>=0.2))

    # Execute the workflow
    w.execute(time_interval)

    # Check the values
#    assert (node.streams[('house', '1'), ].window(time_interval).values()[:1] ==
#            [{u'electricity-04063': 0.0, 'noise': None, 'door': None, 'uid': u'04063', 'electricity': None,
#              'light': None, 'motion': None, 'dust': None, 'cold-water': None, 'humidity': None, 'hot-water': None,
#              'temperature': None}])
