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

from datetime import datetime, timedelta

from hyperstream import HyperStream, TimeInterval, UTC
from plugins.sphere.utils.sphere_helpers import PredefinedTools


if __name__ == '__main__':
    hyperstream = HyperStream()

    tools = PredefinedTools(hyperstream)

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
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    T = hyperstream.channel_manager.tools
    D = hyperstream.channel_manager.mongo

    # Create a simple one step workflow for querying
    w = hyperstream.create_workflow(
        workflow_id="bbc_workflow",
        name="BBC workflow",
        owner="WP5",
        description="Workflow to analyse data from the BBC stay in the SPHERE house")

    time_interval = TimeInterval(t1, t2)

    # Get environmental streams from the database
    # TODO: TD Changed the plate from H1.L to H1 for now
    n_environmental = w.create_node(stream_name="environmental", channel=S, plate_ids=["H1"])
    w.create_factor(tool=tools.environmental, sources=None, sink=n_environmental).execute(time_interval)

    print("Environmental")
    n_environmental.print_head(None, key, time_interval)

    # Get wearable streams from the database
    n_wearable = w.create_node(stream_name="wearable", channel=S, plate_ids=["H1"])
    w.create_factor(tool=tools.wearable, sources=None, sink=n_wearable).execute(time_interval)

    print("Wearable")
    n_wearable.print_head(None, key, time_interval)

    # Get RSS stream from the database
    n_rss = w.create_node(stream_name="rss", channel=S, plate_ids=["H1"])
    w.create_factor(tool=tools.wearable_rss, sources=None, sink=n_rss).execute(time_interval)

    print("Wearable RSS")
    n_rss.print_head(None, key, time_interval)

    # Get the clock ticks every 10s to perform sliding window averaging
    n_clock_10s = w.create_node(stream_name="clock_10s", channel=M, plate_ids=None)
    w.create_factor(tool=tools.clock_10s, sources=None, sink=n_clock_10s).execute(time_interval)

    print("Clock 10s")
    n_clock_10s.print_head(None, None, time_interval)

    # Perform sliding window aggregation on each of the environmental streams

    # TODO: TD Changed the plate from H1.L to H1 for now
    n_environmental_rw = w.create_node(stream_name='environmental_rw', channel=M, plate_ids=['H1'])
    w.create_factor(tool=tools.relative_window_minus10_0, sources=[n_environmental], sink=n_environmental_rw,
                    alignment_node=n_clock_10s).execute(time_interval)

    print("Environmental relative window")
    n_environmental_rw.print_head(None, key, time_interval)

    # TODO: TD Changed the plate from H1.L to H1 for now
    n_environmental_10s = w.create_node(stream_name="environmental_10s", channel=M, plate_ids=["H1"])
    w.create_factor(tool=tools.environmental_relative_apply, sources=[n_environmental_rw],
                    sink=n_environmental_10s).execute(time_interval)

    print("Environmental 10s aggregates")
    n_environmental_10s.print_head(None, key, time_interval)

    # Get humidity component
    # TODO: TD Changed the plate from H1.L to H1 for now
    n_humid = w.create_node(stream_name="humid", channel=M, plate_ids=["H1"])
    w.create_factor(tool=tools.environmental_humidity, sources=[n_environmental_10s],
                    sink=n_humid, alignment_node=n_clock_10s).execute(time_interval)

    print("Humidity")
    n_humid.print_head(None, key, time_interval)

    # Create relative window over the humidity
    # TODO: TD Changed the plate from H1.L to H1 for now
    n_humid_10s = w.create_node(stream_name="humid_10s", channel=M, plate_ids=["H1"])
    w.create_factor(tool=tools.relative_window_minus10_0, sources=[n_humid], sink=n_humid_10s,
                    alignment_node=n_clock_10s).execute(time_interval)

    print("Humidity 10s aggregate")
    n_humid_10s.print_head(None, key, time_interval)

    # TODO: why does this difference need an alignment node?

    # Calculate humidity differences for each of the humidity sensors
    # TODO: TD Changed the plate from H1.L to H1 for now
    n_humid_diff_10s = w.create_node(stream_name="humid_diff_10s", channel=M, plate_ids=["H1"])
    w.create_factor(tool=tools.relative_apply_diff, sources=[n_humid_10s],
                    sink=n_humid_diff_10s, alignment_node=n_clock_10s).execute(time_interval)

    print("Humidity differences")
    n_humid_diff_10s.print_head(None, key, time_interval)

    # Perform sliding window aggregation on the RSS stream
    rss_aggregators = {
        'kitchen': 'mean',
        'lounge': 'mean',
        'hallway': 'mean',
    }

    # TODO: Needs to be separated into plates
    n_rss_vals = w.create_node(stream_name="rss_vals", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool=tools.wearable_rss_values, sources=[n_rss], sink=n_rss_vals).execute(time_interval)

    print("RSS values only")
    n_rss_vals.print_head(None, key, time_interval)

    n_rss_10s = w.create_node(stream_name="rss_10s", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool=tools.relative_window_minus10_0, sources=[n_rss_vals], sink=n_rss_10s,
                             alignment_node=n_clock_10s).execute(time_interval)

    print("RSS relative window")
    n_rss_10s.print_head(None, key, time_interval)

    n_rss_10s_mean = w.create_node(stream_name="rss_10s_mean", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool=tools.relative_apply_mean, sources=[n_rss_10s], 
                             sink=n_rss_10s_mean).execute(time_interval)

    print("RSS 10s average")
    n_rss_10s_mean.print_head(None, key, time_interval)

    raise Exception("TD: Executing up to here")

    # Perform localisation based on the RSS stream
    n_localisation_10s = w.create_node(stream_name="localisation_10s", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool="location_predictor", sources=[n_rss_10s], sink=n_localisation_10s,
                             alignment_node=n_clock_10s).execute(time_interval)

    # Perform Van Hees inactivity prediction based on the wearable stream
    n_van_hees_10s = w.create_node(stream_name="van_hees_10s", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool="van_hees_algorithm", sources=[n_wearable.rel_window(-10, 0)], sink=n_van_hees_10s,
                             alignment_node=n_clock_10s).execute(time_interval)  # rel_window might have some other size

    # Apply rules to predict activities
    n_activities_10s = w.create_node(stream_name="activities_10s", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool="activity_recogniser",
                             sources=[n_localisation_10s, n_environmental_10s, n_humid_diff_10s, n_van_hees_10s],
                             sink=n_activities_10s, alignment_node=n_clock_10s).execute(time_interval)

    # Get the clock ticks every 5m to perform sliding window averaging
    n_clock_5m = w.create_node(stream_name="clock_5m", channel=M, plate_ids=[])
    factor = w.create_factor(tool=tools.clock_5m, sources=None, sink=n_clock_5m).execute(time_interval)

    # Aggregate activity predictions within 5min periods
    n_activities_5m = w.create_node(stream_name="activities_5m", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool="sliding_window_aggregator", sources=[n_activities_10s.rel_window(-5 * 60, 0)],
                             sink=n_activities_5m, alignment_node=n_clock_5m).execute(time_interval)

    # Produce a summary
    n_bbc_summary = w.create_node(stream_name="bbc_summary", channel=M, plate_ids=["H1"])
    factor = w.create_factor(tool="bbc_summariser", sources=[n_activities_5m.abs_window(t1, t2)], sink=n_bbc_summary,
                             ).execute(time_interval)

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
