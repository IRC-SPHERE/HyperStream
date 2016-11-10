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

import datetime
import pytz

from hyperstream import HyperStream, TimeInterval, TimeIntervals
from hyperstream.stream import StreamId

from sphere_helpers import PredefinedTools, scripted_experiments, second, minute, hour


def unix2datetime(u):
    return datetime.datetime.fromtimestamp(u / 1000.0, tz=pytz.UTC) + datetime.timedelta(hours=0)


if __name__ == '__main__':
    hyperstream = HyperStream()
    tools = PredefinedTools(hyperstream)
    
    # Create a simple one step workflow for querying
    w = hyperstream.create_workflow(
        workflow_id="localisation_prediction",
        name="localisation from technician's scripted walk through the house, annotation locations",
        owner="NT",
        description="Get the annotations and RSSIs;"
                    "train a localisation model; test the model;"
                    "visualise and mine the results")
    
    # Some plate values for display purposes
    h1 = (('house', '1'),)
    wA = (('wearable', 'A'),)
    
    S = hyperstream.channel_manager.sphere
    M = hyperstream.channel_manager.memory
    
    nodes = (
        ("every_2s", hyperstream.channel_manager.memory, ["H1"]),  # sliding windows one every minute
        ("rss_raw", hyperstream.channel_manager.sphere, ["H1"]),  # Raw RSS data
        ("rss_per_uid", hyperstream.channel_manager.memory, ["H1.W"]),  # Raw RSS data
        ("rss_per_uid_2s", hyperstream.channel_manager.memory, ["H1.W"]),  # max RSS per access point in past 2s of RSS data
        ("location_prediction_models", hyperstream.channel_manager.memory, ["H1"]),
        ("predicted_locations_1", hyperstream.channel_manager.memory, ["H1.W"]),
    )
    
    # Set times for execution
    start_time = datetime.datetime(year=2016, month=10, day=19, hour=12, minute=28, tzinfo=pytz.UTC)
    duration = datetime.timedelta(seconds=20)
    
    end_time = start_time + duration
    time_interval = TimeInterval(start_time, end_time)
    
    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)
    
    multires_importer = hyperstream.channel_manager.get_tool(
        name="multiresident_experiment_importer",
        parameters=dict())
    exp_meta = multires_importer.experiment_metadata
    exp_times = TimeIntervals()
    for (i, row) in exp_meta.iterrows():
        exp_times = exp_times + TimeIntervals(
            [TimeInterval(unix2datetime(row.first_occurrence - 1), unix2datetime(row.last_occurrence))])
    print(exp_times)

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="wearable4")
        ),
        sources=None,
        sink=N["rss_raw"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter",
            parameters=dict(
                element="uid",
                mapping={'a0e6f8ad8504': 'A'})
        ),
        source=N["rss_raw"],
        sink=N["rss_per_uid"])
    
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            parameters=dict(lower=datetime.timedelta(seconds=-2),
                            upper=datetime.timedelta(seconds=0),
                            increment=datetime.timedelta(seconds=2))
        ),
        sources=None,
        sink=N["every_2s"])
    
    def component_wise_max(init_value=None, id_field='aid', value_field='rss'):
        if init_value is None:
            init_value = {}
        
        def func(data):
            result = init_value.copy()
            for (time, value) in data:
                if result.has_key(value[id_field]):
                    result[value[id_field]] = max(result[value[id_field]], value[value_field])
                else:
                    result[value[id_field]] = value[value_field]
            return result
        
        return func
    
    
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_apply",
            parameters=dict(func=component_wise_max())
        ),
        sources=[N["every_2s"], N["rss_per_uid"]],
        sink=N["rss_per_uid_2s"])

    w.execute(time_interval)
    
    sid = StreamId(name='rss_per_uid_2s', meta_data=(('house', '1'), ('wearable', 'A')))
    for doc in M[sid].window(time_interval):
        print doc

    exit()
    
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="localisation_model_predict",
            parameters=dict()
        ),
        sources=[N['location_prediction_models'], N["rss_2s"]],
        sink=N["predicted_locations_1"])
    
    w.execute(time_interval)
    
    sid = StreamId('predicted_locations', h1)
    for doc in hyperstream.channel_manager.mongo[sid].window(time_interval):
        print doc
