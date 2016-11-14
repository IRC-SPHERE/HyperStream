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

from __future__ import print_function

import datetime

import pytz

from hyperstream import HyperStream, TimeInterval, TimeIntervals
from hyperstream.stream import StreamId
from hyperstream.utils import unix2datetime
from plugins.sphere.utils.sphere_helpers import PredefinedTools

# Analysis of data from:
# http://10.70.18.138/data_viewer/scripted/001/2/Location/
# Experiment id: 001   Start: 2015-08-06T13:35:36.035000Z   End: 2015-08-06T14:12:22.008000Z
# Offsets: 0:01:06, 0:01:06, 0:01:06   Annotators: 2, 3, 5
# Annotation files: [ S1060001_jdl.eaf | S1060001_ac.eaf | S1060001_ks.eaf ]
# Mongo query: db.annotations.find({tier: 'Location', start: {$gt: ISODate('2015-08-06T13:35:36.035000Z')},
# end: {$lte: ISODate('2015-08-06T14:12:22.008000Z')}})


if __name__ == '__main__':
    hyperstream = HyperStream()

    tools = PredefinedTools(hyperstream)

    # Various channel managers
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    T = hyperstream.channel_manager.tools
    D = hyperstream.channel_manager.mongo

    # Create a simple one step workflow for querying
    w = hyperstream.create_workflow(
        workflow_id="localisation_from_tech_script",
        name="localisation from technician's scripted walk through the house, annotation locations",
        owner="MK",
        description="Get the annotations and RSSIs;"
                    "train a localisation model; test the model;"
                    "visualise and mine the results")

    nodes = (
        ("every_2s",                    M, ["H1"]),  # sliding windows one every minute
        ("rss_raw",                     S, ["H1"]),  # Raw RSS data
        ("annotations_raw",             S, ["H1"]),  # Raw annotation data
        ("rss_2s",                      M, ["H1"]),  # max RSS per access point in past 2s of RSS data
        ("annotation_state",            M, ["H1"]),  # Current annotation data in 2s windows
        ("annotation_state_2s_windows", M, ["H1"]),  # Current annotation data in 2s windows
        ("merged_2s",                   M, ["H1"]),  # Current annotation data in 2s windows
        ("dataframe",                   M, ["H1"]),  # Current annotation data in 2s windows
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    multires_importer = hyperstream.channel_manager.get_tool(
        name="multi_resident_experiment_importer",
        parameters=dict())
    exp_meta = multires_importer.experiment_metadata
    exp_times = TimeIntervals()
    for (i, row) in exp_meta.iterrows():
        exp_times = exp_times + TimeIntervals([TimeInterval(unix2datetime(row.first_occurrence - 1),
                                                            unix2datetime(row.last_occurrence))])
    print(exp_times)

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="wearable4")
        ),
        sources=None,
        sink=N["rss_raw"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            parameters=dict(lower=-2.0, upper=0.0, increment=2.0)
        ),
        sources=None,
        sink=N["every_2s"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="annotations", annotators=[0], elements={"Location"}, filters={})
        ),
        sources=None,
        sink=N["annotations_raw"]
    )
    start_time = datetime.datetime(year=2016, month=10, day=18, hour=1, tzinfo=pytz.UTC)
    ti_start = datetime.datetime(year=2016, month=10, day=19, hour=12, minute=30, tzinfo=pytz.UTC)
    ti_start = datetime.datetime(year=2016, month=10, day=19, hour=12, minute=28, tzinfo=pytz.UTC)
    duration = datetime.timedelta(minutes=12)
    duration = datetime.timedelta(minutes=72)
    end_time = ti_start + duration
    #    end_time = datetime.datetime(year=2016,month=10,day=19,hour=23,tzinfo=pytz.UTC)
    time_interval = TimeInterval(ti_start, end_time)
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="annotation_state",
            parameters=dict(start_time=start_time)
        ),
        sources=[N["every_2s"], N["annotations_raw"]],
        sink=N["annotation_state"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="aligning_window",
            parameters=dict(lower=-2.0, upper=0.0)
        ),
        sources=[N["annotation_state"]],
        sink=N["annotations_state_2s_windows"]
    )


    def component_wise_max(init_value={}, id_field='aid', value_field='rss'):
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
        sources=[N["annotations_state_2s_windows"], N["rss_raw"]],
        sink=N["rss_2s"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="aligned_merge",
            parameters=dict(names=["annotations", "rssi"])
        ),
        sources=[N["annotation_state"], N["rss_2s"]],
        sink=N["merged_2s"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="dallan_data_frame_builder",
            parameters=dict(time_interval=TimeInterval(start_time, end_time))
        ),
        sources=[N["merged_2s"]],
        sink=N["dataframe"]
    )

    w.execute(time_interval)

    print('number of non_empty_streams: {}'.format(len(M.non_empty_streams)))

    stream = M.data[StreamId(name="annotation_state", meta_data=(("house", "1"),))]
    for t in sorted(stream):
        print('{} : {}'.format(t, stream[t]))

    stream2 = M.data[StreamId(name="dataframe", meta_data=(("house", "1"),))]
    df = stream2[stream2.keys()[0]]

    df.to_csv("dallan_loc_dataframe.csv", sep="\t")
