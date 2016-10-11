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
from hyperstream.utils import UTC

from sphere_connector_package.sphere_connector import SphereLogger


motion_sensors = {
    "bedroom 1": "motion-S1_B1",
    "study": "motion-S1_B0",
    "bedroom 2": "motion-S1_B2",
    "bathroom": "motion-S1_BR",
    "hallway": "motion-S1_H",
    "kitchen": "motion-S1_K",
    "lounge": "motion-S1_L",
    "stairs": "motion-S1_S",
    "toilet": "motion-S1_WC"
}

# Mapping from access point to location, and wearable uid to wearable name
mappings = {
    "aid": {
        # New access points and sensor locations
        "b827eb26dbd6": "hallway",
        "b827eb643ba7": "lounge",
        "b827eb0654a9": "study",
        "b827eb48a755": "kitchen",
        "b827eb645a8e": "kitchen",
        "b827eb62fe45": "hallway",
        "b827eb036271": "lounge",
        "b827ebfd0967": "bedroom 1",
        "b827ebcb1413": "bedroom 1",
        "b827eb3f1106": "bedroom 2",
        # Old access points
        "b827ebea7bc4": "lounge",
        "b827eb94cbd1": "kitchen",
        "b827eb524fec": "study"
    },
    'uid': {
        "0aa05543a5c2": "A"
    }
}

# Analysis of data from:
# http://10.70.18.138/data_viewer/scripted/001/2/Location/
# Experiment id: 001   Start: 2015-08-06T13:35:36.035000Z   End: 2015-08-06T14:12:22.008000Z
# Offsets: 0:01:06, 0:01:06, 0:01:06   Annotators: 2, 3, 5
# Annotation files: [ S1060001_jdl.eaf | S1060001_ac.eaf | S1060001_ks.eaf ]
# Mongo query: db.annotations.find({tier: 'Location', start: {$gt: ISODate('2015-08-06T13:35:36.035000Z')},
# end: {$lte: ISODate('2015-08-06T14:12:22.008000Z')}})

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
    t1 = datetime(2015, 8, 6, 13, 35, 36, 35000, UTC)
    # TODO: Shortened time interval for now
    # t2 = datetime(2015, 8, 6, 14, 12, 22, 8000, UTC)
    second = timedelta(seconds=1)
    minute = timedelta(minutes=1)
    hour = timedelta(hours=1)
    t2 = t1 + 1 * minute

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
        workflow_id="localisation",
        name="Test of localisation",
        owner="TD",
        description="Would like to test localisation using PIR and RSSI, so we need to first get the appropriate data"
                    "out of the SPHERE stream, with the appropriate meta-data attached, and then use a model")

    # 5 minutes of data to start with
    time_interval = TimeInterval(t1, t2)

    # PLATES:

    # House 1 plate
    h1 = w.plates["H1"]
    key = h1.values[0]

    # Location plate
    # Here we used the splitter tool over the RSS data to generate the plate
    n_rss_flat = w.create_node(stream_name="rss", channel=S, plate_ids=["H1"])
    w.create_factor(tool_name="sphere", tool_parameters=dict(modality="wearable", elements=("rss",)),
                    source_nodes=None, sink_node=n_rss_flat, alignment_node=None).execute(time_interval)

    n_rss_flat.print_head(key, time_interval)

    n_rss_aid = w.create_node(stream_name="rss_aid", channel=M, plate_ids=["H1.L"])
    w.create_multi_output_factor(tool_name="splitter", tool_parameters=dict(element="aid", mapping=mappings["aid"]),
                                 source_node=n_rss_flat, sink_node=n_rss_aid).execute(time_interval)

    # w.execute(time_interval)
    # for loc in w.plates["H1.L"].values:
    for loc in ["kitchen", "hallway", "lounge"]:
        print(loc)
        n_rss_aid.print_head((("house", "1"), ("location", loc)), time_interval)
        print("")

    n_rss_aid_uid = w.create_node(stream_name="rss_aid_uid", channel=M, plate_ids=["H1.L.W"])
    w.create_multi_output_factor(tool_name="splitter", tool_parameters=dict(element="uid", mapping=mappings["uid"]),
                                 source_node=n_rss_aid, sink_node=n_rss_aid_uid).execute(time_interval)

    # for loc in w.plates["H1.L"].values:
    for loc in ["kitchen", "hallway", "lounge"]:
        print(loc)
        n_rss_aid_uid.print_head((("house", "1"), ("location", loc), ('wearable', 'A')), time_interval)
        print("")

    n_rss = w.create_node(stream_name="rss", channel=M, plate_ids=["H1.L.W"])
    w.create_factor(tool_name="component", tool_parameters=dict(key="wearable-rss"),
                    source_nodes=[n_rss_aid_uid], sink_node=n_rss, alignment_node=None).execute(time_interval)

    # for loc in w.plates["H1.L"].values:
    for loc in ["kitchen", "hallway", "lounge"]:
        print(loc)
        n_rss.print_head((("house", "1"), ("location", loc), ('wearable', 'A')), time_interval)
        print("")


    # Load in annotations from the same time period

    exit(0)

    # PIR sensor plate

    # Stream to get motion sensor data
    n_pir = w.create_node(stream_name="environmental_db", channel=D, plate_ids=["H1"])
    f_pir = w.create_factor(tool_name="sphere", tool_parameters=dict(modality="environmental", elements=("motion",)),
                            source_nodes=None, sink_node=n_pir, alignment_node=None)

    # Stream to get RSS data
    n_rss_aid = w.create_node(stream_name="wearable", channel=S, plate_ids=["H1"])
    f_rss = w.create_factor(tool_name="sphere", tool_parameters=dict(modality="wearable", elements=("rss",)),
                            source_nodes=None, sink_node=n_rss_aid, alignment_node=None)


    # Execute the workflow
    w.execute(time_interval)

    print(n_pir.streams[('house', '1'), ].window(time_interval).values()[0:5])
    print(n_rss_aid.streams[('house', '1'),].window(time_interval).values()[0:5])

