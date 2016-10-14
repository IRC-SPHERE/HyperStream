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

from hyperstream import ChannelManager, HyperStreamConfig, Workflow, PlateManager, WorkflowManager, Client, TimeInterval
from hyperstream.utils import UTC

from sphere_connector_package.sphere_connector import SphereLogger
from sphere_helpers import PredefinedTools, scripted_experiments


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
    channel_manager = ChannelManager(hyperstream_config.tool_path)
    plate_manager = PlateManager(hyperstream_config.meta_data)
    workflow_manager = WorkflowManager(channel_manager=channel_manager, plates=plate_manager.plates)

    tools = PredefinedTools(channel_manager)
    #
    # # Various constants
    # t1 = datetime(2015, 8, 6, 13, 35, 36, 35000, UTC)
    # t2 = datetime(2015, 8, 6, 14, 12, 22, 8000, UTC)
    # second = timedelta(seconds=1)
    # minute = timedelta(minutes=1)
    # hour = timedelta(hours=1)
    # # t2 = t1 + 5 * minute

    # Various channel_manager
    M = channel_manager.memory
    S = channel_manager.sphere
    T = channel_manager.tools
    D = channel_manager.mongo

    # Create a simple one step workflow for querying
    w = Workflow(
        channels=channel_manager,
        plates=plate_manager.plates,
        workflow_id="localisation",
        name="Test of localisation",
        owner="TD",
        description="Would like to test localisation using PIR and RSSI, so we need to first get the appropriate data"
                    "out of the SPHERE stream, with the appropriate meta-data attached, and then use a model")

    # time_interval = TimeInterval(t1, t2)

    # TODO just two for now
    for i, (time_interval, annotator_ids) in enumerate(scripted_experiments[:2]):
        plate_manager.create_plate(
            plate_id="H1.scripted_{}".format(i + 1),
            description="Annotators for scripted experiment {} in SPHERE house".format(i + 1),
            meta_data_id="annotator",
            values=[],
            complement=True,
            parent_plate="H1.scripted"
        )

        # Load in annotations from the same time period
        n_annotations_flat = w.create_node(stream_name="annotations_flat", channel=M, plate_ids=["H1.scripted"])

        annotations_location = channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="annotations", annotators=annotator_ids, elements={"Location"},
                            filters={"trigger": 1})
        )

        w.create_factor(
            tool=annotations_location, sources=None, sink=n_annotations_flat).execute(time_interval)\
            .sink.print_head((("house", "1"), ('scripted', str(i + 1))), time_interval)

        plate_id = "H1.scripted_{}".format(i + 1)
        # Put these on to an annotators plate
        n_annotations_split = w.create_node(stream_name="annotations_split", channel=M, plate_ids=[plate_id])
        w.create_multi_output_factor(
            tool=tools.split_annotator, source=n_annotations_flat, sink=n_annotations_split).execute(time_interval)

        # for annotator in annotator_ids:
        #     print("Annotator " + annotator)
        #     n_annotations_split.print_head((("house", "1"), ("annotator", annotator)), time_interval)

        # Pull out the label
        n_annotations = w.create_node(stream_name="annotations", channel=M, plate_ids=[plate_id])
        w.create_factor(tool=tools.annotations_label, sources=[n_annotations_split],
                        sink=n_annotations).execute(time_interval)

        for annotator in annotator_ids:
            print("Annotator " + annotator)
            n_annotations.print_head((("house", "1"), ("annotator", annotator)), time_interval)

        continue

        # Here we used the splitter tool over the RSS data to generate the plate
        n_rss_flat = w.create_node(stream_name="rss", channel=S, plate_ids=["H1"])
        w.create_factor(tool=tools.wearable_rss, sources=None, sink=n_rss_flat).execute(time_interval)

        # n_rss_flat.print_head(key, time_interval)

        n_rss_aid = w.create_node(stream_name="rss_aid", channel=M, plate_ids=["H1.L"])
        w.create_multi_output_factor(tool=tools.split_aid, source=n_rss_flat, sink=n_rss_aid).execute(time_interval)

        # for loc in ["kitchen", "hallway", "lounge"]:
        #     print(loc)
        #     n_rss_aid.print_head((("house", "1"), ("location", loc)), time_interval)
        #     print("")

        n_rss_aid_uid = w.create_node(stream_name="rss_aid_uid", channel=M, plate_ids=["H1.L.W"])
        w.create_multi_output_factor(tool=tools.split_uid, source=n_rss_aid, sink=n_rss_aid_uid).execute(time_interval)

        # for loc in ["kitchen", "hallway", "lounge"]:
        #     print(loc)
        #     n_rss_aid_uid.print_head((("house", "1"), ("location", loc), ('wearable', 'A')), time_interval)
        #     print("")

        n_rss = w.create_node(stream_name="rss", channel=D, plate_ids=["H1.L.W"])
        w.create_factor(tool=tools.wearable_rss_values, sources=[n_rss_aid_uid],
                        sink=n_rss, alignment_node=None).execute(time_interval)

        # for loc in w.plate_manager["H1.L"].values:
        for loc in ["kitchen", "hallway", "lounge"]:
            print(loc)
            n_rss.print_head((("house", "1"), ("location", loc), ('wearable', 'A')), time_interval)
            print("")

    exit(0)

    # PIR sensor plate

    # Stream to get motion sensor data
    n_pir = w.create_node(stream_name="environmental_db", channel=D, plate_ids=["H1"])
    f_pir = w.create_factor(tool=tools.environmental_motion, sources=None, sink=n_pir, alignment_node=None)

    # Execute the workflow
    w.execute(time_interval)

    print(n_pir.streams[('house', '1'), ].window(time_interval).values()[0:5])
    print(n_rss_aid.streams[('house', '1'), ].window(time_interval).values()[0:5])
