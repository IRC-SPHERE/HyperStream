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
import itertools
import logging

from hyperstream import HyperStream, TimeInterval

from sphere_helpers import PredefinedTools, scripted_experiments, second, minute, hour


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
        workflow_id="localisation",
        name="Test of localisation",
        owner="TD",
        description="Would like to test localisation using PIR and RSS, so we need to first get the appropriate data"
                    "out of the SPHERE stream, with the appropriate meta-data attached, and then use a model")

    # The nodes
    N = w.nodes

    # Some plate values for display purposes
    h1 = (('house', '1'),)
    wA = (('wearable', 'A'),)
    s1 = (('scripted', '1'),)
    s2 = (('scripted', '2'),)
    locs = tuple(("location", loc) for loc in ["kitchen", "hallway", "lounge"])
    eids = tuple(("scripted", str(i + 1)) for i in range(0, len(scripted_experiments.intervals)))
    locs_eids = tuple(itertools.product(locs, eids))

    hyperstream.plate_manager.create_plate(
        plate_id="H1.S_1,2",
        description="Scripted experiments 1 and 2 in SPHERE house",
        meta_data_id="scripted",
        values=["1", "2"],
        complement=False,
        parent_plate="H1"
    )

    anns = []

    # Create necessary plates
    for i, time_interval in enumerate(scripted_experiments[:2]):
        # time_interval.end = time_interval.start + minute

        experiment_id = str(i + 1)
        annotator_ids = tools.experiment_id_to_annotator_ids[experiment_id]
        anns.append(tuple(("annotator", ann) for ann in annotator_ids))

        hyperstream.plate_manager.create_plate(
            plate_id="H1.S{}".format(experiment_id),
            description="Scripted experiment {} in SPHERE house".format(experiment_id),
            meta_data_id="scripted",
            values=[experiment_id],
            complement=False,
            parent_plate="H1"
        )

        hyperstream.plate_manager.create_plate(
            plate_id="H1.S{}.A".format(experiment_id),
            description="All annotators for scripted experiment {} in SPHERE house".format(experiment_id),
            meta_data_id="annotator",
            values=[],
            complement=True,
            parent_plate="H1.S{}".format(experiment_id)
        )

    nodes = (
        ("rss_raw",     S, ["H1"]),                 # Raw RSS data
        ("rss_time",    M, ["H1.S_1,2"]),           # RSS values per scripted experiment (just experiments 1, 2)
        ("rss_train",   M, ["H1.S1"]),              # RSS values scripted experiment 1
        ("rss_test",    M, ["H1.S2"]),              # RSS values scripted experiment 2
        ("ann_raw",     S, ["H1"]),                 # Annotations (raw format)
        ("ann",         M, ["H1"]),                 # Annotations (clean format)
        ("ann_time",    M, ["H1.S_1,2"]),           # Annotations per scripted experiment (just experiments 1, 2)
        ("ann_time1",   M, ["H1.S_1,2"]),           # Annotations per scripted experiment (just experiments 1, 2)
        ("ann_train",   M, ["H1.S1"]),              # Annotations for scripted experiment 1
        ("ann_test",    M, ["H1.S2"]),              # Annotations for scripted experiment 1
        ("ann_train_test", M, ["H1"]),              # Annotations for
        ("model",       M, ["H1.S1"]),              # Outputs of model trained on scripted experiment 1
    )

    # Create all of the nodes
    for stream_name, channel, plate_ids in nodes:
        w.create_node(stream_name, channel, plate_ids)

    w.create_factor(
        tool=tools.annotations_location,
        sources=None,
        sink=N["ann_raw"])

    w.create_factor(
        tool=tools.annotations_components,
        sources=[N["ann_raw"]],
        sink=N["ann"]
    )

    w.create_multi_output_factor(
        tool=tools.split_time,
        source=N["ann"],
        splitting_node=None,
        sink=N["ann_time"])

    w.create_factor(
        tool=tools.index_of_1,
        sources=[N["ann_time"]],
        sink=N["ann_train"])

    w.create_factor(
        tool=tools.index_of_2,
        sources=[N["ann_time"]],
        sink=N["ann_test"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="aggregate",
            parameters=dict(
                func=
            )
        )
    )

    w.create_factor(
        tool=tools.wearable_rss,
        sources=None,
        sink=N["rss_raw"])

    # Now we want to split by time interval onto a time-oriented plate
    w.create_multi_output_factor(
        tool=tools.split_time,
        source=N["rss_raw"],
        splitting_node=None,
        sink=N["rss_time"])

    # Now separate this onto single plates for train and test
    w.create_factor(
        tool=tools.index_of_1,
        sources=[N["rss_time"]],
        sink=N["rss_train"])

    w.create_factor(
        tool=tools.index_of_2,
        sources=[N["rss_time"]],
        sink=N["rss_test"])

    w.create_factor()

    if True:
        w.create_factor(
            tool=hyperstream.channel_manager.get_tool("location_predictor", parameters=dict()),
            sources=[N["rss_train"], N["ann_train"]],
            sink=N["model"]
        )

    # time_interval = TimeInterval(scripted_experiments.intervals[0].start,
    #                              scripted_experiments.intervals[0].start + second)
    # time_interval = scripted_experiments[0] + (-1, 0)
    time_interval = scripted_experiments.span + (-1, 0)

    w.execute(time_interval)

    def print_head(node_id, parent_plate_values, plate_values, interval, n=10, print_func=print):
        print_func("Node: {}".format(node_id))
        N[node_id].print_head(parent_plate_values, plate_values, interval, n, print_func)

    print_head("ann_train",     h1, s1,    time_interval, 10, print)
    print_head("ann_test",      h1, s2,    time_interval, 10, print)
    print_head("rss_train",     h1, s1,    time_interval, 10, print)
    print_head("rss_test",      h1, s2,    time_interval, 10, print)

    # TODO: Example of training on one set of data and testing on another
    # TODO: Example of training on several sets of data and testing on other sets

    exit(0)

    # TODO just two for now
    # for i, (time_interval, annotator_ids) in enumerate(scripted_experiments[:2]):
    for i, time_interval in enumerate(scripted_experiments[:2]):
        time_interval.end = time_interval.start + minute

        experiment_id = str(i + 1)
        annotator_ids = experiment_id_to_annotator_ids[experiment_id]
        anns = [("annotator", ann) for ann in annotator_ids]

        hyperstream.plate_manager.create_plate(
            plate_id="H1.scripted_{}".format(experiment_id),
            description="Annotators for scripted experiment {} in SPHERE house".format(experiment_id),
            meta_data_id="annotator",
            values=[],
            complement=True,
            parent_plate="H1"
        )

        if False:
            # Load in annotations from the same time period
            N["annotations_flat"] = w.create_node(stream_name="annotations_flat", channel=M, plate_ids=["H1.scripted"])

            annotations_location = hyperstream.channel_manager.get_tool(
                name="sphere",
                parameters=dict(modality="annotations", annotators=annotator_ids, elements={"Location"},
                                filters={"trigger": 1})
            )

            w.create_factor(
                tool=annotations_location, sources=None, sink=N["annotations_flat"]).execute(time_interval) \
                .sink.print_head(None, (("house", "1"), ('scripted', str(i + 1))), time_interval)

            plate_id = "H1.scripted_{}".format(i + 1)
            # Put these on to an annotators plate
            N["annotations_split"] = w.create_node(stream_name="annotations_split", channel=M, plate_ids=[plate_id])
            w.create_multi_output_factor(
                tool=tools.split_annotator, source=N["annotations_flat"], sink=N["annotations_split"])\
                .execute(time_interval)
            N["annotations_split"].print_head(h1, anns, time_interval)

            # Pull out the label
            N["annotations"] = w.create_node(stream_name="annotations", channel=M, plate_ids=[plate_id])
            w.create_factor(tool=tools.annotations_label, sources=[N["annotations_split"]],
                            sink=N["annotations"]).execute(time_interval)
            N["annotations"].print_head(h1, anns, time_interval)

        # Here we used the splitter tool over the RSS data to generate the plate
        N["rss_flat"] = w.create_node(stream_name="rss", channel=S, plate_ids=["H1"])
        w.create_factor(tool=tools.wearable_rss, sources=None, sink=N["rss_flat"]).execute(time_interval)
        N["rss_flat"].print_head(None, h1, time_interval, 10, print)

        N["rss_aid"] = w.create_node(stream_name="rss_aid", channel=M, plate_ids=["H1.L"])
        w.create_multi_output_factor(tool=tools.split_aid, source=N["rss_flat"], sink=N["rss_aid"])\
            .execute(time_interval)
        N["rss_aid"].print_head(h1, locs, time_interval)

        N["rss_aid_uid"] = w.create_node(stream_name="rss_aid_uid", channel=M, plate_ids=["H1.L.W"])
        w.create_multi_output_factor(tool=tools.split_uid, source=N["rss_aid"], sink=N["rss_aid_uid"])\
            .execute(time_interval)
        N["rss_aid_uid"].print_head(h1 + wA, locs, time_interval)

        N["rss"] = w.create_node(stream_name="rss", channel=D, plate_ids=["H1.L.W"])
        w.create_factor(tool=tools.wearable_rss_values, sources=[N["rss_aid_uid"]],
                        sink=N["rss"], alignment_node=None).execute(time_interval)
        N["rss"].print_head(h1 + wA, locs, time_interval)

    exit(0)

    # PIR sensor plate

    # Stream to get motion sensor data
    N["pir"] = w.create_node(stream_name="environmental_db", channel=D, plate_ids=["H1"])
    f_pir = w.create_factor(tool=tools.environmental_motion, sources=None, sink=N["pir"], alignment_node=None)

    # Execute the workflow
    w.execute(time_interval)

    print(N["pir"].streams[('house', '1'), ].window(time_interval).values()[0:5])
    print(N["rss_aid"].streams[('house', '1'), ].window(time_interval).values()[0:5])
