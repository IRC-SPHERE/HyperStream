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

from hyperstream import HyperStream, TimeInterval, TimeIntervals
from hyperstream.utils import unix2datetime
from plugins.sphere.utils.sphere_helpers import PredefinedTools, scripted_experiments, minute

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

    wearable3 = hyperstream.channel_manager.get_tool(
        name="sphere",
        parameters=dict(modality="wearable3"))

    split_aid = hyperstream.channel_manager.get_tool(
        name="splitter_with_unlist",
        parameters=dict(element="gw",subelement="aid",mapping={"aaaa::212:4b00:a49:e387":"kitchen","aaaa::212:4b00:a49:e503":"lounge","aaaa::212:4b00:a49:f283":"hallway"})) # actually no idea about the locations

    multires_importer = hyperstream.channel_manager.get_tool(
        name="multiresident_experiment_importer",
        parameters=dict())
    exp_meta = multires_importer.experiment_metadata
    exp_times = TimeIntervals()
    for (i,row) in exp_meta.iterrows():
        exp_times = exp_times + TimeIntervals([TimeInterval(unix2datetime(row.first_occurrence-1),unix2datetime(row.last_occurrence))])
    print(exp_times)

    split_time_by_exp = hyperstream.channel_manager.get_tool(
        name="splitter_time_aware",
        parameters=dict(time_intervals=exp_times, meta_data_id="multiresident_experiment")
    )

    # Various channel managers
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    T = hyperstream.channel_manager.tools
    D = hyperstream.channel_manager.mongo

    # Create a simple one step workflow for querying
    w = hyperstream.create_workflow(
        workflow_id="multiresident",
        name="Test of multiresident localisation",
        owner="MK",
        description="Get multi-resident annotations and wearable data;"
                    "train a localisation model; test the model;"
                    "visualise and mine the results")

    # Some plate values for display purposes
    h1 = (('house', '1'),)
    wA = (('wearable', 'A'),)

    hyperstream.plate_manager.create_plate(
        plate_id="H1.multiresident_experiments",
        description="Multiresident experiments",
        meta_data_id="multiresident_experiment",
        values=[],
        complement=True,
        parent_plate="H1"
    )
    hyperstream.plate_manager.create_plate(
        plate_id="H1.W",
        description="Wearables in house 1",
        meta_data_id="wearable",
        values=[],
        complement=True,
        parent_plate="H1"
    )

    #    locs = tuple(("location", loc) for loc in ["kitchen", "hallway", "lounge"])
#    eids = tuple(("scripted", i + 1) for i in range(0, len(scripted_experiments.intervals)))
#    locs_eids = tuple(itertools.product(locs, eids))

    nodes = (
        ("every_min",   M, ["H1.W"]),                    # sliding windows one every minute
        ("rss_raw",     M, ["H1"]),                    # Raw RSS data
        ("vidloc_raw",  M, ["H1"]),                    # Raw video location annotation data
        ("rss_uid",     M, ["H1.W"]),                  # RSS by wearable id
        ("vidloc_uid",  M, ["H1.W"]),                  # RSS by wearable id
        ("vidloc_uid_align",  M, ["H1.W"]),                  # RSS by wearable id
        ("vidloc_rss",  M, ["H1.W"]),                  # RSS by wearable id
        ("vidloc_rss_anno",  M, ["H1.W"]),                  # RSS by wearable id
        ("vidloc_rss_anno_dict",  M, ["H1.W"]),                  # RSS by wearable id
        ("rss_vec",     M, ["H1.W"]),                  # RSS by wearable id
        ("rss_counter", M, ["H1.W"]),                  # RSS by wearable id
        ("rss_aid",     M, ["H1.L"]),                  # RSS by access point id
        ("rss_aid_uid", M, ["H1.L.W"]),                # RSS by access point id and device id
        ("rss",         M, ["H1.L.W"]),                # RSS values only (by access point id and device id)
        ("rss_time",    M, ["H1.L.W", "H1.multiresident_experiments"])  # RSS values per experiment
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

#    w.create_factor(tool=tools.wearable_rss, sources=None, sink=N["rss_raw"])
    f = w.create_factor(tool=wearable3, sources=None, sink=N["rss_raw"])
#    w.create_multi_output_factor(tool=tools.split_aid, source=N["rss_raw"], splitting_node=None, sink=N["rss_aid"])

#    w.create_multi_output_factor(tool=split_aid, source=N["rss_raw"], splitting_node=None, sink=N["rss_aid"])
#    w.create_multi_output_factor(tool=tools.split_uid, source=N["rss_aid"], splitting_node=None, sink=N["rss_aid_uid"])

    w.create_multi_output_factor(tool=tools.split_uid, source=N["rss_raw"], splitting_node=None, sink=N["rss_uid"])

 #    N["rss_vec"] = w.create_node("rss_vec",M,"H1.W")
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="rssi_to_vector",
            parameters=dict(aids=["aaaa::212:4b00:a49:f283","aaaa::212:4b00:a49:e503","aaaa::212:4b00:a49:e387"],default_value=-120)
        ),
        sources=[N["rss_uid"]],
        sink=N["rss_vec"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            parameters=dict(lower=datetime.timedelta(minutes=-1), upper=datetime.timedelta(minutes=0),
                 increment=datetime.timedelta(minutes=1))
        ),
        sources=None,
        sink=N["every_min"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_apply",
            parameters=dict(func=lambda x:len(list(x)))
        ),
        sources=[N["every_min"],N["rss_uid"]],
        sink=N["rss_counter"]
    )
    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter",
            parameters=dict(element="wearable_id",mapping=dict(A="A",B="B",C="C",D="D"))
        ),
        splitting_node=None,
        source=N["vidloc_raw"],
        sink=N["vidloc_uid"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="multiresident_experiment_importer",
            parameters=dict()
        ),
        sources=None,
        sink=N["vidloc_raw"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="aligning_window",
            parameters=dict(lower=datetime.timedelta(seconds=-2),upper=datetime.timedelta(0))
        ),
        sources=[N["vidloc_uid"]],
        sink=N["vidloc_uid_align"]
    )
    def component_wise_max(data):
        res = [-120,-120,-120]
        for (time,value) in data:
            for i in range(3):
                res[i] = max(res[i],value[i])
        return res
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_apply",
            parameters=dict(func=component_wise_max) # lambda data:list(data)[-1][1])
        ),
        sources=[N["vidloc_uid_align"],N["rss_vec"]],
        sink=N["vidloc_rss"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="aligned_merge",
            parameters=dict(names=["anno","rssi"])
        ),
        sources=[N["vidloc_uid"],N["vidloc_rss"]],
        sink=N["vidloc_rss_anno"]
    )
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="multiresident_reformat",
            parameters=dict()
        ),
        sources=[N["vidloc_rss_anno"]],
        sink=N["vidloc_rss_anno_dict"]
    )

#    w.create_factor(tool=tools.wearable_rss_values, sources=[N["rss_aid_uid"]], sink=N["rss"])

    # Now we want to split by time interval onto a time-oriented plate
    # N["rss_time"] = w.create_node(stream_name="rss_time", channel=M, plate_ids=["H.L", "H.scripted"])
#    w.create_multi_output_factor(tool=split_time_by_exp, source=N["rss"], splitting_node=None, sink=N["rss_time"])

    # time_interval = scripted_experiments.span
    # time_interval = TimeInterval(scripted_experiments.intervals[0].start,
    #                              scripted_experiments.intervals[0].start + second)
#    time_interval = scripted_experiments.intervals[0] + (-1, 0)

#    w.execute(exp_times.span)
    f.execute(TimeInterval(exp_times.start,exp_times.start+datetime.timedelta(minutes=5)))
#    w.execute(TimeInterval(exp_times.start,exp_times.start+datetime.timedelta(hours=48)))

###    file = open("vidloc_rss_anno.csv","w")
###    file.write("dt,wearable,person,exper,camera,rssi1,rssi2,rssi3\n")
###    for wearable in ["A","B","C","D"]:
###        stream = M.data[StreamId(name="vidloc_rss_anno",meta_data=(("house","1"),("wearable",wearable)))]
###        for t in sorted(stream):
###            anno = stream[t]["anno"]
###            rssi = stream[t]["rssi"]
###            row = [str(t),wearable,str(anno["person_id"]),str(anno["exper_id"]),str(anno["camera_id"])] + [str(x) for x in rssi]
###            file.write(",".join(row)+"\n")
###    file.close()


#    w.execute(time_interval)
    exit(0)

    def print_head(node_id, parent_plate_values, plate_values, interval, n=10, print_func=print):
        print_func("Node: {}".format(node_id))
        N[node_id].print_head(parent_plate_values, plate_values, interval, n, print_func)

    print_head("rss_raw",       None,       h1,         time_interval, 10, print)
    print_head("rss_aid",       h1,         locs,       time_interval, 10, print)
    print_head("rss_aid_uid",   wA,         locs,       time_interval, 10, print)
    print_head("rss",           h1 + wA,    locs,       time_interval, 10, print)
    print_head("rss_time",      h1 + wA,    locs_eids,  time_interval, 10, print)

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
            parent_plate="H1.scripted"
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
                tool=tools.split_annotator, source=N["annotations_flat"], splitting_node=None,
                sink=N["annotations_split"]).execute(time_interval)
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
        w.create_multi_output_factor(tool=tools.split_aid, source=N["rss_flat"],
                                     splitting_node=None, sink=N["rss_aid"]).execute(time_interval)
        N["rss_aid"].print_head(h1, locs, time_interval)

        N["rss_aid_uid"] = w.create_node(stream_name="rss_aid_uid", channel=M, plate_ids=["H1.L.W"])
        w.create_multi_output_factor(tool=tools.split_uid, source=N["rss_aid"],
                                     splitting_node=None, sink=N["rss_aid_uid"]).execute(time_interval)
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
