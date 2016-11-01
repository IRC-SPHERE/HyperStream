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
import datetime
import pytz

from hyperstream import HyperStream, TimeInterval, TimeIntervals
from hyperstream.stream import StreamId

from sphere_helpers import PredefinedTools, scripted_experiments, second, minute, hour


# Analysis of data from:
# http://10.70.18.138/data_viewer/scripted/001/2/Location/
# Experiment id: 001   Start: 2015-08-06T13:35:36.035000Z   End: 2015-08-06T14:12:22.008000Z
# Offsets: 0:01:06, 0:01:06, 0:01:06   Annotators: 2, 3, 5
# Annotation files: [ S1060001_jdl.eaf | S1060001_ac.eaf | S1060001_ks.eaf ]
# Mongo query: db.annotations.find({tier: 'Location', start: {$gt: ISODate('2015-08-06T13:35:36.035000Z')},
# end: {$lte: ISODate('2015-08-06T14:12:22.008000Z')}})

def unix2datetime(u):
    return datetime.datetime.fromtimestamp(u/1000.0,tz=pytz.UTC)+datetime.timedelta(hours=0)



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

    # Some plate values for display purposes
    h1 = (('house', '1'),)
    wA = (('wearable', 'A'),)

    # hyperstream.plate_manager.create_plate(
    #     plate_id="H1.multiresident_experiments",
    #     description="Multiresident experiments",
    #     meta_data_id="multiresident_experiment",
    #     values=[],
    #     complement=True,
    #     parent_plate="H1"
    # )
    # hyperstream.plate_manager.create_plate(
    #     plate_id="H1.W",
    #     description="Wearables in house 1",
    #     meta_data_id="wearable",
    #     values=[],
    #     complement=True,
    #     parent_plate="H1"
    # )

    #    locs = tuple(("location", loc) for loc in ["kitchen", "hallway", "lounge"])
    #    eids = tuple(("scripted", i + 1) for i in range(0, len(scripted_experiments.intervals)))
    #    locs_eids = tuple(itertools.product(locs, eids))

    # get a dict of experiment_id => annotator_id mappings
    #    experiment_id_to_annotator_ids = dict(
    #        (k, [a['data'] for a in g])
    #        for k, g in itertools.groupby(
    #            (m for m in hyperstream.config.meta_data if 'tag' in m and m['tag'] == 'annotator'),
    #            lambda x: x['identifier'].split('.')[1].split('_')[1]))

    nodes = (
        ("every_2s",    M, ["H1"]),                    # sliding windows one every minute
        ("rss_raw",     S, ["H1"]),                    # Raw RSS data
        ("anno_raw",    M, ["H1"]),                    # Raw annotation data
        ("rss_2s",    M, ["H1"]),                    # Raw annotation data
        # ("vidloc_raw",  M, ["H1"]),                    # Raw video location annotation data
        # ("rss_uid",     M, ["H1.W"]),                  # RSS by wearable id
        # ("vidloc_uid",  M, ["H1.W"]),                  # RSS by wearable id
        # ("vidloc_uid_align",  M, ["H1.W"]),                  # RSS by wearable id
        # ("vidloc_rss",  M, ["H1.W"]),                  # RSS by wearable id
        # ("vidloc_rss_anno",  M, ["H1.W"]),                  # RSS by wearable id
        # ("vidloc_rss_anno_dict",  M, ["H1.W"]),                  # RSS by wearable id
        # ("rss_vec",     M, ["H1.W"]),                  # RSS by wearable id
        # ("rss_counter", M, ["H1.W"]),                  # RSS by wearable id
        # ("rss_aid",     M, ["H1.L"]),                  # RSS by access point id
        # ("rss_aid_uid", M, ["H1.L.W"]),                # RSS by access point id and device id
        # ("rss",         M, ["H1.L.W"]),                # RSS values only (by access point id and device id)
        # ("rss_time",    M, ["H1.L.W", "H1.multiresident_experiments"])  # RSS values per experiment
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    multires_importer = hyperstream.channel_manager.get_tool(
        name="multiresident_experiment_importer",
        parameters=dict())
    exp_meta = multires_importer.experiment_metadata
    exp_times = TimeIntervals()
    for (i,row) in exp_meta.iterrows():
        exp_times = exp_times + TimeIntervals([TimeInterval(unix2datetime(row.first_occurrence-1),unix2datetime(row.last_occurrence))])
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
            parameters=dict(lower=datetime.timedelta(seconds=-2), upper=datetime.timedelta(seconds=0),
                            increment=datetime.timedelta(seconds=2))
        ),
        sources=None,
        sink=N["every_2s"]
    )


    def component_wise_max(init_value={},id_field='aid',value_field='rss'):
        def func(data):
            result = init_value
            for (time, value) in data:
                if result.has_key(value[id_field]):
                    result[value[id_field]] = max(result[value[id_field]],value[value_field])
                else:
                    result[value[id_field]] = value[value_field]
            return result
        return func
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_apply",
            parameters=dict(func=component_wise_max())
        ),
        sources=[N["every_2s"], N["rss_raw"]],
        sink=N["rss_2s"]
    )

    #     w.create_factor(
    #         tool=hyperstream.channel_manager.get_tool(
    #             name="sphere",
    # #            parameters=dict(modality="annotations",annotators=["WebApp_Technician"],elements={"Location","Location_Fine"},filters={"trigger":1})
    #             parameters=dict(modality="annotations",annotators=[0],elements={"Location"},filters={"trigger":1})
    #         ),
    #         sources=None,
    #         sink=N["anno_raw"]
    #     )

    #    w.execute(exp_times.span)
    ti_start = datetime.datetime(year=2016,month=10,day=18,hour=13,minute=40,tzinfo=pytz.UTC)
    time_interval = TimeInterval(ti_start,ti_start+datetime.timedelta(minutes=10))

    w.execute(time_interval)
    #    f1.execute(time_interval)
    #    w.execute(

    print('number of non_empty_streams: {}'.format(len(M.non_empty_streams)))


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
                tool=tools.split_annotator, source=N["annotations_flat"], sink=N["annotations_split"]) \
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
        w.create_multi_output_factor(tool=tools.split_aid, source=N["rss_flat"], sink=N["rss_aid"]) \
            .execute(time_interval)
        N["rss_aid"].print_head(h1, locs, time_interval)

        N["rss_aid_uid"] = w.create_node(stream_name="rss_aid_uid", channel=M, plate_ids=["H1.L.W"])
        w.create_multi_output_factor(tool=tools.split_uid, source=N["rss_aid"], sink=N["rss_aid_uid"]) \
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
