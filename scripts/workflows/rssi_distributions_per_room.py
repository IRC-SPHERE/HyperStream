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

from hyperstream.utils import utcnow
from hyperstream import StreamId, StreamInstance


def create_workflow_rssi_distributions_per_room(hyperstream, house, experiment_indices, experiment_ids, safe=True):
    experiment_ids_str = '_'.join(experiment_ids)
    # Create a simple one step workflow for querying
    workflow_id = "rssi_distributions_per_room_" + experiment_ids_str

    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="RSSI distributions per room",
            owner="MK",
            description="RSSI distributions per room, based on technician walkarounds",
            online=False)
    except KeyError as e:
        if safe:
            raise e
        else:
            return hyperstream.workflow_manager.workflows[workflow_id]

    # Various channels
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    T = hyperstream.channel_manager.tools
    D = hyperstream.channel_manager.mongo
    A = hyperstream.channel_manager.assets

    nodes = (
        ("experiments_list",            M, ["H"]),  # Current annotation data in 2s windows
        ("experiments_mapping",         M, ["H"]),  # Current annotation data in 2s windows
        ("rss_raw",                     S, ["H"]),  # Raw RSS data
        ("rss_time",                    S, ["H.SelectedLocalisationExperiment"]),  # RSS data split by experiment
        ("annotation_raw_locations",    S, ["H"]),  # Raw annotation data
        ("annotation_time",             S, ["H.SelectedLocalisationExperiment"]),  # RSS data split by experiment
        ("every_2s",                    M, ["H.SelectedLocalisationExperiment"]),  # sliding windows one every minute
        ("annotation_state_location",   M, ["H.SelectedLocalisationExperiment"]),  # Annotation data in 2s windows
        ("annotation_state_2s_windows", M, ["H.SelectedLocalisationExperiment"]),
        ("rss_2s",                      M, ["H.SelectedLocalisationExperiment"]),  # max(RSS) per AP in past 2s of RSS
        ("merged_2s",                   M, ["H.SelectedLocalisationExperiment"]),  # rss_2s with annotation_state_2s
        ("merged_2s_flat_"+experiment_ids_str,              M, ["H"]),                                 # flattened version of merged_2s
        ("dataframe_"+experiment_ids_str,                   M, ["H"]),
        ("csv_string_"+experiment_ids_str,                   M, ["H"]),
        ("pdf_"+experiment_ids_str,                   M, ["H"]),
        ("experiments_selected",        A, ["H"])
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    # TODO: Perhaps we want to do this same
    A.write_to_stream(
        stream_id=StreamId(name="experiments_selected", meta_data=dict(house=house)),
        data=StreamInstance(timestamp=utcnow(), value=list(experiment_ids))
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="experiments_mapping_builder",
            parameters={}
        ),
        sources=[N["experiments_list"], N["experiments_selected"]],
        sink=N["experiments_mapping"]
    )

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="wearable3")
        ),
        source=None,
        splitting_node=None,
        sink=N["rss_raw"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_time_aware_from_stream",
            parameters=dict(meta_data_id="localisation-experiment")
        ),
        source=N["rss_raw"],
        splitting_node=N["experiments_mapping"],
        sink=N["rss_time"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="annotations",
                            annotators=[0],
                            elements={"Location"},
                            filters={})
        ),
        source=None,
        splitting_node=None,
        sink=N["annotation_raw_locations"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_time_aware_from_stream",
            parameters=dict(meta_data_id="localisation-experiment")
        ),
        source=N["annotation_raw_locations"],
        splitting_node=N["experiments_mapping"],
        sink=N["annotation_time"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sliding_window",
            parameters=dict(lower=-2.0, upper=0.0, increment=2.0)
        ),
        sources=None,
        sink=N["every_2s"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="annotation_state_location",
            parameters=dict()
        ),
        sources=[N["every_2s"], N["annotation_time"]],
        sink=N["annotation_state_location"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="aligning_window",
            parameters=dict(lower=-2.0, upper=0.0)
        ),
        sources=[N["annotation_state_location"]],
        sink=N["annotation_state_2s_windows"])

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
        sources=[N["annotation_state_2s_windows"], N["rss_time"]],
        sink=N["rss_2s"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="aligned_merge",
            parameters=dict(names=["annotations", "rssi"])
        ),
        sources=[N["annotation_state_location"], N["rss_2s"]],
        sink=N["merged_2s"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="aggregate_plate",
            parameters=dict(aggregation_meta_data="localisation-experiment")
        ),
        sources=[N["merged_2s"]],
        sink=N["merged_2s_flat_" + experiment_ids_str])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="dallan_data_frame_builder",
            parameters=dict()
        ),
        sources=[N["merged_2s_flat_" + experiment_ids_str]],
        sink=N["dataframe_" + experiment_ids_str])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="data_frame_to_csv_string",
            parameters=dict()
        ),
        sources=[N["dataframe_" + experiment_ids_str]],
        sink=N["csv_string_" + experiment_ids_str])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="r_rssi_comparison_plot",
            parameters=dict(
                output_path=hyperstream.config.output_path,
                filename_suffix="_rssi_comparison_plot_{}.pdf".format('_'.join(map(str, experiment_indices))))
        ),
        sources=[N["csv_string_" + experiment_ids_str]],
        sink=N["pdf_" + experiment_ids_str])
    
#   w.create_factor(
#        tool=hyperstream.channel_manager.get_tool(
#             name="localisation_model_learn",
#             parameters=dict(nan_value=-110.0)
#         ),
#         sources=[N["merged_2s_flat_"+experiment_ids_str]],
#         sink=N["location_prediction_lda_"+experiment_ids_str])
#
    return w
