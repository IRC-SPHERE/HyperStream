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


def create_workflow_lda_localisation_model_learner(hyperstream, experiment_ids, safe=True):

    # Create a simple one step workflow for querying
    workflow_id = "lda_localisation_model_learner"
    try:
        w = hyperstream.create_workflow(
            workflow_id=workflow_id,
            name="localisation from technician's scripted walk through the house, annotation locations",
            owner="NT",
            description="Get the annotations and RSSIs;"
                        "train a localisation model; test the model;"
                        "visualise and mine the results")
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

    nodes = (
        ("experiments_list",            M, ["H1"]),  # Current annotation data in 2s windows
        ("experiments_dataframe",       M, ["H1"]),  # Current annotation data in 2s windows
        ("experiments_mapping",         M, ["H1"]),  # Current annotation data in 2s windows
        ("rss_raw",                     S, ["H1"]),  # Raw RSS data
        ("rss_time",                    S, ["H1.SelectedLocalisationExperiment"]),  # RSS data split by experiment
        ("annotation_raw_locations",    S, ["H1"]),  # Raw annotation data
        ("annotation_time",             S, ["H1.SelectedLocalisationExperiment"]),  # RSS data split by experiment
        ("every_2s",                    M, ["H1.SelectedLocalisationExperiment"]),  # sliding windows one every minute
        ("annotation_state_location",   M, ["H1.SelectedLocalisationExperiment"]),  # Annotation data in 2s windows
        ("annotation_state_2s_windows", M, ["H1.SelectedLocalisationExperiment"]),
        ("rss_2s",                      M, ["H1.SelectedLocalisationExperiment"]),  # max(RSS) per AP in past 2s of RSS
        ("merged_2s",                   M, ["H1.SelectedLocalisationExperiment"]),  # rss_2s with annotation_state_2s
        ("merged_2s_flat",              M, ["H1"]),                                 # flattened version of merged_2s
        ("dataframe",                   M, ["H1"]),
        ("location_prediction_lda_mk1", D, ["H1"])
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="experiments_mapping_builder",
            parameters=dict(experiment_ids=experiment_ids)
        ),
        sources=[N["experiments_list"]],
        sink=N["experiments_mapping"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="wearable4")
        ),
        sources=None,
        sink=N["rss_raw"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_time_aware_from_stream",
            parameters=dict(meta_data_id="localisation-experiment")
        ),
        source=N["rss_raw"],
        splitting_node=N["experiments_mapping"],
        sink=N["rss_time"])

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="annotations",
                            annotators=[0],
                            elements={"Location"},
                            filters={})
        ),
        sources=None,
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
        sink=N["merged_2s_flat"])
    
    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="localisation_model_learn",
            parameters=dict(nan_value=-110.0, folds=None)
        ),
        sources=[N["merged_2s_flat"]],
        sink=N["location_prediction_lda_mk1"])

    return w
