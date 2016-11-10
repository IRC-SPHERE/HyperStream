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


def create_localisation_prediction(hyperstream, safe=True):

    # Create a simple one step workflow for querying
    workflow_id = "localisation_prediction"
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
        ("anno_raw_experiments",        S, ["H1"]),  # Raw annotation data
        ("experiments_list",            M, ["H1"]),  # Current annotation data in 2s windows
        ("experiments_dataframe",       M, ["H1"]),  # Current annotation data in 2s windows
        ("experiments_mapping",         M, ["H1"]),  # Current annotation data in 2s windows
        ("every_2s",                    M, ["H1"]),  # sliding windows one every minute
        ("rss_raw",                     S, ["H1"]),  # Raw RSS data
        ("rss_time",                    S, ["H1.LocalisationExperiment"]),  # RSS data split by experiment
        ("anno_raw_locations",          S, ["H1"]),  # Raw annotation data
        ("anno_time",                   S, ["H1.LocalisationExperiment"]),  # RSS data split by experiment
        ("rss_2s",                      M, ["H1"]),  # max RSS per access point in past 2s of RSS data
        ("anno_state",                  M, ["H1"]),  # Current annotation data in 2s windows
        ("anno_state_2s_windows",       M, ["H1"]),
        ("merged_2s",                   M, ["H1"]),
        ("dataframe",                   M, ["H1"]),
        ("location_prediction_lda_mk1", D, ["H1"])
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="annotations", annotators=[0], elements={"Experiment"}, filters={})
        ),
        sources=None,
        sink=N["anno_raw_experiments"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="extract_experiments_from_annotations",
            parameters=dict()
        ),
        sources=[N["anno_raw_experiments"]],
        sink=N["experiments_list"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="experiments_dataframe_builder",
            parameters=dict()
        ),
        sources=[N["experiments_list"]],
        sink=N["experiments_dataframe"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="experiments_mapping_builder",
            parameters=dict(exp_ids={17, 21})
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
        sink=N["anno_raw_locations"])

    w.create_multi_output_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="splitter_time_aware_from_stream",
            parameters=dict(meta_data_id="localisation-experiment")
        ),
        source=N["anno_raw_locations"],
        splitting_node=N["experiments_mapping"],
        sink=N["anno_time"])

    return w
