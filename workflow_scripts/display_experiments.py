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

from hyperstream import HyperStream, TimeInterval, StreamId
from hyperstream.utils import construct_experiment_id, all_time
import arrow

from sphere_helpers import PredefinedTools


if __name__ == '__main__':

    hyperstream = HyperStream()

    tools = PredefinedTools(hyperstream)

    # Various channels
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

    nodes = (
        ("anno_raw",    S, ["H1"]),                    # Raw annotation data
        ("experiments_list",    M, ["H1"]),                    # Current annotation data in 2s windows
        ("experiments_dataframe",    M, ["H1"]),                    # Current annotation data in 2s windows
    )

    # Create all of the nodes
    N = dict((stream_name, w.create_node(stream_name, channel, plate_ids)) for stream_name, channel, plate_ids in nodes)

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="sphere",
            parameters=dict(modality="annotations", annotators=[0], elements={"Experiment"}, filters={})
        ),
        sources=None,
        sink=N["anno_raw"]
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="extract_experiments_from_annotations",
            parameters=dict()
        ),
        sources=[N["anno_raw"]],
        sink=N["experiments_list"]
    )

    def func(instance):
        return construct_experiment_id(TimeInterval(instance.value["start"], instance.value["end"]))

    w.create_node_creation_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="meta_instance",
            parameters=dict(func=func)
        ),
        source=N["experiments_list"],
        output_plate=dict(
            plate_id="H1.LocalisationExperiment",
            meta_data_id="localisation-experiment",
            description="Technician localisation walk-around",
            use_provided_values=False
        ),
        plate_manager=hyperstream.plate_manager
    )

    w.create_factor(
        tool=hyperstream.channel_manager.get_tool(
            name="experiments_dataframe_builder",
            parameters=dict()
        ),
        sources=[N["experiments_list"]],
        sink=N["experiments_dataframe"]
    )

    w.execute(all_time())
    #    f1.execute(time_interval)

    print('number of sphere non_empty_streams: {}'.format(len(S.non_empty_streams)))
    print('number of memory non_empty_streams: {}'.format(len(M.non_empty_streams)))

    experiment_data = M[StreamId('experiments_list', dict(house=1))].window(all_time()).values()
    df = M[StreamId('experiments_dataframe', dict(house=1))].window(all_time()).values()[0]
#    arrow.get(x).humanize()
#    df['start'] = df['start'].map('{:%Y-%m-%d %H:%M:%S}'.format)
    df['duration'] = df['end']-df['start']
    df['start'] = map(lambda x:'{:%Y-%m-%d %H:%M:%S}'.format(x),df['start'])
    df['end'] = map(lambda x:'{:%Y-%m-%d %H:%M:%S}'.format(x),df['end'])
    #    df['duration'] = map(lambda x:'{:%Mmin %Ssec}'.format(x),df['duration'])

    def duration2str(x):
        minutes, seconds = divmod(x.total_seconds(), 60)
        return '{} min {} sec'.format(int(minutes),int(seconds))

    df['start_as_text'] = map(lambda x:arrow.get(x).humanize(),df['start'])
    df['duration_as_text'] = map(lambda x:duration2str(x),df['duration'])
    print(df[['id', 'start_as_text', 'duration_as_text', 'start', 'end', 'direction', 'annotator']])

    exit(0)

