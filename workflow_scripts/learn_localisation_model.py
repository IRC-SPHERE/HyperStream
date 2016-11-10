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
import pytz

from hyperstream import HyperStream, TimeInterval, TimeIntervals
from hyperstream.stream import StreamId
from hyperstream.utils import unix2datetime, all_time

from sphere_helpers import PredefinedTools, scripted_experiments, second, minute, hour


if __name__ == '__main__':
    hyperstream = HyperStream()
    tools = PredefinedTools(hyperstream)
    
    # Create a simple one step workflow for querying
    w = hyperstream.create_workflow(
        workflow_id="localisation_prediction",
        name="localisation from technician's scripted walk through the house, annotation locations",
        owner="NT",
        description="Get the annotations and RSSIs;"
                    "train a localisation model; test the model;"
                    "visualise and mine the results")
    
    # Some plate values for display purposes
    h1 = (('house', '1'),)
    wA = (('wearable', 'A'),)

    # Various channels
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    T = hyperstream.channel_manager.tools
    D = hyperstream.channel_manager.mongo

    nodes = (
        ("anno_raw_experiments", S, ["H1"]),  # Raw annotation data
        ("experiments_list", M, ["H1"]),  # Current annotation data in 2s windows
        ("experiments_dataframe", M, ["H1"]),  # Current annotation data in 2s windows
        ("experiments_mapping", M, ["H1"]),  # Current annotation data in 2s windows
        ("every_2s", hyperstream.channel_manager.memory, ["H1"]),  # sliding windows one every minute
        ("rss_raw", hyperstream.channel_manager.sphere, ["H1"]),  # Raw RSS data
        ("rss_time", hyperstream.channel_manager.sphere, ["H1.LocalisationExperiment"]),  # RSS data split by experiment
        ("anno_raw", hyperstream.channel_manager.sphere, ["H1"]),  # Raw annotation data
        ("rss_2s", hyperstream.channel_manager.memory, ["H1"]),  # max RSS per access point in past 2s of RSS data
        ("anno_state", hyperstream.channel_manager.memory, ["H1"]),  # Current annotation data in 2s windows
        ("anno_state_2s_windows", hyperstream.channel_manager.memory, ["H1"]),
        ("merged_2s", hyperstream.channel_manager.memory, ["H1"]),
        ("dataframe", hyperstream.channel_manager.memory, ["H1"]),
        ("location_prediction_lda_mk1", hyperstream.channel_manager.mongo, ["H1"]),
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
            parameters=dict(exp_ids={17,21})
        ),
        sources=[N["experiments_list"]],
        sink=N["experiments_mapping"]
    )

    # Set times for execution
    start_time = datetime.datetime(year=2016, month=10, day=18, hour=1, tzinfo=pytz.UTC)
    
    ti_start = datetime.datetime(year=2016, month=10, day=19, hour=12, minute=28, tzinfo=pytz.UTC)
    duration = datetime.timedelta(minutes=72)
    duration = datetime.timedelta(days=2)

    end_time = ti_start + duration
    time_interval = TimeInterval(ti_start, end_time)

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
            parameters = dict(meta_data_id="localisation-experiment")
        ),
        source=N["rss_raw"],
        splitting_node=N["experiments_mapping"],
        sink=N["rss_time"])


    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="sliding_window",
    #         parameters=dict(lower=datetime.timedelta(seconds=-2),
    #                         upper=datetime.timedelta(seconds=0),
    #                         increment=datetime.timedelta(seconds=2))
    #     ),
    #     sources=None,
    #     sink=N["every_2s"])
    #
    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="sphere",
    #         parameters=dict(modality="annotations",
    #                         annotators=[0],
    #                         elements={"Location"},
    #                         filters={})
    #     ),
    #     sources=None,
    #     sink=N["anno_raw"])
    #
    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="anno_state",
    #         parameters=dict(start_time=start_time)
    #     ),
    #     sources=[N["every_2s"], N["anno_raw"]],
    #     sink=N["anno_state"])
    #
    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="aligning_window",
    #         parameters=dict(lower=datetime.timedelta(seconds=-2),
    #                         upper=datetime.timedelta(0))
    #     ),
    #     sources=[N["anno_state"]],
    #     sink=N["anno_state_2s_windows"])
    #
    #
    # def component_wise_max(init_value=None, id_field='aid', value_field='rss'):
    #     if init_value is None:
    #         init_value = {}
    #
    #     def func(data):
    #         result = init_value.copy()
    #         for (time, value) in data:
    #             if result.has_key(value[id_field]):
    #                 result[value[id_field]] = max(result[value[id_field]], value[value_field])
    #             else:
    #                 result[value[id_field]] = value[value_field]
    #         return result
    #
    #     return func
    #
    #
    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="sliding_apply",
    #         parameters=dict(func=component_wise_max())
    #     ),
    #     sources=[N["anno_state_2s_windows"], N["rss_raw"]],
    #     sink=N["rss_2s"])
    #
    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="aligned_merge",
    #         parameters=dict(names=["anno", "rssi"])
    #     ),
    #     sources=[N["anno_state"], N["rss_2s"]],
    #     sink=N["merged_2s"])
    #
    # # from NT: removed the necessity for this by adding a DictVectoriser to the classifier pipeline - makes
    # #   classifier more robust to differing sensor contexts
    # # w.create_factor(
    # #     tool=hyperstream.channel_manager.get_tool(
    # #         name="dallan_dataframe_builder",
    # #         parameters=dict(time_interval=TimeInterval(start_time, end_time))
    # #     ),
    # #     sources=[N["merged_2s"]],
    # #     sink=N["dataframe"])
    #
    # w.create_factor(
    #     tool=hyperstream.channel_manager.get_tool(
    #         name="localisation_model_learn",
    #         parameters=dict(nan_value=-110,
    #         folds={17, 21})
    #     ),
    #     sources=[N["merged_2s"]],
    #     sink=N["location_prediction_lda_mk1"])
    #
    w.execute(all_time())
    
    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))
    
    exit()
    
    sid = StreamId(name='dataframe', meta_data=(('house', '1'),))
    df = hyperstream.channel_manager.memory.data[sid][end_time]
    
    stream = hyperstream.channel_manager.memory.data[StreamId(name="anno_state", meta_data=(("house", "1"),))]
    for t in sorted(stream):
        print('{} : {}'.format(t, stream[t]))
    
    from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
    from sklearn.model_selection import PredefinedSplit
    from sklearn.preprocessing import LabelEncoder, LabelBinarizer, Imputer
    import numpy as np
    
    feature_columns = ['b827eb128626', 'b827eb1eecd0', 'b827eb1f4617', 'b827eb30ee27',
                       'b827eb541227', 'b827eb5b2fdd', 'b827eb5ca171', 'b827eb5ceb88',
                       'b827eb65c7d5', 'b827eb8ec558', 'b827eba3d9a7', 'b827ebaef0a3',
                       'b827ebbb08be', 'b827ebc375f1', 'b827ebceb75e', 'b827ebd107f0',
                       'b827ebd91657', 'b827ebdfd545']
    label_column = 'location'
    fold_column = 'fold'
    
    
    def print_cm(cm, labels, hide_zeroes=False, hide_diagonal=False, hide_threshold=None):
        """pretty print for confusion matrixes"""
        columnwidth = max([len(x) for x in labels] + [5])  # 5 is value length
        empty_cell = " " * columnwidth
        # Print header
        print "    " + empty_cell,
        for label in labels:
            print "%{0}s".format(columnwidth) % label,
        print
        # Print rows
        for i, label1 in enumerate(labels):
            print "    %{0}s".format(columnwidth) % label1,
            for j in range(len(labels)):
                cell = "%{0}.1f".format(columnwidth) % cm[i, j]
                if hide_zeroes:
                    cell = cell if float(cm[i, j]) != 0 else empty_cell
                if hide_diagonal:
                    cell = cell if i != j else empty_cell
                if hide_threshold:
                    cell = cell if cm[i, j] > hide_threshold else empty_cell
                print cell,
            print
    
    
    from sklearn.preprocessing import MinMaxScaler
    from sklearn.pipeline import Pipeline
    
    from sklearn import metrics
    from sklearn.metrics import confusion_matrix
    
    keep_inds = np.c_[df.fold != 'MIX', df.location != 'MIX', np.c_[df.fold == 17, df.fold == 21].any(axis=1)].all(
        axis=1)
    df = df[keep_inds]
    fold_encoder = LabelEncoder()
    fold_inds = fold_encoder.fit_transform(df[fold_column])
    folds = PredefinedSplit(fold_inds)
    for train_inds, test_inds in folds.split():
        train_x = np.asarray(df[feature_columns].iloc[train_inds])
        test_x = np.asarray(df[feature_columns].iloc[test_inds])
        train_y = np.asarray(df[label_column].iloc[train_inds])
        test_y = np.asarray(df[label_column].iloc[test_inds])
        
        train_x[np.isnan(train_x)] = -110
        test_x[np.isnan(test_x)] = -110
        
        # Convert labels to required form
        lb = LabelEncoder()
        lb.fit(train_y)
        train_y = lb.transform(train_y)
        test_y = lb.transform(test_y)
        
        # Learn the model
        clf = Pipeline([
            # ('scale', MinMaxScaler()),
            ('clf', LinearDiscriminantAnalysis())
        ])
        clf.fit(train_x, train_y)
        print metrics.accuracy_score(train_y, clf.predict(train_x))
        print metrics.accuracy_score(test_y, clf.predict(test_x))
        print
