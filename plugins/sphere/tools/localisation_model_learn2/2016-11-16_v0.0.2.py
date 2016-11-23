# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.


from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count
from hyperstream.utils import utcnow, reconstruct_interval

import numpy as np

from copy import deepcopy

from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.svm import LinearSVC
from sklearn.multiclass import OneVsRestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_extraction import DictVectorizer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import PredefinedSplit
from sklearn import metrics

from plugins.sphere.utils import FillZeros, serialise_dict, serialise_pipeline


def predefined_train_test_split(data, labels, folds, workflow, label_encoder):
    folds = np.asarray(folds)
    
    fold_encoder = LabelEncoder()
    split_encoded = fold_encoder.fit_transform(folds)
    
    num_classes = len(label_encoder.classes_)
    
    performance = {
        'classes': label_encoder.classes_.tolist(),
        'intervals': {key: np.sum(folds == key) for key in sorted(list(set(folds)))}
    }
    
    split = PredefinedSplit(split_encoded)
    for fold_index, (train_inds, test_inds) in enumerate(split.split()):
        train_x, train_y = [data[ii] for ii in train_inds], [labels[ii] for ii in train_inds]
        test_x, test_y = [data[ii] for ii in test_inds], [labels[ii] for ii in test_inds]
        
        prior_train = [0] * num_classes
        for yy in train_y:
            prior_train[yy] += 1
        
        prior_test = [0] * num_classes
        for yy in test_y:
            prior_test[yy] += 1
        
        clf = deepcopy(workflow)
        clf.fit(train_x, train_y)
        param_dict = {kk: vv.__dict__ for kk, vv in clf.named_steps.iteritems()}
        
        test_pred = clf.predict(test_x)
        
        test_ind = folds[test_inds[0]]
        performance[test_ind] = {
            'accuracy': metrics.accuracy_score(test_y, test_pred),
            'precision_micro': metrics.precision_score(test_y, test_pred, average='micro'),
            'precision_macro': metrics.precision_score(test_y, test_pred, average='micro'),
            'recall_micro': metrics.recall_score(test_y, test_pred, average='micro'),
            'recall_macro': metrics.recall_score(test_y, test_pred, average='macro'),
            'f1_score_micro': metrics.f1_score(test_y, test_pred, average='micro'),
            'f1_score_macro': metrics.f1_score(test_y, test_pred, average='macro'),
            'confusion_matrix': metrics.confusion_matrix(test_y, test_pred).tolist(),
            'prior_train': prior_train,
            'prior_test': prior_test,
            'model': serialise_dict(param_dict)
        }
    
    return serialise_dict(performance)


class LocalisationModelLearn2(Tool):
    """
    Given a dataframe of
    """
    
    def __init__(self, nan_value=-110):
        super(LocalisationModelLearn2, self).__init__(nan_value=nan_value)

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        source = sources[0]
        data = list(source.window(interval, force_calculation=True))
        classifier_name = dict(source.stream_id.meta_data)['localisation_model']

        if classifier_name == "lda":
            classifier = LinearDiscriminantAnalysis()
        elif classifier_name == "svm":
            classifier = OneVsRestClassifier(LinearSVC())
        else:
            raise NotImplementedError("Unknown classifier type {}".format(classifier_name))

        if not data:
            return

        yy_key = 'annotations'
        xx_key = 'rssi'
        ex_key = 'localisation-experiment'
        
        # TODO: change data to go from ['anno']['Location'] to just ['anno'}
        keep_inds = []
        for di, (tt, dd) in enumerate(data):
            exp = dd[ex_key]
            loc = list(dd[yy_key]['Location'])
            
            if len(loc) == 1 and loc[0] != 'MIX' and exp != 'MIX':
                keep_inds.append(di)

        folds = [data[ii].value[ex_key] for ii in keep_inds]
        train_x = [data[ii].value[xx_key] for ii in keep_inds]
        train_y = [list(data[ii].value[yy_key]['Location'])[0] for ii in keep_inds]
        # TODO: update ['anno']['Location'] keys format changed
        
        label_encoder = LabelEncoder()
        train_y_trans = label_encoder.fit_transform(train_y)
        
        param_dict = {
            'vectorisation': DictVectorizer(sparse=False),
            'fill_missing': FillZeros(self.nan_value),
            'classifier': classifier,
        }
        
        clf = Pipeline([(kk, param_dict[kk]) for kk in ('vectorisation', 'fill_missing', 'classifier')])
        clf.fit(train_x, train_y_trans)
        
        clf_serialised = serialise_pipeline(clf)
        clf_serialised['label_encoder'] = serialise_dict(label_encoder.__dict__)
        clf_serialised['performance'] = predefined_train_test_split(train_x, train_y_trans, folds, clf, label_encoder)

        experiment_ids = sources[0].stream_id.name.split('_')[-2:]
        clf_serialised['experiment_ids_str'] = '_'.join(experiment_ids)
        clf_serialised['experiment_interval'] = map(reconstruct_interval, experiment_ids)
        clf_serialised['tool_parameters'] = dict((x, self.__dict__[x]) for x in self.__dict__ if not x.startswith("_"))

        yield StreamInstance(utcnow(), clf_serialised)
