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

import numpy as np

from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.calibration import CalibratedClassifierCV
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_extraction import DictVectorizer
from sklearn.pipeline import Pipeline

from plugins.sphere.utils import FillZeros


class LocalisationModelLearn(Tool):
    """
    Given a dataframe of
    """
    
    def __init__(self, folds, nan_value=-110):
        super(LocalisationModelLearn, self).__init__(nan_value=nan_value, folds=folds)
        self.nan_value = nan_value
        self.folds = folds
    
    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        data = list(sources[0].window(interval, force_calculation=True))
        
        keep_inds = []
        for di, (tt, dd) in enumerate(data):
            exp = dd['annotations']['Experiment']
            loc = dd['annotatinos']['Location']
            
            if 'MIX' not in exp and 'MIX' not in loc:
                if exp in self.folds:
                    keep_inds.append(di)
        
        train_x = [data[ii][1]['rssi'] for ii in keep_inds]
        train_y = [list(data[ii][1]['annotations']['Location'])[0] for ii in keep_inds]
        
        label_encoder = LabelEncoder()
        train_y_trans = label_encoder.fit_transform(train_y)
        
        param_dict = {
            'vectorisation': DictVectorizer(sparse=False),
            'fill_missing': FillZeros(-110),
            'classifier': LinearDiscriminantAnalysis(),
            'calibration': CalibratedClassifierCV('sigmoid'),
            'label_encoder': label_encoder
        }
        
        clf = Pipeline([(kk, param_dict[kk]) for kk in ('vectorisation', 'fill_missing', 'classifier')])
        clf.fit(train_x, train_y_trans)
        
        for step_name, step_model in param_dict.iteritems():
            step_dict = {}
            for kk, vv in step_model.__dict__.iteritems():
                print step_name, kk, type(vv), vv
                
                try:
                    if vv is None:
                        step_dict[kk] = None
                    elif type(vv).__module__ == np.__name__:
                        step_dict[kk] = vv.tolist()
                    elif isinstance(vv, (dict, str, list, bool)) and not isinstance(vv, type):
                        step_dict[kk] = vv
                        
                except Exception as ex:
                    raise ex
            
            param_dict[step_name] = step_dict
        
        yield StreamInstance(interval.end, param_dict)
