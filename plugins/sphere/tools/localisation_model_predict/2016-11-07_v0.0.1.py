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

from sklearn.feature_extraction import DictVectorizer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder

from plugins.sphere.utils import FillZeros
from hyperstream.utils import MIN_DATE, MAX_DATE
from hyperstream.time_interval import TimeInterval

from plugins.sphere.utils import deserialise_json_pipeline


class LocalisationModelPredict(Tool):
    """
    Given a dataframe of
    """
    
    def __init__(self):
        super(LocalisationModelPredict, self).__init__()
    
    @check_input_stream_count(2)
    def _execute(self, sources, alignment_stream, interval):
        param_doc = sources[0].window(TimeInterval(MIN_DATE, MAX_DATE), force_calculation=True).last()
        if param_doc is None:
            return
        
        steps = deserialise_json_pipeline({
            'vectorisation': DictVectorizer(sparse=False),
            'fill_missing': FillZeros(),
            'classifier': LinearDiscriminantAnalysis(),
            'label_encoder': LabelEncoder()
        }, param_doc.value)
        
        clf = Pipeline([(kk, steps[kk]) for kk in ('vectorisation', 'fill_missing', 'classifier')])
        locations = steps['label_encoder'].classes_
        
        data = sources[1].window(interval, force_calculation=True)
        for tt, dd in data:
            yield StreamInstance(tt, {locations[ii]: pp for ii, pp in enumerate(clf.predict_proba(dd)[0])})
