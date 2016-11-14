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


from sklearn.base import BaseEstimator, TransformerMixin
from copy import deepcopy
from numpy import asarray, __name__ as numpy_name


class FillZeros(BaseEstimator, TransformerMixin):
    def __init__(self, default_value=-110):
        super(FillZeros, self).__init__()
        self.default_value = -110
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X, y=None):
        X[X == 0] = self.default_value
        
        return X


def serialise_sklearn_model(model):
    """
    
    :param model:
    :return:
    """
    
    if model is dict:
        return model
    
    params = {}
    
    for kk, vv in model.__dict__.iteritems():
        try:
            if isinstance(vv, type):
                continue
            
            if vv is None:
                params[kk] = None
            
            elif type(vv).__module__ == numpy_name:
                params[kk] = vv.tolist()
            
            elif isinstance(vv, (dict, str, list, bool, int, float)):
                params[kk] = vv
        
        except Exception as ex:
            raise ex
    
    return params


def serialise_json_pipeline(param_dict):
    """
    
    :param param_dict:
    :return:
    """
    param_dict = deepcopy(param_dict)
    
    # TODO: look into using get_params/set_params as an alternative to this
    for step_name, step_model in param_dict.iteritems():
        if isinstance(step_model, (dict, int, list)):
            continue
            
        param_dict[step_name] = serialise_sklearn_model(step_model)
    
    return param_dict


def deserialise_json_pipeline(steps, param_dict):
    """
    
    :param steps:
    :param param_dict:
    :return:
    """
    
    steps = deepcopy(steps)
    
    for step_name in steps.keys():
        for param_name, param_value in param_dict[step_name].iteritems():
            if isinstance(param_value, list):
                setattr(steps[step_name], param_name, asarray(param_value))
            else:
                setattr(steps[step_name], param_name, param_value)
    
    return steps
