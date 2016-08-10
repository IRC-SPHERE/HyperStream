"""
The MIT License (MIT)
Copyright (c) 2014-2017 University of Bristol

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.
"""

from hyperstream import Tool
from datetime import datetime, timedelta


class Clock(Tool):
    def __str__(self):
        return __name__
    
    def __hash__(self):
        return hash(__name__)
    
    def process_params(self, first=datetime.min, stride=timedelta(seconds=1), optim=42):
        print('Defining a Clock stream')
        return [], {'first': first, 'stride': stride, 'optim': optim}
    
    def normalise_kwargs(self, kwargs):
        kwargs2 = {}
        for k in kwargs:
            # TODO ... bug below?
            if k not in 'optim':
                kwargs2[k] = kwargs[k]
        return kwargs2
    
    def __call__(self, stream_def, start, end, writer, first, stride, optim):
        print('Clock running from ' + str(start) + ' to ' + str(end) + ' with stride ' + str(stride))
        if start < first:
            start = first
        n_strides = int((start - first).total_seconds() // stride.total_seconds())
        t = first + n_strides * stride
        while t <= end:
            if t > start:
                writer([(t, t)])
            t += stride
