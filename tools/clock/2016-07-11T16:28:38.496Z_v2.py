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

    def normalise_tool(self):
        return 'codebase.tools.clock.2016-07-11T12:09:11:807Z_v1'

    def normalise_kwargs(self, kwargs):
        kwargs2 = {}
        for k in kwargs:
            if not k in ('optim'):
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
