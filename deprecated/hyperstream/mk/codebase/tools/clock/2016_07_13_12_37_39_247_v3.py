from hyperstream import Tool
from hyperstream_intervals import MIN_DATE, date, delta


class Clock(Tool):
    def __str__(self):
        return (__name__)
    
    def __hash__(self):
        return (hash(__name__))
    
    def process_params(self, first=MIN_DATE, stride=delta(seconds=1), optim=42, optim2=83):
        print('Defining a Clock stream')
        return ([], {'first': first, 'stride': stride, 'optim': optim, 'optim2': optim2})
    
    def normalise_kwargs(self, kwargs):
        kwargs2 = {}
        for k in kwargs:
            if not k in ('optim', 'optim2'):
                kwargs2[k] = kwargs[k]
        return (kwargs2)
    
    def __call__(self, stream_def, start, end, writer, first, stride, optim, optim2):
        print('Clock running from ' + str(start) + ' to ' + str(end) + ' with stride ' + str(stride))
        if start < first:
            start = first
        n_strides = int((start - first).total_seconds() // stride.total_seconds())
        t = first + n_strides * stride
        while t <= end:
            if t > start:
                writer([(t, t)])
            t = t + stride
