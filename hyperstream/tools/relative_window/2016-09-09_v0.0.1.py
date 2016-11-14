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

from hyperstream import RelativeTimeInterval
from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count
from datetime import timedelta


class RelativeWindow(Tool):
    def __init__(self, relative_start, relative_end, values_only=True):
        super(RelativeWindow, self).__init__(
            relative_start=relative_start, relative_end=relative_end, values_only=values_only)
        self.values_only = values_only
        self.relative_interval = RelativeTimeInterval(start=relative_start, end=relative_end)

    # noinspection PyCompatibility
    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        data_stream = sources[0]

        data = iter(data_stream.window(interval + self.relative_interval))
        
        window = []
        future = []
        for (t, _) in alignment_stream.window(interval, force_calculation=True):
            while (len(window) > 0) and (window[0][0] <= t + timedelta(seconds=self.relative_interval.start)):
                window = window[1:]
            while (len(future) > 0) and (future[0][0] <= t + timedelta(seconds=self.relative_interval.end)):
                doc = future[0]
                future = future[1:]
                if t + timedelta(seconds=self.relative_interval.start) < doc[0] \
                        <= t + timedelta(seconds=self.relative_interval.end):
                    window.append(doc)
            while True:
                try:
                    doc = next(data)
                    if t + timedelta(seconds=self.relative_interval.start) < doc[0] \
                            <= t + timedelta(seconds=self.relative_interval.end):
                        window.append(doc)
                    elif doc[0] > t + timedelta(seconds=self.relative_interval.end):
                        future.append(doc)
                        break
                    else:
                        pass
                except StopIteration:
                    break
            
            if self.values_only:
                yield StreamInstance(t, map(lambda si: si.value, window))
                
            else:
                yield StreamInstance(t, list(window))
