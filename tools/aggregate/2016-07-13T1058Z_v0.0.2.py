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

from hyperstream import TimeInterval, RelativeTimeInterval
from hyperstream.tool import Tool, check_input_stream_count
from datetime import timedelta
import logging


# TODO from niall: suggest renaming this class to "WindowApply" or similar
class Aggregate(Tool):
    def __init__(self, timer, func):
        super(Aggregate, self).__init__(timer=timer, func=func)
        self.timer = timer
        self.func = func

    # noinspection PyCompatibility
    @check_input_stream_count(1)
    def _execute(self, input_streams, interval, writer):
        rel_start = timedelta(0)
        rel_end = timedelta(0)
        if isinstance(input_streams[0].time_interval, RelativeTimeInterval):
            rel_start = input_streams[0].time_interval.start
            rel_end = input_streams[0].time_interval.end
        elif isinstance(input_streams[0].time_interval, TimeInterval):
            if interval != input_streams[0].time_interval:
                # TODO: What if stream and input stream have different absolute intervals?
                logging.error("interval {} != input_streams[0].time_interval {}".format(
                    interval, input_streams[0].time_interval))
                raise NotImplementedError

        data = input_streams[0].execute(interval).iteritems()

        window = []
        future = []
        for (t, _) in self.timer.execute(interval).iteritems():
            while (len(window) > 0) and (window[0][0] <= t + rel_start):
                window = window[1:]
            while (len(future) > 0) and (future[0][0] <= t + rel_end):
                doc = future[0]
                future = future[1:]
                if t + rel_start < doc[0] <= t + rel_end:
                    window.append(doc)
            while True:
                try:
                    doc = next(data)
                    if t + rel_start < doc[0] <= t + rel_end:
                        window.append(doc)
                    elif doc[0] > t + rel_end:
                        future.append(doc)
                        break
                    else:
                        pass
                except StopIteration:
                    break
            # single-document case:
            writer([(t, self.func(iter(window)))])
            # multi-document case:
            # for x in func( (doc for doc in execute) ):
            #        writer([(t,x)])
