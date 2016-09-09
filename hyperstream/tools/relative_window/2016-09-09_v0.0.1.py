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
from hyperstream.stream import StreamInstance, RelativeStreamView
from hyperstream.tool import Tool, check_input_stream_count
from datetime import timedelta
import logging


class RelativeWindow(Tool):
    def __init__(self, values_only=True):
        super(RelativeWindow, self).__init__(values_only=values_only)

        self.values_only = values_only

    # noinspection PyCompatibility
    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        data_stream = sources[0]
        assert isinstance(data_stream, RelativeStreamView)

        rel_start = data_stream.relative_time_interval.start
        rel_end = data_stream.relative_time_interval.end
        
        data = iter(data_stream.window(interval))
        
        window = []
        future = []
        for (t, _) in alignment_stream.window(interval):
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
            
            if self.values_only:
                yield StreamInstance(t, map(lambda si: si.value, window))
                
            else:
                yield StreamInstance(t, list(window))
                
            # for (t, _) in alignment_stream.window(interval):
            #     lower = rel_start + t
            #     upper = rel_end + t
            #
            #     # Prune the old data points from the window
            #     num_to_remove = 0
            #     for win_time, win_data in window:
            #         if lower <= win_time <= upper:
            #             break
            #
            #         num_to_remove += 1
            #     window = window[num_to_remove:]
            #
            #     # Add those stolen from the future
            #     num_to_remove = 0
            #     for doc in future:
            #         fut_time, fut_data = doc
            #
            #         if lower <= fut_time <= upper:
            #             break
            #
            #         num_to_remove += 1
            #         window.append(doc)
            #     future = future[num_to_remove:]
            #
            #     # Take data from the execute
            #     while True:
            #         try:
            #             doc = next(data)
            #             tt, dd = doc
            #
            #             if lower <= tt <= upper:
            #                 window.append(doc)
            #
            #             elif tt > upper:
            #                 future.append(doc)
            #                 break
            #
            #         except StopIteration:
            #             break
            #
            #     # print interval.start, interval.end
            #     # print '\t', lower, upper
            #     # for datum in execute:
            #     #     print '\t\t{} {}'.format(datum.timestamp, datum.value)
            #     # print '\t', self.func(execute)
            #     # print
            #
            #     yield StreamInstance(t, window)