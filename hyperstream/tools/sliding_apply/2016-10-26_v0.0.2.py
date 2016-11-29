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

from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count


class SlidingApply(Tool):
    def __init__(self, func):
        super(SlidingApply, self).__init__(func=func)
        self.func = func
    
    # noinspection PyCompatibility
    @check_input_stream_count(2)
    def _execute(self, sources, alignment_stream, interval):
        sliding_window = sources[0].window(interval, force_calculation=True)
        data = iter(sources[1].window(interval, force_calculation=True))
        
        window = []
        future = []
        
        for time, rel_window in sliding_window:
            lower = rel_window.start
            upper = rel_window.end
            
            # Prune the old data points from the window
            num_to_remove = 0
            for win_time, win_data in window:
                if lower < win_time <= upper: # MK: changed from lower <= win_time <= upper
                    break
                    
                num_to_remove += 1
            window = window[num_to_remove:]
            
            # Add those stolen from the future
            num_to_remove = 0
            for doc in future:
                fut_time, fut_data = doc
                
                # if lower <= fut_time <= upper: (MK: this was a bug because things in the far future were thrown away from the future
                #    break
                if fut_time > upper: # added by MK: if in the far future, then must remain in future
                    break

                num_to_remove += 1
                if fut_time >= lower:
                    window.append(doc)
            future = future[num_to_remove:]
            
            # Take data from the execute
            while True:
                try:
                    doc = next(data)
                    tt, dd = doc
                    
                    if lower < tt <= upper: # MK: changed from lower <= win_time <= upper
                        window.append(doc)
                        
                    elif tt > upper:
                        future.append(doc)
                        break
                        
                except StopIteration:
                    break

            # print interval.start, interval.end
            # print '\t', lower, upper
            # for datum in execute:
            #     print '\t\t{} {}'.format(datum.timestamp, datum.value)
            # print '\t', self.func(execute)
            # print
            
            value = self.func(iter(window))
            try:
                if len(value) > 0:
                    yield StreamInstance(time, value)
                else:
                    # TODO: Should we yield anything???
                    # yield StreamInstance(time, {})
                    pass
            except TypeError:
                # Not iterable
                yield StreamInstance(time, value)
