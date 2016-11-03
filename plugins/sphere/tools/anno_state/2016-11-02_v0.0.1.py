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
from hyperstream import TimeInterval, TimeIntervals
from copy import deepcopy
import logging

# this tool currently assumes non-overlapping sliding windows in its first input stream

class AnnoState(Tool):
    def __init__(self, start_time):
        super(AnnoState, self).__init__(start_time=start_time)
        self.start_time = start_time

    # noinspection PyCompatibility
    @check_input_stream_count(2)
    def _execute(self, sources, alignment_stream, interval):
        interval2 = TimeInterval(self.start_time,interval.end)
        windows = iter(sources[0].window(interval, force_calculation=True))
        data = iter(sources[1].window(interval2, force_calculation=True))

        # run from self.start_time to interval.start to find out the state at the beginning of the interval
        # then from there on yield documents according to the sliding window, containing the active annotations within the window

        win_future = []
        data_future = []
        anno = {}

        while True:
            if len(data_future)==0:
                try:
                    doc = next(data)
                    data_future.append(doc)
                except StopIteration:
                    pass
            if len(win_future)==0:
                try:
                    _,win = next(windows)
                    win_future.append(win)
                    win_start = win_future[0].start
                    win_end = win_future[0].end
                    win_anno = deepcopy(anno)
                except StopIteration:
                    pass
            if len(win_future)==0:
                return
            if (len(data_future)>0) and (data_future[0].timestamp<=win_end): # the current annotation has potential effect on the current window
                doc = data_future.pop(0)
                tt, dd = doc
                trigger = dd['trigger']
                tier = dd['tier']
                label = dd['label']
                if label=="Experiment Time":
                    tier = "Experiment"
                    label = "on"
                if not anno.has_key(tier):
                    anno[tier] = set()
                if trigger==1:
                    anno[tier].add(label)
                elif trigger==-1:
                    try:
                        anno[tier].remove(label) # raises KeyError if removing a label which is not present
                    except KeyError:
                        logging.warn("At time {} label {} of tier {} ending without a start".
                                     format(tt,label,tier))
                else:
                    raise ValueError("trigger must have value +1 or -1")
                if tt>win_start: # the current annotation is changing a tier within the current window
                    win_anno[tier] = set(['MIX'])
                else:
                    win_anno[tier] = anno[tier].copy()
            else: # must yield a window because it has been finished
                if win_anno.has_key("Experiment") and win_anno["Experiment"]=={"on"}:
                    yield StreamInstance(win_end,win_anno)
                win_future.pop(0)
