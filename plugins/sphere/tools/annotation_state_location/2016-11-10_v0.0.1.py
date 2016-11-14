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
from hyperstream import TimeInterval
from copy import deepcopy
import logging
import datetime


class AnnotationStateLocation(Tool):
    """
    This tool currently assumes non-overlapping sliding windows in its first input stream
    """
    def __init__(self):
        super(AnnotationStateLocation, self).__init__()

    @check_input_stream_count(2)
    def _execute(self, sources, alignment_stream, interval):
        data = list(sources[1].window(interval, force_calculation=True))
        timestamps = [x.timestamp for x in data]
        try:
            interval2 = TimeInterval(min(timestamps) - datetime.timedelta(microseconds=1), max(timestamps))
        except ValueError:
            # TODO: Something more intelligent - This is raised when the source stream is empty
            return

        windows = iter(sources[0].window(interval2, force_calculation=True))
        data = iter(data)

        # run from self.start_time to interval.start to find out the state at the beginning of the interval
        # then from there on yield documents according to the sliding window,
        # containing the active annotations within the window

        win_future = []
        data_future = []
        annotations = {}

        while True:
            if len(data_future) == 0:
                try:
                    doc = next(data)
                    data_future.append(doc)
                except StopIteration:
                    pass
            if len(win_future) == 0:
                try:
                    _, win = next(windows)
                    win_future.append(win)
                    win_start = win_future[0].start
                    win_end = win_future[0].end
                    win_annotations = deepcopy(annotations)
                except StopIteration:
                    pass
            if len(win_future) == 0:
                return

            # the current annotation has potential effect on the current window
            if (len(data_future) > 0) and (data_future[0].timestamp <= win_end):
                doc = data_future.pop(0)
                tt, dd = doc
                trigger = dd['trigger']
                tier = dd['tier']
                label = dd['label']
                if not annotations.has_key(tier):
                    annotations[tier] = set()
                if trigger == 1:
                    annotations[tier].add(label)
                elif trigger == -1:
                    try:
                        annotations[tier].remove(label)  # raises KeyError if removing a label which is not present
                    except KeyError:
                        logging.warn("At time {} label {} of tier {} ending without a start".
                                     format(tt, label, tier))
                else:
                    raise ValueError("trigger must have value +1 or -1")

                if tt > win_start:  # the current annotation is changing a tier within the current window
                    win_annotations[tier] = {'MIX'}
                else:
                    win_annotations[tier] = annotations[tier].copy()
            else:
                # must yield a window because it has been finished
                yield StreamInstance(win_end, win_annotations)
                win_future.pop(0)
