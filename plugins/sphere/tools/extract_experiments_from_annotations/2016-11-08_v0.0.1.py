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
from hyperstream.utils import MIN_DATE
from hyperstream import TimeInterval, TimeIntervals
from copy import deepcopy
import logging

# this tool currently assumes non-overlapping sliding windows in its first input stream

class ExtractExperimentsFromAnnotations(Tool):
    def __init__(self):
        super(ExtractExperimentsFromAnnotations, self).__init__()

    # noinspection PyCompatibility
    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        interval2 = TimeInterval(MIN_DATE,interval.end)
        data = iter(sources[1].window(interval2, force_calculation=True))
        exp_list = []
        for doc in data:
            if doc.tier=="Experiment":
                exp_list.append(doc)
        for i in range(len(exp_list)):
            doc = exp_list[i]
            if (doc.end<=interval.end) and (doc.start>=interval.start):
                yield StreamInstance(doc.end,doc)
