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
        max_interval = TimeInterval(MIN_DATE, interval.end)
        exp_list = {}
        for timestamp, value in sources[0].window(max_interval, force_calculation=True):
            if value['tier'] != "Experiment":
                continue
            d = deepcopy(value)
            mongo_id = d.pop('_id')
            trigger = d.pop('trigger')
            if trigger == 1:
                u = {'start': timestamp}
            else:
                u = {'end': timestamp}

            if mongo_id in exp_list:
                if u.keys()[0] in exp_list[mongo_id]:
                    raise ValueError("Duplicate {} triggers found for timestamp {}".format(trigger, timestamp))
                exp_list[mongo_id].update(u)
            else:
                d.update(u)
                exp_list[mongo_id] = d

        for i, doc in enumerate(exp_list.values()):
            if TimeInterval(doc['start'], doc['end']) in max_interval:
                yield StreamInstance(doc['end'], doc)
            # if (doc.value['end'] <= interval.end) and (doc.value['start'] >= interval.start):
            #     yield StreamInstance(doc.value['end'], doc)
