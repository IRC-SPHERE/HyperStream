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

from hyperstream.stream import StreamInstance
from hyperstream.tool import AggregateTool
from hyperstream.utils import MIN_DATE


class Aggregate(AggregateTool):
    """
    This tool aggregates over a given plate, for example, if the input is all the streams in a node on plate A.B,
    and the aggregation is over plate B, the results will live on plate A alone.
    This can also be thought of as marginalising one dimension of a tensor over the plates
    """
    def __init__(self, func, aggregation_meta_data):
        super(Aggregate, self).__init__(func=func, aggregation_meta_data=aggregation_meta_data)
        self.func = func

    def _execute(self, sources, alignment_stream, interval):

        # Put all of the data in a dict of sorted lists (inefficient!)
        data = dict((source.stream_id,
                     sorted(source.window(interval, force_calculation=True), key=lambda x: x.timestamp))
                    for source in sources)

        # Create a set of all of the timestamps available (also inefficient!)
        timestamps = sorted(set(item.timestamp for d in data.values() for item in d))

        # maintain dict of indices where the timestamps appear
        last_timestamps = dict((stream_id, MIN_DATE) for stream_id in data)

        # Now loop through the timestamps, and aggregate over the aggregation plate
        for ts in timestamps:
            values = []
            for stream_id in data:
                for item in data[stream_id]:
                    if item.timestamp < last_timestamps[stream_id]:
                        continue
                    if item.timestamp < ts:
                        continue
                    if item.timestamp == ts:
                        values.append(item.value)
                    last_timestamps[stream_id] = item.timestamp
                    break
            yield StreamInstance(ts, self.func(values))

