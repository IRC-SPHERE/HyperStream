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

from hyperstream.tool import AggregateTool

import itertools
from copy import deepcopy
from hyperstream import StreamInstance

class AggregateIntoDictAndApply(AggregateTool):
    """
    This tool aggregates over a given plate, for example, if the input is all the streams in a node on plate A.B,
    and the aggregation is over plate B, the results will live on plate A alone.
    This can also be thought of as marginalising one dimension of a tensor over the plates.
    This version differs from Aggregate in that the timestamps are maintained, and the meta data is put into the value
    """
    def __init__(self, func, aggregation_meta_data):
        super(AggregateIntoDictAndApply, self).__init__(func=func, aggregation_meta_data=aggregation_meta_data)

    def _execute(self, sources, alignment_stream, interval):
        # Combine the data, apply the mapping and sort (inefficient!)
        results = dict()
        for source in sources:
            data = source.window(interval, force_calculation=True)
            meta_data = source.stream_id.meta_data
            d = dict(meta_data)
            try:
                v = d.pop(self.aggregation_meta_data)
            except:
                raise
            for timestamp,value in data:
                if not timestamp in results.keys():
                    results[timestamp] = StreamInstance(timestamp,dict())
                results[timestamp].value[v] = self.func(value)
        return sorted(results.values(),key=lambda x: x.timestamp)

