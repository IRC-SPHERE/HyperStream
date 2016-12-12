# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

from ..time_interval import TimeInterval, TimeIntervals
from ..utils import Printable
from . import StreamInstance

import logging
from collections import deque


class StreamView(Printable):
    """
    Simple helper class for storing streams with a time interval (i.e. a "view" on a stream)
    :param stream: The stream upon which this is a view
    :param time_interval: The time interval over which this view is defined
    :param force_calculation: Whether we should force calculation for this stream view if data does not exist
    :type stream: Stream
    :type time_interval: TimeInterval
    """
    def __init__(self, stream, time_interval, force_calculation=False):
        from . import Stream
        if not isinstance(stream, Stream):
            raise ValueError("stream must be Stream object")
        if not isinstance(time_interval, TimeInterval):
            raise ValueError("relative_time_interval must be TimeInterval object")
        self.stream = stream
        self.time_interval = time_interval
        self.force_calculation = force_calculation

    def __iter__(self):
        required_intervals = TimeIntervals([self.time_interval]) - self.stream.calculated_intervals
        from . import AssetStream
        # if not isinstance(self.stream, AssetStream) and not required_intervals.is_empty:
        if not required_intervals.is_empty:
            if self.force_calculation:
                if self.stream.parent_node is not None and self.stream.parent_node.factor is not None:
                    # Try to request upstream computation
                    for interval in required_intervals:
                        self.stream.parent_node.factor.execute(interval)

            # Is there still computation needing doing?
            required_intervals = TimeIntervals([self.time_interval]) - self.stream.calculated_intervals
            if not required_intervals.is_empty:
                logging.warn(
                    "Stream {} not available for time interval {}. Perhaps upstream calculations haven't been performed"
                    .format(self.stream.stream_id, required_intervals))

        for item in self.stream.channel.get_results(self.stream, self.time_interval):
            yield item

    def items(self):
        """
        Return all results as a list
        :return: The results
        :rtype: list[StreamInstance]
        """
        return list(self.iteritems())

    def iteritems(self):
        return iter(self)

    def dict_iteritems(self, flat=True):
        return map(lambda x: x.as_dict(flat=flat), self)

    def dict_items(self, flat=True):
        return list(self.dict_iteritems(flat))

    def timestamps(self):
        return list(self.itertimestamps())

    def itertimestamps(self):
        return map(lambda x: x.timestamp, self.iteritems())

    def values(self):
        return list(self.itervalues())

    def itervalues(self):
        return map(lambda x: x.value, self.iteritems())

    def last(self, default=None):
        item = default
        for item in self:
            pass
        return item

    def first(self, default=None):
        for el in self:
            return el
        return default

    def head(self, n):
        i = 0
        for d in self.iteritems():
            i += 1
            if i > n:
                break
            yield d

    def tail(self, n):
        return iter(deque(self, maxlen=n))

    def component(self, key):
        # TODO: is this needed now we have a Component() tool?
        for (time, data) in self.iteritems():
            if key in data:
                yield StreamInstance(time, data[key])

    def component_filter(self, key, values):
        # TODO: is this needed now we have a ComponentFilter() tool?
        for (time, data) in self.iteritems():
            if key in data and data[key] in values:
                yield StreamInstance(time, data)

    def delete_nones(self):
        # TODO: Test this against ComponentFilter(key, values=[None], complement=true)
        for (time, data) in self.iteritems():
            data2 = {}
            for (key, value) in data.items():
                if value is not None:
                    data2[key] = value
            yield StreamInstance(time, data2)