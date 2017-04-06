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

from . import BaseTool
from ..time_interval import TimeInterval, TimeIntervals
from ..stream import Stream

import logging
from collections import defaultdict


class SelectorTool(BaseTool):
    """
    This type of tool performs sub-selection of streams within a node.
    This can either be done using a selector in the parameters or using an input stream.
    The sink node plate should be a sub-plate of the source node.
    Examples are IndexOf and SubArray, either with fixed or variable parameters
    """
    def __init__(self, selector_meta_data, **kwargs):
        super(SelectorTool, self).__init__(selector_meta_data=selector_meta_data, **kwargs)
        self.selector_meta_data = selector_meta_data

    def _execute(self, sources, interval):
        raise NotImplementedError

    def execute(self, sources, sinks, interval):
        """
        Execute the tool over the given time interval.

        :param sources: The source streams
        :param sinks: The sink streams
        :param interval: The time interval
        :type sources: list[Stream] | tuple[Stream]
        :type sinks: list[Stream] | tuple[Stream]
        :type interval: TimeInterval
        :return: None
        """
        if not isinstance(interval, TimeInterval):
            raise TypeError('Expected TimeInterval, got {}'.format(type(interval)))
        # logging.info(self.message(interval))

        calculated_intervals = None

        for sink in sinks:
            if interval.end > sink.channel.up_to_timestamp:
                raise ValueError('The stream is not available after {} and cannot be calculated'
                                 .format(sink.channel.up_to_timestamp))
            if calculated_intervals is None:
                calculated_intervals = sink.calculated_intervals
                continue
            if sink.calculated_intervals != calculated_intervals:
                # TODO: What we actually want to do here is find any parts of the sinks that haven't been calculated,
                # and recompute all of the sinks for that time period. This would only happen if computation of one of
                # the sinks failed for some reason. For now we will just assume that all sinks have been computed the
                # same amount, and we will raise an exception if this is not the case
                raise RuntimeError("Partially executed sinks not yet supported")

        required_intervals = TimeIntervals([interval]) - calculated_intervals

        if not required_intervals.is_empty:
            for interval in required_intervals:
                produced_data = set()

                for item in self._execute(sources=sources, interval=interval):
                    # Join the output meta data with the parent plate meta data
                    try:
                        sink = next(s for s in sinks
                                    if tuple(sorted(s.stream_id.meta_data)) == tuple(sorted(item.meta_data)))
                    except StopIteration:
                        # raise
                        continue
                    sink.writer(item.stream_instance)
                    produced_data.add(sink)

                for sink in sinks:
                    if sink not in produced_data:
                        logging.debug("{} did not produce any data for time interval {} on sink {}".format(
                            self.name, interval, sink))
                    sink.calculated_intervals += interval
