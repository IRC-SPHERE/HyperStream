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
from ..utils import StreamNotAvailableError, ToolExecutionError

from datetime import timedelta
import logging


class Tool(BaseTool):
    """
    Base class for tools. Tools are the unit of computation, operating on input streams to produce an output stream
    """

    def _execute(self, sources, alignment_stream, interval):
        """
        Tool implementations should override this function to actually perform computations

        :param sources: The source streams (possibly None)
        :param alignment_stream: The alignment stream
        :param interval: The time interval
        :type sources: list[Stream] | tuple[Stream] | None
        :type alignment_stream: Stream | None
        :type interval: TimeInterval
        :return: None
        """
        raise NotImplementedError

    def execute(self, sources, sink, alignment_stream, interval):
        """
        Execute the tool over the given time interval.
        If an alignment stream is given, the output instances will be aligned to this stream

        :param sources: The source streams (possibly None)
        :param sink: The sink stream
        :param alignment_stream: The alignment stream
        :param interval: The time interval
        :type sources: list[Stream] | tuple[Stream] | None
        :type sink: Stream
        :type alignment_stream: Stream | None
        :type interval: TimeInterval
        :return: None
        """
        if not isinstance(interval, TimeInterval):
            raise TypeError('Expected TimeInterval, got {}'.format(type(interval)))
        # logging.info(self.message(interval))

        if interval.end > sink.channel.up_to_timestamp:
            raise StreamNotAvailableError(sink.channel.up_to_timestamp)

        required_intervals = TimeIntervals([interval]) - sink.calculated_intervals

        if not required_intervals.is_empty:
            produced_data = False

            for interval in required_intervals:
                for stream_instance in self._execute(
                        sources=sources, alignment_stream=alignment_stream, interval=interval):
                    sink.writer(stream_instance)
                    produced_data = True
                sink.calculated_intervals += interval

            required_intervals = TimeIntervals([interval]) - sink.calculated_intervals
            if not required_intervals.is_empty:
                # raise ToolExecutionError(required_intervals)
                logging.error("{} execution error for time interval {} on stream {}".format(
                    self.name, interval, sink))

            if not produced_data:
                logging.debug("{} did not produce any data for time interval {} on stream {}".format(
                    self.name, interval, sink))
