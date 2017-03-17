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


class MultiOutputTool(BaseTool):
    """
    Special type of tool that outputs multiple streams on a new plate rather than a single stream.
    There are in this case multiple sinks rather than a single sink, and a single source rather than multiple sources.
    Note that no alignment stream is required here.
    Also note that we don't subclass Tool due to different calling signatures
    """
    def _execute(self, source, splitting_stream, interval, output_plate):
        """
        Tool implementations should override this function to actually perform computations

        :param source: The source stream
        :param splitting_stream: The stream over which to split
        :param interval: The time interval over which to calculate
        :param output_plate: The plate where data is put onto
        :type source: Stream
        :type interval: TimeInterval
        :type output_plate: Plate
        :return: None
        """
        raise NotImplementedError

    def execute(self, source, splitting_stream, sinks, interval, input_plate_value, output_plate):
        """
        Execute the tool over the given time interval.

        :param source: The source stream
        :param splitting_stream: The stream over which to split
        :param sinks: The sink streams
        :param interval: The time interval
        :param input_plate_value: The value of the plate where data comes from (can be None)
        :param output_plate: The plate where data is put onto
        :type source: Stream
        :type sinks: list[Stream] | tuple[Stream]
        :type interval: TimeInterval
        :type input_plate_value: tuple[tuple[str, str]] | None
        :type output_plate: Plate
        :return: None
        """
        if not isinstance(interval, TimeInterval):
            raise TypeError('Expected TimeInterval, got {}'.format(type(interval)))
        # logging.info(self.message(interval))

        calculated_intervals = None

        for sink in sinks:
            if interval.end > sink.channel.up_to_timestamp:
                raise ValueError(
                    'The stream is not available after {} and cannot be calculated'.format(
                        sink.channel.up_to_timestamp))
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
            produced_data = False

            for interval in required_intervals:
                for item in self._execute(source=source, splitting_stream=splitting_stream,
                                          interval=interval, output_plate=output_plate):
                    # Join the output meta data with the parent plate meta data
                    meta_data = input_plate_value + (item.meta_data,) if input_plate_value else (item.meta_data, )
                    try:
                        sink = next(s for s in sinks if set(s.stream_id.meta_data) == set(meta_data))
                        sink.writer(item.stream_instance)
                        produced_data = True
                    except StopIteration:
                        logging.warn("A multi-output tool has produced a value {} "
                                     "which does not belong to the output plate".format(meta_data))
                        continue
                    except TypeError:
                        logging.error("A multi-output tool has produced a value {} "
                                      "which cannot be hashed and does not belong to the output plate"
                                      .format(meta_data))
            if not produced_data:
                logging.debug("{} did not produce any data for time interval {} on stream {}".format(
                    self.name, required_intervals, source))
