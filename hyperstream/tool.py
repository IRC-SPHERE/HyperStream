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

import logging
from time_interval import TimeInterval, TimeIntervals
from utils import Printable, Hashable


class Tool(Printable, Hashable):
    def __init__(self, **kwargs):
        if kwargs:
            logging.debug('Defining a {} tool with parameters {}'.format(self.__class__.__name__, kwargs))
        else:
            logging.debug('Defining a {} tool'.format(self.__class__.__name__))
        
    def __eq__(self, other):
        # TODO: requires a unit test
        return isinstance(other, Tool) and hash(self) == hash(other)
    
    def message(self, interval):
        return '{} running from {} to {}'.format(self.__class__.__name__, str(interval.start), str(interval.end))
    
    @property
    def name(self):
        return self.__class__.__module__
    
    def _execute(self, sources, alignment_stream, interval):
        raise NotImplementedError
    
    def execute(self, sources, sink, alignment_stream, interval):
        if not isinstance(interval, TimeInterval):
            raise TypeError('Expected TimeInterval, got {}'.format(type(interval)))
        logging.info(self.message(interval))

        if interval.end > sink.channel.up_to_timestamp:
            raise ValueError(
                'The stream is not available after {} and cannot be calculated'.format(self.up_to_timestamp))

        required_intervals = TimeIntervals([interval]) - sink.calculated_intervals

        if not required_intervals.is_empty:
            produced_data = False

            for interval in required_intervals:
                for stream_instance in self._execute(
                        sources=sources, alignment_stream=alignment_stream, interval=interval):
                    sink.writer(stream_instance)
                    produced_data = True
                sink.calculated_intervals += TimeIntervals([interval])

            required_intervals = TimeIntervals([interval]) - sink.calculated_intervals
            if not required_intervals.is_empty:
                raise RuntimeError('Tool execution did not cover the time interval {}.'.format(required_intervals))

            if not produced_data:
                logging.warn("Tool did not produce any data for time interval {}".format(required_intervals))


class MultiOutputTool(Printable, Hashable):
    """
    Special type of tool that outputs multiple streams on a new plate rather than a single stream.
    There are in this case multiple sinks rather than a single sink, and a single source rather than multiple sources.
    Note that no alignment stream is required here.
    Also note that we don't subclass Tool due to different calling signatures
    """
    def __init__(self, **kwargs):
        if kwargs:
            logging.debug('Defining a {} tool with parameters {}'.format(self.__class__.__name__, kwargs))
        else:
            logging.debug('Defining a {} tool'.format(self.__class__.__name__))

    def __eq__(self, other):
        return isinstance(other, Tool) and hash(self) == hash(other)

    def message(self, interval):
        return '{} running from {} to {}'.format(self.__class__.__name__, str(interval.start), str(interval.end))

    @property
    def name(self):
        return self.__class__.__module__

    def _execute(self, source, interval, output_plate):
        raise NotImplementedError

    def execute(self, source, sinks, interval, input_plate_value, output_plate):
        if not isinstance(interval, TimeInterval):
            raise TypeError('Expected TimeInterval, got {}'.format(type(interval)))
        logging.info(self.message(interval))

        calculated_intervals = None

        for sink in sinks:
            if interval.end > sink.channel.up_to_timestamp:
                raise ValueError(
                    'The stream is not available after {} and cannot be calculated'.format(self.up_to_timestamp))
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
                for item in self._execute(source=source, interval=interval, output_plate=output_plate):
                    # Join the output meta data with the parent plate meta data
                    meta_data = input_plate_value + (item.meta_data,)
                    sink = next(s for s in sinks if s.stream_id.meta_data == meta_data)
                    sink.writer(item.stream_instance)
                    produced_data = True

            if not produced_data:
                logging.warn("Tool did not produce any data for time interval {} on stream {}".format(
                    required_intervals, source))


def check_input_stream_count(expected_number_of_streams):
    """
    Decorator for Tool._execute that checks the number of input streams
    :param expected_number_of_streams: The expected number of streams
    :return: the decorator
    """
    
    def stream_count_decorator(func):
        def func_wrapper(*args, **kwargs):
            self = args[0]
            sources = kwargs['sources'] if 'sources' in kwargs else args[1]
            if expected_number_of_streams == 0:
                if sources:
                    raise ValueError("No input streams expected")
            else:
                given_number_of_streams = len(sources) if sources else 0
                if given_number_of_streams != expected_number_of_streams:
                    raise ValueError("{} tool takes {} stream(s) as input ({} given)".format(
                        self.__class__.__name__, expected_number_of_streams, given_number_of_streams))
            return func(*args, **kwargs)
        
        return func_wrapper
    
    return stream_count_decorator
