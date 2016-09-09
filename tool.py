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
        
        self.sources = None
        self.sink = None
    
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
            for interval in required_intervals:
                source_views = [source for source in sources] if sources else None
                for stream_instance in self._execute(
                        sources=source_views, alignment_stream=alignment_stream, interval=interval):
                    sink.writer(stream_instance)
                sink.calculated_intervals += TimeIntervals([interval])

            required_intervals = TimeIntervals([interval]) - sink.calculated_intervals
            if not required_intervals.is_empty:
                raise RuntimeError('Tool execution did not cover the specified time interval.')

        # for stream_instance in self._execute(sources, interval):
        #     sink.writer(stream_instance)


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


class ExplicitFactor(Printable, Hashable):
    def __init__(self, tool, sources, sink):
        if sources is not None and not isinstance(sources, list):
            raise ValueError("Sources should be a list of streams")
        
        self.tool = tool
        self.sources = sources
        self.sink = sink
        
        self.sink.set_tool_reference(self)
    
    def execute(self, interval):
        if not isinstance(interval, TimeInterval):
            raise TypeError('Expected TimeInterval, got {}'.format(type(interval)))
        
        self.propagate_computation(interval)
        
        logging.info("Executing code for stream: {}".format(self.sink))
        for stream_instance in self.tool._execute(self.sources, None, interval):
            self.sink.writer(stream_instance)
    
    def propagate_computation(self, interval):
        if self.sources is not None:
            for source in self.sources:
                assert isinstance(source.tool_reference, ExplicitFactor)
                
                logging.info("Propagating inference back to: {}".format(source))
                source.tool_reference.execute(interval)
