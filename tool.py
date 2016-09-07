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
from time_interval import TimeInterval
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

    def _execute(self, input_streams, interval):
        raise NotImplementedError

    def execute(self, input_streams, interval, writer):
        if not isinstance(interval, TimeInterval):
            raise TypeError('Expected TimeInterval, got {}'.format(type(interval)))
        logging.info(self.message(interval))
        
        for stream_instance in self._execute(input_streams, interval):
            writer(stream_instance)


def check_input_stream_count(expected_number_of_streams):
    """
    Decorator for Tool._execute that checks the number of input streams
    :param expected_number_of_streams: The expected number of streams
    :return: the decorator
    """
    def stream_count_decorator(func):
        def func_wrapper(*args, **kwargs):
            self = args[0]
            input_streams = kwargs['time_interval'] if 'time_interval' in kwargs else args[1]
            if expected_number_of_streams == 0:
                if input_streams:
                    raise ValueError("No input streams expected")
            else:
                given_number_of_streams = len(input_streams) if input_streams else 0
                if given_number_of_streams != expected_number_of_streams:
                    raise ValueError("{} tool takes {} stream(s) as input ({} given)".format(
                        self.__class__.__name__, expected_number_of_streams, given_number_of_streams))
            return func(*args, **kwargs)
        return func_wrapper
    return stream_count_decorator
