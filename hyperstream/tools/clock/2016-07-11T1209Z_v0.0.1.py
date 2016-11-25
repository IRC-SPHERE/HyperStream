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

from hyperstream import TimeInterval
from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count
from hyperstream.utils import MIN_DATE
from datetime import timedelta
import logging


class Clock(Tool):
    def __init__(self, first=MIN_DATE, stride=timedelta(seconds=1)):
        """
        Simple clock ticker tool
        :param first: Start of the clock
        :param stride: Tick stride as timedelta
        """
        super(Clock, self).__init__(first=first, stride=stride)
        # TODO: type checking
        self.first = first
        
        if isinstance(stride, float):
            stride = timedelta(seconds=stride)
            
        assert isinstance(stride, timedelta)
        self.stride = stride

    def message(self, interval):
        return '{} running from {} to {} with stride {}'.format(
            self.__class__.__name__, str(interval.start), str(interval.end), str(self.stride))

    @check_input_stream_count(0)
    def _execute(self, sources, alignment_stream, interval):
        if interval.start < self.first:
            interval.start = self.first
        n_strides = int((interval.start - self.first).total_seconds() // self.stride.total_seconds())
        t = self.first + n_strides * self.stride
        while t <= interval.end:
            if t > interval.start:
                yield StreamInstance(t, t)
            t += self.stride
