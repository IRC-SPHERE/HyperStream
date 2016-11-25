"""
The MIT License (MIT)
Copyright (c) 2014-2017 University of Bristol

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.
"""

from hyperstream import TimeInterval
from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count
from hyperstream.utils import MIN_DATE
from datetime import timedelta
import logging


class SlidingWindow(Tool):
    def __init__(self, first=MIN_DATE, lower=timedelta(seconds=-1), upper=timedelta(seconds=0),
                 increment=timedelta(seconds=1)):
        """
        Simple clock ticker tool
        :param first: Start of the clock
        :param lower: The lower end of the sliding execute
        :param upper: The upper end of the sliding execute
        """
        super(SlidingWindow, self).__init__(
            first=first,
            lower=lower,
            upper=upper,
            increment=increment
        )

        if upper <= lower:
            raise ValueError("upper should be strictly greater than lower")

        lower = get_timedelta(lower)
        upper = get_timedelta(upper)
        increment = get_timedelta(increment)

        self.first = first
        self.lower = upper
        self.width = upper - lower
        self.increment = increment
    
    @check_input_stream_count(0)
    def _execute(self, sources, alignment_stream, interval):
        if interval.start < self.first:
            interval.start = self.first

        n_widths = int((interval.start - self.first).total_seconds() // self.width.total_seconds())
        
        lower = self.first + n_widths * self.width
        upper = lower + self.width
        
        while upper <= interval.end:
            yield StreamInstance(upper, TimeInterval(lower, upper))

            lower += self.increment
            upper += self.increment


def get_timedelta(value):
    if isinstance(value, int):
        return timedelta(seconds=value)
    elif isinstance(value, timedelta):
        return value
    else:
        raise ValueError("Expected int or timedelta, got {}".format(type(value)))
