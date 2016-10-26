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

from hyperstream import TimeInterval, RelativeTimeInterval
from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count
from hyperstream.utils import MIN_DATE, get_timedelta
from datetime import timedelta
import logging


class AligningWindow(Tool):
    def __init__(self, lower=timedelta(seconds=-1), upper=timedelta(seconds=0)):
        """
        Simple clock ticker tool
        :param first: Start of the clock
        :param lower: The lower end of the sliding execute
        :param upper: The upper end of the sliding execute
        """
        super(AligningWindow, self).__init__(
            lower=lower,
            upper=upper
        )

#        # Use relative time interval since it supports validation
#        relative_interval = RelativeTimeInterval(lower, upper)

#        self.lower = relative_interval.end
#        self.width = relative_interval.width
        self.lower = lower
        self.upper = upper

#        # Additional validation for the increment
#        self.increment = get_timedelta(increment)

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        for (time,_) in sources[0].window(interval, force_calculation=True):
            yield StreamInstance(time,TimeInterval(time+self.lower,time+self.upper))

