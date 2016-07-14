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
from __future__ import absolute_import
import pprint
from collections import namedtuple
from datetime import datetime, timedelta
from dateutil.parser import parse


class Printable(object):
    def __str__(self):
        pp = pprint.PrettyPrinter(indent=4)
        return pp.pformat(self.__dict__)


# TimeRange = namedtuple('TimeRange', ['start', 'end'])


class TimeRange(Printable):
    _start = None
    _end = None

    def __init__(self, start, end):
        self.start = start
        self.end = end

    def to_tuple(self):
        return self.start, self.end

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, val):
        if not isinstance(val, datetime):
            raise TypeError("start should datetime.datetime object")
        if self._end is not None and val >= self._end:
            raise ValueError("start should be < end")
        self._start = val

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, val):
        if not isinstance(val, datetime):
            raise TypeError("end should datetime.datetime object")
        if self._start is not None and val <= self._start:
            raise ValueError("start should be < end")
        self._end = val

    def __repr__(self):
        return "TimeRange({0}, {1})".format(self._start.__repr__(), self._end.__repr__())


def parse_time(start, end):
    now = datetime.now()
    if isinstance(start, int):
        offset = timedelta(seconds=start)
        start_time = now - offset
    elif start is None:
        start_time = datetime(1970, 1, 1)
    else:
        start_time = parse(start)

    if isinstance(end, int):
        # TODO: add check for future (negative values) and ensure that start < end
        offset = timedelta(seconds=end)
        end_time = now - offset
    else:
        end_time = parse(end)

    return TimeRange(start=start_time, end=end_time)
