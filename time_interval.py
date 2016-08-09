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
from datetime import datetime, timedelta
from dateutil.parser import parse
import logging


class TimeIntervals:  # example object: (t1,t2]U(t3,t4]U...
    def __init__(self, intervals=None):
        self.intervals = []
        if intervals:
            for v in intervals:
                if isinstance(v, (tuple, list)):
                    if len(v) != 2:
                        raise TypeError()
                    v = parse_time_tuple(*v)
                elif not isinstance(v, TimeInterval):
                    raise TypeError()
                self.intervals.append(v)

    def __str__(self):
        return "U".join(["(" + str(a) + "," + str(b) + "]" for (a, b) in self.intervals])

    def __repr__(self):
        return str(self)

    def split(self, points):
        if len(points) == 0:
            return
        p = points[-1]
        for i in range(len(self.intervals)):
            if (self.intervals[i].start < p) and (self.intervals[i].end > p):
                self.intervals = self.intervals[:i] \
                                 + [(self.intervals[i].start, p), (p, self.intervals[i].end)] \
                                 + self.intervals[(i + 1):]
        self.split(points[:-1])

    def compress(self):
        if len(self.intervals) == 0:
            return
        v = self.intervals[:1]
        for i in range(1, len(self.intervals)):
            if self.intervals[i][0] == v[-1][1]:
                v[-1] = (v[-1][0], self.intervals[i].end)
            else:
                v.append(self.intervals[i])
        self.intervals = v

    def __add__(self, other):
        self_points = [point for interval in self.intervals for point in interval]
        other_points = [point for interval in other.value for point in interval]
        self.split(other_points)
        other.split(self_points)
        v = list(set(self.intervals).union(set(other.value)))
        v.sort()
        new = TimeIntervals(v)
        self.compress()
        other.compress()
        new.compress()
        return new

    def __sub__(self, other):
        self_points = [point for interval in self.intervals for point in interval]
        other_points = [point for interval in other.value for point in interval]
        self.split(other_points)
        other.split(self_points)
        v = list(set(self.intervals).difference(set(other.value)))
        v.sort()
        new = TimeIntervals(v)
        self.compress()
        other.compress()
        new.compress()
        return new


class TimeInterval(object):
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
        return "{0}({1}, {2})".format(self.__class__.__name__, self._start.__repr__(), self._end.__repr__())

    def __str__(self):
        return "({0}, {1})".format(self.start, self.end)


def parse_time_tuple(start, end):
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

    return TimeInterval(start=start_time, end=end_time)


def compare_time_ranges(desired_ranges, computed_ranges):
    # Check here whether we need to recompute anything
    result_ranges = []
    for desired in desired_ranges:
        needs_full_computation = True
        for computed in computed_ranges:
            if desired.start < computed.start:
                if desired.end < computed.start:
                    # Completely outside range
                    continue
                else:
                    result_ranges.append(TimeInterval(start=desired.start, end=computed.start))
                    logging.debug("Time range partially computed {0}, new end time={1}".format(desired, computed.start))
            else:
                if desired.start > computed.end:
                    # Completely outside range
                    continue
                if desired.end <= computed.end:
                    needs_full_computation = False
                    logging.debug("Time range fully computed {0}".format(desired))
                else:
                    result_ranges.append(TimeInterval(start=computed.end, end=desired.end))
                    logging.debug("Time range partially computed {0}, new start time={1}".format(desired, computed.end))
        if needs_full_computation:
            logging.debug("Time range requires full computation {0}".format(desired))
            result_ranges.append(desired)
    return result_ranges
