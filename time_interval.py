from __future__ import print_function

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
from utils import MIN_DATE, MAX_DATE, utcnow, UTC, Printable

from datetime import date, datetime, timedelta
from dateutil.parser import parse
import logging


class TimeIntervals(Printable):  # example object: (t1,t2] U (t3,t4] U ...
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
        return " U ".join(map(str, self.intervals))

    @property
    def is_empty(self):
        return len(self.intervals) == 0
    
    @property
    def is_not_empty(self):
        return len(self.intervals) > 0
    
    def split(self, points):
        if len(points) == 0:
            return
        p = points[-1]
        for i in range(len(self.intervals)):
            if (self.intervals[i].start < p) and (self.intervals[i].end > p):
                self.intervals = self.intervals[:i] \
                                 + [TimeInterval(self.intervals[i].start, p), TimeInterval(p, self.intervals[i].end)] \
                                 + self.intervals[(i + 1):]
        self.split(points[:-1])

    def compress(self):
        if len(self.intervals) == 0:
            return
        v = self.intervals[:1]
        for i in range(1, len(self.intervals)):
            if self.intervals[i].start == v[-1].end:
                v[-1] = TimeInterval(v[-1].start, self.intervals[i].end)
            else:
                v.append(self.intervals[i])
        self.intervals = v

    def __add__(self, other):
        self_points = [point for interval in self.intervals for point in (interval.start, interval.end)]
        other_points = [point for interval in other.intervals for point in (interval.start, interval.end)]
        self.split(other_points)
        other.split(self_points)
        v = list(set(self.intervals).union(set(other.intervals)))
        v.sort(key=lambda ii: ii.start)
        new = TimeIntervals(v)
        self.compress()
        other.compress()
        new.compress()
        return new
    
    def __sub__(self, other):
        self_points = [point for interval in self.intervals for point in (interval.start, interval.end)]
        other_points = [point for interval in other.intervals for point in (interval.start, interval.end)]
        self.split(other_points)
        other.split(self_points)
        v = list(set(self.intervals).difference(set(other.intervals)))
        v.sort(key=lambda ii: ii.start)
        new = TimeIntervals(v)
        self.compress()
        other.compress()
        new.compress()
        return new

    def __eq__(self, other):
        return isinstance(other, TimeIntervals) and all(z[0] == z[1] for z in zip(self.intervals, other.intervals))


class TimeInterval(object):
    _start = None
    _end = None
    
    def __init__(self, start, end):
        self.start = start
        self.end = end
    
    def to_tuple(self):
        return self.start, self.end
    
    def validate_types(self, val):
        if not isinstance(val, (date, datetime)):
            raise TypeError("start should datetime.datetime object")
        
        if self._end is not None and val >= self._end:
            raise ValueError("start should be < end")
    
    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, val):
        self.validate_types(val)
        self._start = val
    
    @property
    def end(self):
        return self._end
    
    @end.setter
    def end(self, val):
        self.validate_types(val)
        self._end = val
    
    def __str__(self):
        return "({0}, {1}]".format(self.start, self.end)

    def __repr__(self):
        return "{}(start={}, end={})".format(self.__class__.__name__, repr(self.start), repr(self.end))

    def __eq__(self, other):
        return isinstance(other, TimeInterval) and self.start == other.start and self.end == other.end

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash((self.start, self.end))

    def __contains__(self, item):
        return self.start < item <= self.end


class RelativeTimeInterval(TimeInterval):
    def __init__(self, start, end):
        super(RelativeTimeInterval, self).__init__(start, end)
        
    def validate_types(self, val):
        if not isinstance(val, timedelta):
            raise TypeError("start should datetime.timedelta object")
        
        if self._end is not None and val >= self._end:
            raise ValueError("start should be < end")


def parse_time_tuple(start, end):
    """
    Parse a time tuple. These can be:
      relative in seconds,       e.g. (-4, 0)
      relative in timedelta,     e.g. (timdelta(seconds=-4), timedelta(0))
      absolute in date/datetime, e.g. (datetime(2016, 4, 28, 20, 0, 0, 0, UTC), datetime(2016, 4, 28, 21, 0, 0, 0, UTC))
      absolute in iso strings,   e.g. ("2016-04-28T20:00:00.000Z", "2016-04-28T20:01:00.000Z")
    Mixtures of relative and absolute are not allowed
    :param start: Start time
    :param end: End time
    :return: TimeInterval or RelativeTimeInterval object
    """
    if isinstance(start, int):
        start_time = timedelta(seconds=start)
    elif isinstance(start, timedelta):
        start_time = start
    elif start is None:
        start_time = MIN_DATE
    elif isinstance(start, (date, datetime)):
        start_time = start.replace(tzinfo=UTC)
    else:
        start_time = parse(start).replace(tzinfo=UTC)
    
    if isinstance(end, int):
        # TODO: add check for future (negative values) and ensure that start < end
        if not isinstance(start_time, timedelta):
            raise ValueError("Can't mix relative and absolute times")
        end_time = timedelta(seconds=end)
    elif isinstance(end, timedelta):
        if not isinstance(start_time, timedelta):
            raise ValueError("Can't mix relative and absolute times")
        end_time = end
    elif end is None:
        end_time = utcnow()  # TODO: or MAX_DATE?
    elif isinstance(end, datetime):
        end_time = end.replace(tzinfo=UTC)
    else:
        end_time = parse(end).replace(tzinfo=UTC)

    if isinstance(start_time, timedelta):
        return RelativeTimeInterval(start=start_time, end=end_time)
    else:
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

