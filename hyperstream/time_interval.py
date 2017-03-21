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
"""
Module for dealing with time intervals containing TimeInterval, TimeIntervals, and RelativeTimeInterval
"""

from utils import MIN_DATE, MAX_DATE, utcnow, UTC, Printable, get_timedelta

from datetime import date, datetime, timedelta
import ciso8601
from collections import namedtuple
import arrow


def profile(ob):
    """
    Comment out this function to be able to use the line_profiler module. e.g. call:
    kernprof -l scripts/deploy_summariser.py --loglevel=10
    python -m line_profiler deploy_summariser.py.lprof > deploy_summariser.py.summary
    :param ob: object
    :return: object
    """
    return ob


class TimeIntervals(Printable):
    """
    Container class for time intervals, that manages splitting and joining
    Example object: (t1,t2] U (t3,t4] U ...
    """
    @profile
    def __init__(self, intervals=None):
        """
        Initialise the object with the given intervals.
        These should be in a format that can be parsed by parse_time_tuple

        :param intervals: The time intervals
        """
        self.intervals = self.parse(intervals)

    # @profile
    def __str__(self):
        return " U ".join(map(str, self.intervals)) if self.intervals else "[]"

    # @profile
    def __repr__(self):
        return "{}([{}])".format(self.__class__.__name__, ", ".join(map(repr, self.intervals)))

    @profile
    def parse(self, intervals):
        parsed = []
        if intervals:
            for v in intervals:
                if isinstance(v, (tuple, list)):
                    if len(v) != 2:
                        raise TypeError()
                    v = parse_time_tuple(*v)
                elif isinstance(v, TimeInterval):
                    v = TimeInterval(v.start, v.end)
                else:
                    raise TypeError("Expected tuple/list/TimeInterval ({} given)".format(type(v)))
                # try:
                #     v = parse_time_tuple(*v)
                # except Exception as e:
                #     import logging
                #     logging.debug(e)
                #     if isinstance(v, TimeInterval):
                #         v = TimeInterval(v.start, v.end)
                #     else:
                #         raise TypeError("Expected tuple/list/TimeInterval ({} given)".format(type(v)))
                parsed.append(v)
        return parsed

    @property
    # @profile
    def is_empty(self):
        return len(self.intervals) == 0

    @property
    # @profile
    def start(self):
        return min(self.intervals, key=lambda x: x.start).start if self.intervals else None

    @property
    # @profile
    def end(self):
        return max(self.intervals, key=lambda x: x.end).end if self.intervals else None

    @property
    # @profile
    def span(self):
        return TimeInterval(self.start, self.end) if self.intervals else None

    @property
    # @profile
    def humanized(self):
        return " U ".join(map(lambda x: x.humanized, self.intervals)) if self.intervals else "Empty"

    @profile
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

    # @profile
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

    @profile
    def __add__(self, other):
        if isinstance(other, TimeInterval):
            if self.is_empty:
                return TimeIntervals([other])
            if other.start > self.end:
                self.intervals.append(other)
            elif other.end < self.start:
                self.intervals.insert(0, other)
            elif other.start == self.end:
                self.intervals[-1].end = other.end
            elif other.end == self.start:
                self.intervals[0].start = other.start
            else:
                return self + TimeIntervals([other])
            return self

        if self.is_empty:
            return TimeIntervals(other.intervals)

        if other.is_empty:
            return TimeIntervals(self.intervals)

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

    @profile
    def __sub__(self, other):
        if self == other:
            return TimeIntervals([])

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

    # @profile
    def __eq__(self, other):
        return isinstance(other, TimeIntervals) and len(self.intervals)==len(other.intervals) and all(z[0] == z[1] for z in zip(self.intervals, other.intervals))

    # @profile
    def __ne__(self, other):
        return not self == other

    # @profile
    def __iter__(self):
        return iter(sorted(self.intervals))

    # @profile
    def __getitem__(self, key):
        if isinstance(key, slice):
            items = self.intervals[key]
            if isinstance(items, TimeInterval):
                return items
            return TimeIntervals(items)
        return self.intervals[key]

    # @profile
    def __bool__(self):
        return self.intervals is not None and len(self.intervals) > 0

    __nonzero__ = __bool__

    # @profile
    def __len__(self):
        return len(self.intervals)


class TimeInterval(namedtuple("TimeInterval", "start end")):
    """
    Time interval object.
    Thin wrapper around a (start, end) tuple of datetime objects that provides some validation
    """
    @classmethod
    # @profile
    def all_time(cls):
        return TimeInterval(MIN_DATE, MAX_DATE)

    @classmethod
    # @profile
    def up_to_now(cls):
        return TimeInterval(MIN_DATE, utcnow())

    @classmethod
    # @profile
    def now_minus(cls, weeks=0, days=0, hours=0, minutes=0, seconds=0, milliseconds=0):
        delta = timedelta(weeks=weeks, days=days, hours=hours,
                          minutes=minutes, seconds=seconds, milliseconds=milliseconds, microseconds=0)
        now = utcnow()
        return TimeInterval(now - delta, now)

    # @profile
    def __new__(cls, start, end):
        """
        Initialise the object with the start and end times

        :param start: The start time
        :param end: The end time
        """
        return super(TimeInterval, cls).__new__(cls, start, end)

    @profile
    def __init__(self, start, end):
        self._start = start
        self._end = end
        self._validate()
        super(TimeInterval, self).__init__()

    # @profile
    def to_tuple(self):
        return self.start, self.end

    @profile
    def _validate(self):
        if self._start >= self._end:
            raise ValueError("start should be strictly less than end")

        # TODO: Temporarily remove extra validation
        #
        # if not isinstance(self._start, (date, datetime)):
        #     raise TypeError("start should be datetime.datetime object")
        #
        # if not isinstance(self._end, (date, datetime)):
        #     raise TypeError("end should be datetime.datetime object")

    @property
    # @profile
    def width(self):
        return self._end - self._start

    @property
    # @profile
    def start(self):
        return self._start

    @start.setter
    # @profile
    def start(self, value):
        self._start = value
        self._validate()

    @property
    # @profile
    def end(self):
        return self._end

    @end.setter
    # @profile
    def end(self, value):
        self._end = value
        self._validate()

    @property
    # @profile
    def humanized(self):
        return "({0} to {1}]".format(arrow.get(self.start).humanize(), arrow.get(self.end).humanize())

    # @profile
    def __str__(self):
        return "({0}, {1}]".format(self.start, self.end)

    # @profile
    def __repr__(self):
        return "{}(start={}, end={})".format(self.__class__.__name__, repr(self.start), repr(self.end))

    # @profile
    def __eq__(self, other):
        return isinstance(other, TimeInterval) and self.start == other.start and self.end == other.end

    # @profile
    def __ne__(self, other):
        return not self == other

    # @profile
    def __hash__(self):
        return hash((self.start, self.end))

    # @profile
    def __contains__(self, item):
        if isinstance(item, (date, datetime)):
            return self.start < item <= self.end
        if isinstance(item, TimeInterval):
            return self.start < item.start and item.end <= self.end
        raise TypeError("can't compare datetime.datetime to {}".format(type(item)))

    # @profile
    def __add__(self, other):
        if isinstance(other, timedelta):
            return TimeInterval(self.start + other, self.end + other)
        if isinstance(other, (tuple, list)) and len(other) == 2:
            other = RelativeTimeInterval(*other)
        if not isinstance(other, RelativeTimeInterval):
            raise ValueError("Can only add a relative time interval to a time interval")
        return TimeInterval(self.start + timedelta(other.start), self.end + timedelta(other.end))

    # def resize(self, *args):
    #     if len(args) == 1:
    #         if isinstance(args[0], RelativeTimeInterval):
    #             rti = args[0]
    #         else:
    #             raise TypeError("Single argument should be RelativeTimeInterval")
    #     elif len(args) == 2:
    #         rti = RelativeTimeInterval(*args)
    #     else:
    #         raise ValueError("Too many input arguments")
    #     return self + rti


# noinspection PyMissingConstructor
class RelativeTimeInterval(TimeInterval):
    """
    Relative time interval object.
    Thin wrapper around a (start, end) tuple of timedelta objects that provides some validation
    """
    def __init__(self, start, end):
        """
        Initialise the object with the start and end times

        :param start: The start time
        :param end: The end time
        """
        self._start = get_timedelta(start)
        self._end = get_timedelta(end)
        self._validate()

    def _validate(self):
        if not isinstance(self._start, timedelta):
            raise TypeError("start should datetime.timedelta object")

        if not isinstance(self._end, timedelta):
            raise TypeError("end should datetime.timedelta object")

        if self._start >= self._end:
            raise ValueError("start should be strictly less than  end")

        if self._end > timedelta(0):
            raise ValueError("relative time intervals in the future are not supported")

    @property
    def start(self):
        return self._start.total_seconds()

    @start.setter
    def start(self, value):
        self._start = get_timedelta(value)
        self._validate()

    @property
    def end(self):
        return self._end.total_seconds()

    @end.setter
    def end(self, value):
        self._end = get_timedelta(value)
        self._validate()

    def absolute(self, dt):
        if not isinstance(dt, (date, datetime)):
            raise ValueError("Expected date|datetime, got {}".format(type(dt)))
        return TimeInterval(start=dt + self._start, end=dt + self._end)


@profile
def parse_time_tuple(start, end):
    """
    Parse a time tuple. These can be:
      relative in seconds,       e.g. (-4, 0)
      relative in timedelta,     e.g. (timedelta(seconds=-4), timedelta(0))
      absolute in date/datetime, e.g. (datetime(2016, 4, 28, 20, 0, 0, 0, UTC), datetime(2016, 4, 28, 21, 0, 0, 0, UTC))
      absolute in iso strings,   e.g. ("2016-04-28T20:00:00.000Z", "2016-04-28T20:01:00.000Z")
      Mixtures of relative and absolute are not allowed

    :param start: Start time
    :param end: End time
    :type start: int | timedelta | datetime | str
    :type end: int | timedelta | datetime | str
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
        start_time = ciso8601.parse_datetime(start).replace(tzinfo=UTC)
    
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
        end_time = ciso8601.parse_datetime(end).replace(tzinfo=UTC)

    if isinstance(start_time, timedelta):
        return RelativeTimeInterval(start=start_time, end=end_time)
    else:
        return TimeInterval(start=start_time, end=end_time)
