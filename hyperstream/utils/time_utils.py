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

import pytz
from datetime import datetime, timedelta


class UTC(pytz.UTC):
    def __repr__(self):
        return "UTC"


MIN_DATE = datetime.min.replace(tzinfo=UTC)
MAX_DATE = datetime.max.replace(tzinfo=UTC)


def all_time():
    """
    All time constant. Defined as a function to avoid circular references
    :return:
    """
    from ..time_interval import TimeInterval
    return TimeInterval(MIN_DATE, MAX_DATE)


# ALL_TIME = all_time()


def utcnow():
    return datetime.utcnow().replace(tzinfo=UTC)


def get_timedelta(value):
    if isinstance(value, (int, float)):
        return timedelta(seconds=value)
    elif isinstance(value, timedelta):
        return value
    else:
        raise ValueError("Expected int, float, or timedelta, got {}".format(type(value)))


def unix2datetime(u):
    return datetime.fromtimestamp(u/1000.0, tz=UTC) + timedelta(hours=0)


def construct_experiment_id(instance):
    """
    Construct an experiment id from a time interval
    :param instance:
    :return: The experiment id
    :type instance: StreamInstance
    :rtype: str
    """
    # Construct id based on unix epoch timestamps
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=UTC)
    start = int((instance.timestamp.start - epoch).total_seconds() * 1000.0)
    end = int((instance.timestamp.end - epoch).total_seconds() * 1000.0)
    return "%d-%d".format(start, end)
