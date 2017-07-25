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
import json
import pytz
from datetime import datetime, date, timedelta
import udatetime


# noinspection PyClassHasNoInit
class UTC(pytz.UTC):
    def __repr__(self):
        return "UTC"


MIN_DATE = datetime.min.replace(tzinfo=UTC)
MAX_DATE = datetime.max.replace(tzinfo=UTC).replace(microsecond=0)


def utcnow():
    """
    Gets the current datetime in UTC format with millisecond precision
    :return:
    """
    now = udatetime.utcnow().replace(tzinfo=UTC)
    milliseconds = int(now.microsecond / 1000 * 1000)
    return datetime(now.year, now.month, now.day, now.hour, now.minute, now.second, milliseconds, UTC)


def get_timedelta(value):
    if isinstance(value, (int, float)):
        return timedelta(seconds=value)
    elif isinstance(value, timedelta):
        return value
    else:
        raise ValueError("Expected int, float, or timedelta, got {}".format(type(value)))


def unix2datetime(u):
    return udatetime.fromtimestamp(u / 1000.0, tz=UTC) + timedelta(hours=0)


def datetime2unix(dt):
    return (dt - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()


def duration2str(x):
    minutes, seconds = divmod(x.total_seconds(), 60)
    return '{} min {} sec'.format(int(minutes), int(seconds))


def construct_experiment_id(time_interval):
    """
    Construct an experiment id from a time interval
    :return: The experiment id
    :type time_interval: TimeInterval
    :rtype: str
    """
    # Construct id based on unix epoch timestamps
    epoch = udatetime.utcfromtimestamp(0).replace(tzinfo=UTC)
    start = int((time_interval.start - epoch).total_seconds() * 1000.0)
    end = int((time_interval.end - epoch).total_seconds() * 1000.0)
    return "{}-{}".format(start, end)


def reconstruct_interval(experiment_id):
    """
    Reverse the construct_experiment_id operation
    :param experiment_id: The experiment id
    :return: time interval
    """
    start, end = map(lambda x: udatetime.utcfromtimestamp(x / 1000.0), map(float, experiment_id.split("-")))
    from ..time_interval import TimeInterval
    return TimeInterval(start, end)


def is_naive(dt):
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()
        return serial

    from ..time_interval import TimeInterval, TimeIntervals
    if isinstance(obj, (TimeInterval, TimeIntervals)):
        return obj.to_json()

    from ..stream import StreamId
    if isinstance(obj, StreamId):
        return obj.to_json()

    from ..channels import BaseChannel
    if isinstance(obj, BaseChannel):
        return json.dumps({'channel_id': obj.channel_id})

    raise TypeError("Type %s not serializable" % type(obj))
