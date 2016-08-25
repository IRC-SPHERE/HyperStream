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
from time_interval import TimeIntervals, parse_time_tuple, RelativeTimeInterval, TimeInterval
from utils import Printable, Hashable, TypedBiDict
from modifiers import Modifier, Identity
from errors import StreamDataNotAvailableError

import logging
from collections import Iterable, namedtuple
from datetime import timedelta, datetime


class StreamInstance(namedtuple("StreamInstance", "timestamp value")):
    """
    Simple helper class for storing data instances that's a bit neater than simple tuples
    """
    def __new__(cls, timestamp, value):
        if not isinstance(timestamp, datetime):
            raise ValueError("Timestamp must be datetime.datetime")
        return super(StreamInstance, cls).__new__(cls, timestamp, value)


class StreamDict(TypedBiDict):
    """
    Custom bi-directional dictionary where keys are StreamID objects and values are Stream objects.
    Raises ValueDuplicationError if the same Stream is added again
    """
    def __init__(self, *args, **kwargs):
        super(StreamDict, self).__init__(StreamId, Stream, *args, **kwargs)


class StreamId(Hashable):
    """
    Helper class for stream identifiers. A stream identifier contains the stream name and any meta-data
    """
    def __init__(self, name, meta_data=None):
        self.name = name
        self.meta_data = meta_data if meta_data else {}

    def __str__(self):
        if self.meta_data:
            return self.name + ": [" + ", ".join("{}={}".format(k, v) for k, v in self.meta_data.items()) + "]"
        else:
            return self.name

    def __repr__(self):
        return "{}(name={}, meta_data={})".format(
            self.__class__.__name__,
            repr(self.name),
            repr(self.meta_data)
        )

    def __eq__(self, other):
        return isinstance(other, StreamId) and \
               self.name == other.name and \
               sorted(self.meta_data) == sorted(other.meta_data)


class Stream(Printable, Hashable):
    """
    Stream reference class
    """
    _calculated_intervals = None

    def __init__(self, channel, stream_id, time_interval, calculated_intervals, modifiers, tool, input_streams):
        """
        :type channel: BaseChannel
        :type stream_id: StreamId
        :type time_interval: TimeInterval
        :type calculated_intervals: TimeIntervals
        :type modifiers: Modifier
        :type get_results_func: func
        :type tool: Stream
        """
        self.channel = channel
        if not isinstance(stream_id, StreamId):
            raise TypeError(str(type(stream_id)))
        self.stream_id = stream_id
        self.time_interval = time_interval
        if modifiers is None:
            raise ValueError(modifiers)
        self.modifiers = modifiers
        # self.get_results_func = get_results_func
        if not isinstance(calculated_intervals, TimeIntervals):
            raise TypeError(str(type(calculated_intervals)))
        self.calculated_intervals = calculated_intervals
        self.kwargs = {}
        self.tool = tool
        self.input_streams = input_streams

    def __str__(self):
        return "{}(stream_id={}, channel_id={}, interval={}, modifiers={})".format(
            self.__class__.__name__, self.stream_id, self.channel.channel_id, self.time_interval, self.modifiers)
        # s = "Stream"
        # s += "\n      CHANNEL_ID  : " + repr(self.channel.channel_id)
        # s += "\n      STREAM_ID   : " + repr(self.stream_id)
        # s += "\n      START       : " + repr(self.time_interval.start if self.time_interval else None)
        # s += "\n      END         : " + repr(self.time_interval.end if self.time_interval else None)
        # s += "\n      MODIFIERS   : " + repr(self.modifiers)
        # s += "\n    "
        # return s

    def __eq__(self, other):
        return str(self) == str(other)

    @property
    def calculated_intervals(self):
        # TODO: this should be read from the stream_status collection if it's in the database channel
        return self._calculated_intervals

    @calculated_intervals.setter
    def calculated_intervals(self, value):
        # TODO: this should be written to the stream_status collection if it's in the database channel
        self._calculated_intervals = value

    @property
    def writer(self):
        return self.channel.get_stream_writer(self.stream_id)

    def define(self, input_streams=None, **kwargs):
        """
        Define the stream with the given input streams and keyword arguments
        :param input_streams: The input streams
        :param kwargs: keyword arguments
        :return: self (for chaining)
        """
        # Don't obliterate existing input_streams definition if there was one
        if input_streams:
            self.input_streams = input_streams

        # TODO: Possibly combine with existing kwargs ... defaults?
        self.kwargs = kwargs
        return self

    def window(self, time_interval):
        """
        Sets the time window for this stream reference
        :param time_interval: either a TimeInterval object or (start, end) tuple of type str or datetime
        :return: self (for chaining)
        """
        if isinstance(time_interval, TimeInterval):
            self.time_interval = time_interval
        elif isinstance(time_interval, Iterable):
            # TODO: What about relative times? parse_time_tuple partially solves this?
            self.time_interval = parse_time_tuple(*time_interval)
        elif isinstance(self.time_interval, RelativeTimeInterval):
            # TODO: Re-implement relative time windows - the stream needs to be passed the time window from the calling
            # stream in order to compute the relative offset.
            raise NotImplementedError
        else:
            raise ValueError
        return self

    def modify(self, modifiers):
        """
        Defines the modifiers for this stream reference
        :param modifiers: The modifiers
        :return: self (for chaining)
        """
        if not isinstance(modifiers, Modifier):
            raise TypeError("Expected Modifier, got {}".format(type(modifiers)))
        self.modifiers = modifiers
        return self

    @property
    def absolute_interval(self):
        start = self.time_interval.start if self.time_interval else timedelta(0)
        abs_start = start
        if isinstance(start, timedelta):
            if isinstance(self.time_interval, RelativeTimeInterval):
                raise StreamDataNotAvailableError('The stream reference to a stream has a relative start time, '
                                                  'need an absolute start time')
            abs_start = self.time_interval.start + start

        end = self.time_interval.end if self.time_interval else timedelta(0)
        abs_end = end
        if isinstance(end, timedelta):
            if isinstance(self.time_interval, RelativeTimeInterval):
                raise StreamDataNotAvailableError('The stream reference to a stream has a relative end time, '
                                                  'need an absolute end time')
            abs_end = self.time_interval.end + end

        if abs_end > self.channel.up_to_timestamp:
            raise StreamDataNotAvailableError(
                'The stream is not available after ' + str(self.channel.up_to_timestamp) + ' and cannot be obtained' +
                ' (' + str(abs_end) + ' is provided)')
        return TimeInterval(start=abs_start, end=abs_end)

    @property
    def required_intervals(self):
        return TimeIntervals([self.absolute_interval]) - self.calculated_intervals

    def items(self):
        # Get interval and writer
        # return self.tool.items(interval, writer)
        logging.debug("---Getting items for {} channel, stream {}, with kwargs {}".format(
            self.channel.channel_id, self.stream_id.name, self.kwargs))
        return self.channel.get_results(self)

    def timestamps(self):
        if self.modifiers and not isinstance(self.modifiers, Identity):
            # TODO: This is designed to replace the Time() modifier
            raise NotImplementedError
        return map(lambda x: x[0], self.items())

    def values(self):
        if self.modifiers:
            # TODO: This is designed to replace the Data() modifier
            raise NotImplementedError
        return map(lambda x: x[1], self.items())

    def last(self):
        # TODO: This is designed to replace the Last() modifier
        # TODO: Can this be done without list()?
        return list(self.items()[-1])

    def first(self, default=None, key=None):
        # TODO: This is designed to replace the Last() modifier
        if key is None:
            for el in self.items():
                if el:
                    return el
        else:
            for el in self.items():
                if key(el):
                    return el
        return default

    def head(self, n):
        # TODO: this is designed to replace the Head() modifier
        i = 0
        for d in self.items():
            i += 1
            if i > n:
                break
            yield d

    def tail(self, n):
        # TODO: This is designed to replace the Tail() modifier
        # TODO: Can this be done without list()?
        return list(self.items())[-n:]

