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

from time_interval import TimeIntervals, parse_time_tuple, RelativeTimeInterval, TimeInterval
from utils import Hashable, TypedBiDict, Printable, check_output_format, check_tool_defined

import logging
# from copy import deepcopy
from collections import Iterable, namedtuple, deque
from datetime import datetime


class StreamView(Printable):
    """
    Simple helper class for storing streams with a time interval (i.e. a "view" on a stream)
    """
    def __init__(self, stream, time_interval):
        if not isinstance(stream, Stream):
            raise ValueError("stream must be Stream object")
        if not isinstance(time_interval, TimeInterval):
            raise ValueError("relative_time_interval must be TimeInterval object")
        self.stream = stream
        self.time_interval = time_interval

    def __iter__(self):
        for item in self.stream.channel.get_results(self.stream):
            yield item

    def items(self):
        """
        Return all results as a list
        :return:
        """
        return list(self.iteritems())

    def iteritems(self):
        return iter(self)

    def timestamps(self):
        return list(self.itertimestamps())

    def itertimestamps(self):
        return map(lambda x: x.timestamp, self.iteritems())

    def values(self):
        return list(self.itervalues())

    def itervalues(self):
        return map(lambda x: x.value, self.iteritems())

    def last(self, default=None):
        item = default
        for item in self:
            pass
        return item

    def first(self, default=None):
        for el in self:
            return el
        return default

    def head(self, n):
        i = 0
        for d in self.iteritems():
            i += 1
            if i > n:
                break
            yield d

    def tail(self, n):
        return iter(deque(self, maxlen=n))

    def component(self, key):
        # TODO: is this needed now we have a Component() tool?
        for (time, data) in self.iteritems():
            if key in data:
                yield StreamInstance(time, data[key])

    def component_filter(self, key, values):
        # TODO: is this needed now we have a ComponentFilter() tool?
        for (time, data) in self.iteritems():
            if key in data and data[key] in values:
                yield StreamInstance(time, data)

    def delete_nones(self):
        # TODO: Test this against ComponentFilter(key, values=[None], complement=true)
        for (time, data) in self.iteritems():
            data2 = {}
            for (key, value) in data.items():
                if value is not None:
                    data2[key] = value
            yield StreamInstance(time, data2)


class RelativeStreamView(Printable):
    def __init__(self, stream, relative_time_interval):
        if not isinstance(stream, Stream):
            raise ValueError("stream must be Stream object")
        if not isinstance(relative_time_interval, RelativeTimeInterval):
            raise ValueError("relative_time_interval must be RelativeTimeInterval object")
        self.stream = stream
        self.relative_time_interval = relative_time_interval

    def window(self, time_interval):
        return self.stream.window(time_interval + self.relative_time_interval)


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
        if meta_data:
            if isinstance(meta_data, dict):
                self.meta_data = tuple(sorted(meta_data.items()))
            elif isinstance(meta_data, tuple):
                self.meta_data = meta_data
            else:
                raise ValueError("Expected dict or tuple, got {}".format(type(meta_data)))
        else:
            self.meta_data = tuple()
        # self.meta_data = meta_data if meta_data else {}
    
    def __str__(self):
        if self.meta_data:
            # return self.name + ": [" + ", ".join("{}={}".format(k, v) for k, v in self.meta_data.items()) + "]"
            return self.name + ": [" + ", ".join("{}={}".format(k, v) for k, v in self.meta_data) + "]"
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

    def as_dict(self):
        return dict(name=self.name, meta_data=dict(self.meta_data))

    def as_raw(self):
        """
        Return a representation of this object that can be used with mongoengine Document.objects(__raw__=x)
        Example:

        >>> stream_id = StreamId(name='test', meta_data=((u'house', u'1'), (u'resident', u'1')))
        >>> stream_id.as_raw()
        {u'stream_id.meta_data.house': u'1', u'stream_id.meta_data.resident': u'1', 'stream_id.name': 'test'}

        :return: The raw representation of this object.
        """
        d = dict(('stream_id.meta_data.' + k, v) for k, v in self.meta_data)
        d['stream_id.name'] = self.name
        return d


class Stream(Hashable):
    """
    Stream reference class
    """
    _calculated_intervals = None
    defined = False

    def __init__(self, channel, stream_id, calculated_intervals=None):
        """
        :param channel: The channel to which this stream belongs
        :param stream_id: The unique identifier for this string
        :param calculated_intervals: The time intervals in which this has been calculated
        :type channel: BaseChannel
        :type stream_id: StreamId
        :type calculated_intervals: TimeIntervals
        """
        self.channel = channel
        if not isinstance(stream_id, StreamId):
            raise TypeError(str(type(stream_id)))
        self.stream_id = stream_id
        # self.get_results_func = get_results_func
        if calculated_intervals:
            if not isinstance(calculated_intervals, TimeIntervals):
                raise TypeError(str(type(calculated_intervals)))
            self.calculated_intervals = calculated_intervals
        else:
            self.calculated_intervals = TimeIntervals()
        self.tool_reference = None # needed to traverse the graph outside of workflows

        # Here we define the output type. When modifiers are applied, this changes
        # self.output_format = 'doc_gen'
        
    def set_tool_reference(self, tool_reference): # needed to traverse the graph outside of workflows
        self.tool_reference = tool_reference
    
    def __str__(self):
        return "{}(stream_id={}, channel_id={})".format(
            self.__class__.__name__,
            self.stream_id,
            self.channel.channel_id)

    def __repr__(self):
        return str(self)
    
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
        return self.channel.get_stream_writer(self)

    def window(self, time_interval):
        """
        Gets a view on this stream for the time interval given
        :param time_interval: either a TimeInterval object or (start, end) tuple of type str or datetime
        :type time_interval: Iterable, TimeInterval
        :return: a stream view object
        """
        if isinstance(time_interval, TimeInterval):
            pass
        elif isinstance(time_interval, Iterable):
            time_interval = parse_time_tuple(*time_interval)
            if isinstance(time_interval, RelativeTimeInterval):
                raise NotImplementedError
        elif isinstance(time_interval, RelativeTimeInterval):
            raise NotImplementedError
        else:
            raise TypeError("Expected TimeInterval or (start, end) tuple of type str or datetime, got {}"
                            .format(type(time_interval)))
        return StreamView(stream=self, time_interval=time_interval)

    def relative_window(self, relative_time_interval):
        """
        Sets the time execute for this stream
        :param relative_time_interval: either a RelativeTimeInterval object or (start, end) tuple of str or timedelta
        :type relative_time_interval: Iterable, RelativeTimeInterval
        :return: a relative stream view object
        """
        if isinstance(relative_time_interval, RelativeTimeInterval):
            pass
        elif isinstance(relative_time_interval, Iterable):
            relative_time_interval = parse_time_tuple(*relative_time_interval)
            if not isinstance(relative_time_interval, RelativeTimeInterval):
                raise NotImplementedError
        elif isinstance(relative_time_interval, TimeInterval):
            raise NotImplementedError
        else:
            raise TypeError("Expected RelativeTimeInterval or (start, end) tuple of type str or timedelta, got {}"
                            .format(type(relative_time_interval)))
        return RelativeStreamView(stream=self, relative_time_interval=relative_time_interval)

    # def window(self, relative_time_interval):
    #     """
    #     Sets the time execute for this stream
    #     :param relative_time_interval: either a TimeInterval object or (start, end) tuple of type str or datetime
    #     :type relative_time_interval: Iterable, TimeInterval
    #     :return: self (for chaining)
    #     """
    #     if isinstance(relative_time_interval, TimeInterval):
    #         self.relative_time_interval = relative_time_interval
    #     elif isinstance(relative_time_interval, Iterable):
    #         self.relative_time_interval = parse_time_tuple(*relative_time_interval)
    #         if isinstance(self.relative_time_interval, RelativeTimeInterval):
    #             raise ValueError("Use relative_window to define relative time windows")
    #     elif isinstance(relative_time_interval, RelativeTimeInterval):
    #         raise ValueError("Use relative_window to define relative time windows")
    #     else:
    #         raise ValueError
    #     return self
    #
    # def relative_window(self, relative_time_interval):
    #     """
    #     Sets the time execute for this stream
    #     :param relative_time_interval: either a TimeInterval object or (start, end) tuple of type str or datetime
    #     :type relative_time_interval: Iterable, TimeInterval
    #     :return: self (for chaining)
    #     """
    #     if isinstance(relative_time_interval, RelativeTimeInterval):
    #         self.relative_time_interval = relative_time_interval
    #     elif isinstance(relative_time_interval, Iterable):
    #         self.relative_time_interval = parse_time_tuple(*relative_time_interval)
    #         if not isinstance(self.relative_time_interval, RelativeTimeInterval):
    #             raise ValueError("Use execute to define absolute time windows")
    #     elif isinstance(relative_time_interval, RelativeTimeInterval):
    #         raise ValueError("Use execute to define absolute time windows")
    #     else:
    #         raise ValueError
    #     return self

    # @property
    # def required_intervals(self):
    #     return TimeIntervals([self.relative_time_interval]) - self.calculated_intervals

    # def execute(self):
    #     self.channel.get_results(self)
