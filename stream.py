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
from utils import Hashable, TypedBiDict, check_output_format, check_tool_defined

import logging
from copy import deepcopy
from collections import Iterable, namedtuple
from datetime import datetime


class StreamView(namedtuple("StreamView", "stream time_interval")):
    """
    Simple helper class for storing streams with time intervals (i.e. a "view" on a stream)
    """
    def __new__(cls, stream, time_interval=None):
        if not isinstance(stream, Stream):
            raise ValueError("stream must be Stream object")
        if time_interval is not None and not isinstance(time_interval, TimeInterval):
            raise ValueError("time_interval must be TimeInterval object")
        return super(StreamView, cls).__new__(cls, stream, time_interval)


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


class Stream(Hashable):
    """
    Stream reference class
    """
    _calculated_intervals = None
    defined = False

    def __init__(self, channel, stream_id, calculated_intervals, input_streams):
        """
        :type channel: BaseChannel
        :type stream_id: StreamId
        :type calculated_intervals: TimeIntervals
        :type tool: Stream, None
        """
        self.channel = channel
        if not isinstance(stream_id, StreamId):
            raise TypeError(str(type(stream_id)))
        self.stream_id = stream_id
        # self.get_results_func = get_results_func
        if not isinstance(calculated_intervals, TimeIntervals):
            raise TypeError(str(type(calculated_intervals)))
        self.calculated_intervals = calculated_intervals
        # self.kwargs = {}
        # self.tool = tool
        self.input_streams = input_streams

        # Here we define the output type. When modifiers are applied, this changes
        # self.output_format = 'doc_gen'

    def __str__(self):
        return "{}(stream_id={}, channel_id={}, input_streams=[{}])".format(
            self.__class__.__name__,
            self.stream_id,
            self.channel.channel_id,
            # self.tool.__class__.__name__ if self.tool else None,
            ", ".join(map(lambda x: x.stream_id.name, self.input_streams)) if self.input_streams else None)

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

    # def window(self, time_interval):
    #     """
    #     Sets the time execute for this stream
    #     :param time_interval: either a TimeInterval object or (start, end) tuple of type str or datetime
    #     :type time_interval: Iterable, TimeInterval
    #     :return: self (for chaining)
    #     """
    #     if isinstance(time_interval, TimeInterval):
    #         self.time_interval = time_interval
    #     elif isinstance(time_interval, Iterable):
    #         self.time_interval = parse_time_tuple(*time_interval)
    #         if isinstance(self.time_interval, RelativeTimeInterval):
    #             raise ValueError("Use relative_window to define relative time windows")
    #     elif isinstance(time_interval, RelativeTimeInterval):
    #         raise ValueError("Use relative_window to define relative time windows")
    #     else:
    #         raise ValueError
    #     return self
    #
    # def relative_window(self, time_interval):
    #     """
    #     Sets the time execute for this stream
    #     :param time_interval: either a TimeInterval object or (start, end) tuple of type str or datetime
    #     :type time_interval: Iterable, TimeInterval
    #     :return: self (for chaining)
    #     """
    #     if isinstance(time_interval, RelativeTimeInterval):
    #         self.time_interval = time_interval
    #     elif isinstance(time_interval, Iterable):
    #         self.time_interval = parse_time_tuple(*time_interval)
    #         if not isinstance(self.time_interval, RelativeTimeInterval):
    #             raise ValueError("Use execute to define absolute time windows")
    #     elif isinstance(time_interval, RelativeTimeInterval):
    #         raise ValueError("Use execute to define absolute time windows")
    #     else:
    #         raise ValueError
    #     return self

    # @property
    # def required_intervals(self):
    #     return TimeIntervals([self.time_interval]) - self.calculated_intervals

    # def execute(self):
    #     self.channel.get_results(self)

    def __iter__(self):
        for item in self.channel.get_results(self):
            yield item

    # @check_output_format({'doc_gen'})
    def items(self):
        """
        Return all results as a list
        :return:
        """
        return list(self.iteritems())

    # @check_output_format({'doc_gen'})
    def iteritems(self):
        return iter(self)

    # @check_output_format({'doc_gen'})
    def timestamps(self):
        return list(self.itertimestamps())

    # @check_output_format({'doc_gen'})
    def itertimestamps(self):
        return map(lambda x: x.timestamp, self.iteritems())

    # @check_output_format({'data_gen', 'doc_gen'})
    def values(self):
        # if self.output_format == 'doc_gen':
        return list(self.itervalues())
        # else:
        #     return self.items()

    # @check_output_format({'data_gen', 'doc_gen'})
    def itervalues(self):
        # if self.output_format == 'doc_gen':
        return map(lambda x: x.value, self.iteritems())
        # else:
        #     return self.iteritems()

    # @check_output_format({'doc_gen'})
    def last(self):
        # TODO: This is designed to replace the Last() modifier
        # TODO: Can this be done without list()?
        return self.items()[-1]

    # @check_output_format({'doc_gen'})
    def first(self, default=None, key=None):
        # TODO: This is designed to replace the Last() modifier
        if key is None:
            for el in self.iteritems():
                if el:
                    return el
        else:
            for el in self.iteritems():
                if key(el):
                    return el
        return default

    # @check_output_format({'doc_gen'})
    def head(self, n):
        # TODO: this is designed to replace the Head() modifier
        i = 0
        for d in self.iteritems():
            i += 1
            if i > n:
                break
            yield d

    # @check_output_format({'doc_gen'})
    def tail(self, n):
        # TODO: This is designed to replace the Tail() modifier
        # TODO: Can this be done without list()?
        return list(self.iteritems())[-n:]

    # @check_output_format({'doc_gen'})
    def component(self, key):
        # TODO: is this needed now we have a Component() tool?
        for (time, data) in self.iteritems():
            if key in data:
                yield StreamInstance(time, data[key])

    # @check_output_format({'doc_gen'})
    def component_filter(self, key, values):
        # TODO: is this needed now we have a ComponentFilter() tool?
        for (time, data) in self.iteritems():
            if key in data and data[key] in values:
                yield StreamInstance(time, data)

    # @check_output_format({'doc_gen'})
    def delete_nones(self):
        # TODO: Test this against ComponentFilter(key, values=[None], complement=true)
        for (time, data) in self.iteritems():
            data2 = {}
            for (key, value) in data.items():
                if value is not None:
                    data2[key] = value
            yield StreamInstance(time, data2)


# class ToolStream(Stream):
#     def __init__(self, stream):
#         super(ToolStream, self).__init__(
#             channel=stream.channel,
#             stream_id=stream.stream_id,
#             calculated_intervals=stream.calculated_intervals,
#             # tool=stream.tool,
#             input_streams=stream.input_streams)
#         self.defined = False
#
#     def define(self, input_streams=None):  # , **kwargs):
#         """
#         Define the stream with the given input streams and keyword arguments
#         :param input_streams: The input streams
#         :param kwargs: keyword arguments
#         :return: self (for chaining)
#         """
#         if self.defined:
#             raise RuntimeError("Tool for stream {} already defined".format(self.stream_id))
#
#         # Don't obliterate existing input_streams definition if there was one
#         if input_streams:
#             self.input_streams = input_streams
#
#         # TODO: Possibly combine with existing kwargs ... defaults?
#         # self.kwargs = kwargs
#         self.defined = True
#         return deepcopy(self)

    # @check_tool_defined
    # def execute(self):
    #     self.channel.get_results(self)
    #
    # @check_tool_defined
    # def __iter__(self):
    #     for item in self.channel.get_results(self):
    #         yield item
    #
    # @check_output_format({'doc_gen'})
    # @check_tool_defined
    # def iteritems(self):
    #     return iter(self)
