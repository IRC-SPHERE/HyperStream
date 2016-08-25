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
import logging
from collections import Iterable


class StreamDict(TypedBiDict):
    """
    Custom bi-directional dictionary where keys are StreamID objects and values are StreamReference objects.
    Raises ValueDuplicationError if the same StreamReference is added again
    """
    def __init__(self, *args, **kwargs):
        super(StreamDict, self).__init__(StreamId, StreamReference, *args, **kwargs)


class StreamId(Hashable):
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

    # def __hash__(self):
    #     return hash((self.name, repr(sorted(self.meta_data.items()))))

    def __eq__(self, other):
        return isinstance(other, StreamId) and \
               self.name == other.name and \
               sorted(self.meta_data) == sorted(other.meta_data)


class StreamReference(Printable, Hashable):
    def __init__(self, channel, stream_id, time_interval, calculated_intervals, modifiers, get_results_func, tool,
                 input_streams):
        self.channel = channel
        if not isinstance(stream_id, StreamId):
            raise TypeError(str(type(stream_id)))
        self.stream_id = stream_id
        self.time_interval = time_interval
        if modifiers is None:
            raise ValueError(modifiers)
        self.modifiers = modifiers
        self.get_results_func = get_results_func
        if not isinstance(calculated_intervals, TimeIntervals):
            raise TypeError(str(type(calculated_intervals)))
        self.calculated_intervals = calculated_intervals
        self.kwargs = {}
        # if tool and not isinstance(tool, Tool):
        #     raise ValueError("Expected Tool, got {}".format(type(tool)))
        # TODO tool is now itself a stream_ref
        self.tool = tool
        self.input_streams = input_streams

    def __str__(self):
        # Deferred imports to avoid circular dependency
        # from channels import ReadOnlyMemoryChannel, FileChannel
        # if isinstance(self.channel, ReadOnlyMemoryChannel):
        #     return 'externally defined, memory-based, read-only stream'
        # if isinstance(self.channel, FileChannel):
        #     return 'externally defined by the file system, read-only stream'

        s = "StreamReference"
        s += "\n      CHANNEL_ID  : " + repr(self.channel.channel_id)
        s += "\n      STREAM_ID   : " + repr(self.stream_id)
        s += "\n      START       : " + repr(self.time_interval.start if self.time_interval else None)
        s += "\n      END         : " + repr(self.time_interval.end if self.time_interval else None)
        s += "\n      MODIFIERS   : " + repr(self.modifiers)
        s += "\n    "
        return s

    def __eq__(self, other):
        return str(self) == str(other)

    # def __hash__(self):
    #     return hash(str(self))

    @property
    def writer(self):
        return self.channel.get_stream_writer(self.stream_id)

    def define(self, input_streams=None, **kwargs):
        """
        Define the stream with the given keyword arguments
        :param input_streams: The input streams
        :param kwargs: keyword arguments
        :return: the stream reference
        """
        # TODO: better documentation
        # Don't obliterate existing input_streams definition if there was one
        if input_streams:
            self.input_streams = input_streams

        # TODO: Possibly combine with existing kwargs ... defaults?
        self.kwargs = kwargs

        # Deferred imports to avoid circular dependency
        from channels import ToolChannel

        if isinstance(self.channel, ToolChannel):
            try:
                return self.get_results_func(self, **kwargs)
            except TypeError as e:
                # TODO: for debugging
                raise e
        else:
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

    # def items(self, **kwargs):
    #     return self.get_results_func(self, kwargs)

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

    def items(self):
        # Get interval and writer
        # return self.tool.items(interval, writer)
        logging.debug("---Getting items for {} channel, stream {}, with kwargs {}".format(
            self.channel.channel_id, self.stream_id.name, self.kwargs))
        return self.channel.get_results(self, **self.kwargs)

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

