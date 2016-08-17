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
from utils import Printable
# from models import StreamDefinitionModel


# class StreamCollection(Printable):
#     streams = {}
#
#     def __init__(self, tool_channel):
#         for stream_definition in StreamDefinitionModel.objects:
#             # TODO: tool_version is currently ignored
#             tool = tool_channel[stream_definition.tool_name]
#             self.streams[stream_definition.stream_id] = StreamDefinition(tool, stream_definition.parameters)


class StreamDict(dict):
    """
    Custom dictionary where keys are StreamID objects and values are StreamReference objects
    """
    def __getitem__(self, key):
        if not isinstance(key, StreamId):
            raise TypeError("key")
        try:
            return super(StreamDict, self).__getitem__(key)
        except KeyError as e:
            # for debugging
            raise e

    def __setitem__(self, key, value):
        if not isinstance(key, StreamId):
            raise TypeError("key")
        # TODO: Enforce values to be StreamReference
        # if not isinstance(value, StreamReference):
        #     raise ValueError("value")
        super(StreamDict, self).__setitem__(key, value)


class StreamId(Printable):
    def __init__(self, name, meta_data=None):
        self.name = name
        self.meta_data = meta_data if meta_data else {}

    def __str__(self):
        if self.meta_data:
            return self.name + "_" + "_".join("{}={}".format(k, v) for k, v in self.meta_data.items())
        else:
            return self.name

    def __hash__(self):
        return hash((self.name, repr(self.meta_data)))

    def __eq__(self, other):
        return isinstance(other, StreamId) and self.name == other.name and self.meta_data == other.meta_data


class StreamReference(object):
    def __init__(self, channel_id, stream_id, time_interval, modifier, get_results_func):
        self.channel_id = channel_id
        if not isinstance(stream_id, StreamId):
            raise TypeError(str(stream_id))
        self.stream_id = stream_id
        self.time_interval = time_interval
        self.modifier = modifier
        self.get_results_func = get_results_func

    def __repr__(self):
        # TODO possibly need repr as well? or even repr instead of str?
        s = "StreamReference"
        s += "\n      CHANNEL_ID  : " + repr(self.channel_id)
        s += "\n      STREAM_ID   : " + repr(self.stream_id)
        s += "\n      START       : " + repr(self.time_interval.start if self.time_interval else None)
        s += "\n      END         : " + repr(self.time_interval.end if self.time_interval else None)
        s += "\n      MODIFIER    : " + repr(self.modifier)
        s += "\n    "
        return s

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    def __call__(self, *args, **kwargs):
        return self.get_results_func(self, args, kwargs)


# TODO: need to check if the same stream already exists in the same channel or not
class StreamDefinition(object):
    """
    This is basically a tool and it's parameters. Also known as a factor in the factor graph representation.
    """
    def __init__(self, tool, *args, **kwargs):
        self.tool = tool
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        s = "StreamDefinition:"
        s += "\n    TOOL  : " + repr(self.tool)
        s += "\n    ARGS  : " + repr(self.args)
        s += "\n    KWARGS: " + repr(self.kwargs)
        return s

    def normalise(self):
        return self.tool.normalise_stream_def(self)

    def __eq__(self, other):
        return self.normalise() == other.normalise()

    def __hash__(self):
        return hash(self.normalise())
