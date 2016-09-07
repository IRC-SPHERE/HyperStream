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

from utils import Printable, utcnow
from errors import StreamNotFoundError, StreamAlreadyExistsError, ChannelNotFoundError, ToolNotFoundError
from models import StreamDefinitionModel
from stream import StreamId, Stream
from time_interval import TimeIntervals

import inspect
import logging
from collections import namedtuple


ChannelCollectionBase = namedtuple("ChannelCollectionBase", "tools sphere memory mongo", verbose=False, rename=False)


class ChannelManager(ChannelCollectionBase, Printable):
    """
    Container for the predefined channels.
    """
    def __new__(cls, tool_path):
        """
        This is immutable, so we use new instead of init
        Note that we use deferred imports to avoid circular dependencies
        :param tool_path: The tool path
        :return: the ChannelManager object
        """
        from channels import ToolChannel, SphereChannel, MemoryChannel, DatabaseChannel
        self = super(ChannelManager, cls).__new__(cls,
                                                  tools=ToolChannel("tools", tool_path, up_to_timestamp=utcnow()),
                                                  sphere=SphereChannel("sphere"),
                                                  memory=MemoryChannel("memory"),
                                                  mongo=DatabaseChannel("mongo")
                                                  )

        self.update_channels()
        return self

    def get_stream(self, stream_id):
        """
        Searches all of the channels for a particular stream by id
        :param stream_id: The stream id
        :return: The stream reference
        """
        for channel in self:
            if stream_id in channel.streams:
                return channel.streams[stream_id]
        raise StreamNotFoundError("{} not found in channels".format(stream_id))

    def get_channel(self, channel_id):
        """
        Get the channel by id
        :param channel_id: The channel id
        :return: The channel object
        """
        for channel in self:
            if channel.channel_id == channel_id:
                return channel
        raise ChannelNotFoundError("Channel {} not found".format(channel_id))

    def update_channels(self):
        """
        Pulls out all of the stream definitions from the database, and populates the channels with stream references
        :return: None
        """
        for s in StreamDefinitionModel.objects():
            stream_id = StreamId(name=s.stream_id.name, meta_data=s.stream_id.meta_data)
            channel = self.get_channel(s.channel_id)

            if stream_id in channel.streams:
                raise StreamAlreadyExistsError(stream_id)

            stream = Stream(
                channel=channel,
                stream_id=stream_id,
                calculated_intervals=TimeIntervals()
            )

            channel.streams[stream_id] = stream

        # Would like to just loop over the objects, but because some streams depend on other streams, we will have to
        # loop through multiple times until they're all processed
        # stream_objects = set(StreamDefinitionModel.objects())
        # # undefined = set()
        #
        # while stream_objects:
        #     logging.debug("{} stream objects to process".format(len(stream_objects)))
        #
        #     copy = stream_objects.copy()
        #     for s in copy:
        #         stream_id = StreamId(name=s.stream_id.name, meta_data=s.stream_id.meta_data)
        #         channel = self.get_channel(s.channel_id)
        #
        #         # logging.debug("Getting input streams for {}".format(stream_id))
        #         # input_streams = []
        #         # before = len(undefined)
        #         # for input_stream in s.input_streams:
        #         #     input_stream_id = StreamId(name=input_stream.name, meta_data=input_stream.meta_data)
        #         #     if input_stream_id in channel:
        #         #         input_streams.append(channel.streams[input_stream_id])
        #         #     else:
        #         #         undefined.add(input_stream_id)
        #         #
        #         # if len(undefined) > before:
        #         #     continue
        #
        #         if stream_id in channel.streams:
        #             raise StreamAlreadyExistsError(stream_id)
        #
        #         # TODO: Create a Factor here?
        #         # tool = self.get_tool(s.tool_name, s.tool_parameters, input_streams)
        #
        #         stream = Stream(
        #             channel=channel,
        #             stream_id=stream_id,
        #             # tool=tool,
        #             calculated_intervals=TimeIntervals()
        #         )
        #
        #         channel.streams[stream_id] = stream
        #
        #         # stream_objects.remove(s)
        #         # if s in undefined:
        #         #     undefined.remove(s)
        #     # if len(stream_objects) > 0 and len(stream_objects) == len(copy):
        #     #     raise StreamNotFoundError("{} stream objects still remain".format(len(stream_objects)))

    def get_tool(self, tool_name, tool_parameters, input_streams):
        # TODO: Use tool versions - here we just take the latest one
        tool_stream = self.tools[StreamId(tool_name)]
        tool_class = tool_stream.items()[-1].value

        # Check that the number of arguments is correct for this tool
        expected = len(inspect.getargspec(tool_class.__init__)[0])
        if expected != len(tool_parameters) + 1:
            raise ValueError("Tool {} takes {} arguments ({} given)".format(
                tool_class.__name__, expected, len(tool_parameters) + 1))

        # Instantiate tool
        tool = tool_class(**tool_parameters)
        if not tool:
            raise ToolNotFoundError

        # Input streams should be stream objects
        for stream in input_streams:
            if not(isinstance(input_streams, Stream)):
                raise ValueError("Stream expected ({} given)".format(type(stream)))

        # Make sure that the tool stream is defined
        # tool_stream.define(input_streams=input_streams, **tool_parameters)

        return tool

    def get_tool2(self, tool, tool_parameters):
        # TODO: Use tool versions - here we just take the latest one
        if isinstance(tool, (str, unicode)):
            tool_id = StreamId(tool)
        elif isinstance(tool, StreamId):
            tool_id = tool
        else:
            raise TypeError(tool)

        tool_stream = self.tools[tool_id]
        tool_class = tool_stream.items()[-1].value

        # Check that the number of arguments is correct for this tool
        expected = len(inspect.getargspec(tool_class.__init__)[0])
        if expected != len(tool_parameters) + 1:
            raise ValueError("Tool {} takes {} arguments ({} given)".format(
                tool_class.__name__, expected, len(tool_parameters) + 1))

        # Instantiate tool
        tool = tool_class(**tool_parameters)
        if not tool:
            raise ToolNotFoundError

        return tool
