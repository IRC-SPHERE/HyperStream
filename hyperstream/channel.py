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
from utils import Printable, utcnow
from errors import StreamNotFoundError, StreamAlreadyExistsError, ChannelNotFoundError
from models import StreamDefinitionModel
from stream import StreamId, Stream
# from modifiers import Identity
from time_interval import TimeInterval, TimeIntervals

# import logging
from collections import namedtuple


ChannelCollectionBase = namedtuple("ChannelCollectionBase", "tools sphere memory mongo", verbose=False, rename=False)


class ChannelCollection(ChannelCollectionBase, Printable):
    """
    Container for the predefined channels.
    """
    def __new__(cls, tool_path):
        """
        This is immutable, so we use new instead of init
        Note that we use deferred imports to avoid circular dependencies
        :param tool_path: The tool path
        :return: the ChannelCollection object
        """
        from channels import ToolChannel, SphereChannel, MemoryChannel, DatabaseChannel
        self = super(ChannelCollection, cls).__new__(cls,
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
            # channel = self.channels[s.channel_id]
            channel = self.get_channel(s.channel_id)

            # TODO: Do we need a TimeInterval defined here?
            # TODO: Do we need anything other than Identity() for the modifiers here?

            if stream_id in channel.streams:
                raise StreamAlreadyExistsError(stream_id)

            # TODO: Use tool versions - here we just take the latest one
            tool_class = self.tools[StreamId(s.tool_name)].define().items()[-1].value
            tool = tool_class(**s.tool_parameters)

            # tool = self.tools.get_tool(s.tool_name, s.tool_version, s.tool_parameters)

            input_streams = []
            for input_stream in s.input_streams:
                input_streams.append(StreamId(name=input_stream.name, meta_data=input_stream.meta_data))

            if tool:
                channel.streams[stream_id] = Stream(
                    channel=channel,
                    stream_id=stream_id,
                    tool=tool,
                    time_interval=None,   # TODO
                    calculated_intervals=TimeIntervals(),
                    modifier=None,
                    # get_results_func=channel.get_results,
                    input_streams=input_streams
                )
