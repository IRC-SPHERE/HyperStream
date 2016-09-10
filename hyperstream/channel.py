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

from utils import Printable, utcnow, MIN_DATE
from errors import StreamNotFoundError, StreamAlreadyExistsError, ChannelNotFoundError, ToolNotFoundError
from models import StreamDefinitionModel, StreamStatusModel
from stream import StreamId, DatabaseStream
from time_interval import TimeIntervals

from mongoengine import DoesNotExist, MultipleObjectsReturned
from mongoengine.context_managers import switch_db
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
                                                  mongo=DatabaseChannel("mongo"))

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
        with switch_db(StreamDefinitionModel, 'hyperstream'):
            for s in StreamDefinitionModel.objects():
                stream_id = StreamId(name=s.stream_id.name, meta_data=s.stream_id.meta_data)
                channel = self.get_channel(s.channel_id)

                if stream_id in channel.streams:
                    raise StreamAlreadyExistsError(stream_id)

                from channels import MemoryChannel, DatabaseChannel
                if isinstance(channel, MemoryChannel):
                    channel.create_stream(stream_id)
                elif isinstance(channel, DatabaseChannel):
                    calculated_intervals = None
                    with switch_db(StreamStatusModel, 'hyperstream'):
                        try:
                            status = StreamStatusModel.objects.get(__raw__=stream_id.as_raw())
                            calculated_intervals = TimeIntervals(map(lambda x: (x.start, x.end),
                                                                     status.calculated_intervals))
                        except DoesNotExist as e:
                            logging.debug(e)
                            status = StreamStatusModel(
                                stream_id=stream_id.as_dict(),
                                calculated_intervals=[],
                                last_accessed=utcnow(),
                                last_updated=utcnow())
                            status.save()
                        except MultipleObjectsReturned as e:
                            raise e

                    channel.streams[stream_id] = DatabaseStream(
                        channel=channel,
                        stream_id=stream_id,
                        calculated_intervals=calculated_intervals,
                        sandbox=s.sandbox)
                else:
                    raise NotImplementedError

    def get_tool(self, tool, tool_parameters):
        """
        Gets the tool object from the tool channel, and instantiates it using the tool parameters
        :param tool: The name or stream id for the tool in the tool channel
        :param tool_parameters: The parameters for the tool
        :return: The instantiated tool object
        """
        # TODO: Use tool versions - here we just take the latest one
        if isinstance(tool, (str, unicode)):
            tool_id = StreamId(tool)
        elif isinstance(tool, StreamId):
            tool_id = tool
        else:
            raise TypeError(tool)

        tool_stream_view = self.tools[tool_id].window((MIN_DATE, utcnow()))
        tool_class = tool_stream_view.last().value

        # Check that the number of arguments is correct for this tool
        arg_spec = inspect.getargspec(tool_class.__init__)
        max_expected = len(arg_spec[0])
        if arg_spec.defaults:
            min_expected = max_expected - len(arg_spec.defaults)
        else:
            min_expected = max_expected
        if not (min_expected <= len(tool_parameters) + 1 <= max_expected):
            message = "Tool {} takes a between {} and {} arguments ({} given)".format(
                tool_class.__name__, min_expected, max_expected, len(tool_parameters) + 1)
            # logging.warn(message)
            raise ValueError(message)

        # Instantiate tool
        tool = tool_class(**tool_parameters)
        if not tool:
            raise ToolNotFoundError

        return tool
