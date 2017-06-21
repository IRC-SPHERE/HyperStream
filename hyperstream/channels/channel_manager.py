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
"""
Channel manager module. Defines the ChannelManager - a container for channels, that can load in plugins
"""


import inspect
import logging
# from mongoengine import DoesNotExist, MultipleObjectsReturned
from mongoengine.context_managers import switch_db
import os

from hyperstream.models import StreamDefinitionModel
from hyperstream.stream import StreamId, DatabaseStream, AssetStream
from hyperstream.utils import Printable, utcnow, MIN_DATE, StreamAlreadyExistsError, ChannelNotFoundError, \
    ToolNotFoundError, ChannelAlreadyExistsError, ToolInitialisationError
from hyperstream.channels import ToolChannel, MemoryChannel, DatabaseChannel, AssetsChannel, AssetsChannel2


class ChannelManager(dict, Printable):
    """
    Container for channels.
    """
    def __init__(self, plugins, **kwargs):
        super(ChannelManager, self).__init__(**kwargs)

        # See this answer http://stackoverflow.com/a/14620633 for why we do the following:
        self.__dict__ = self

        tool_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tools'))

        self.tools = ToolChannel("tools", tool_path, up_to_timestamp=utcnow())
        self.memory = MemoryChannel("memory")
        self.mongo = DatabaseChannel("mongo")
        self.assets = AssetsChannel("assets")

        for plugin in plugins:
            for channel in plugin.load_channels():
                if channel.channel_id in self:
                    raise ChannelAlreadyExistsError(channel.channel_id)
                self[channel.channel_id] = channel

        self.update_channels()

    @property
    def tool_channels(self):
        """
        The tool channels as a list
        """
        return [c for c in self.values() if isinstance(c, ToolChannel)]

    @property
    def memory_channels(self):
        """
        The memory channels as a list
        """
        return [c for c in self.values() if isinstance(c, MemoryChannel)]

    @property
    def database_channels(self):
        """
        The database channels as a list
        """
        return [c for c in self.values() if isinstance(c, DatabaseChannel)]

    def get_channel(self, channel_id):
        """
        Get the channel by id

        :param channel_id: The channel id
        :return: The channel object
        """
        try:
            return self[channel_id]
        except KeyError:
            raise ChannelNotFoundError("Channel {} not found".format(channel_id))

    def update_channels(self):
        """
        Pulls out all of the stream definitions from the database, and populates the channels with stream references
        """
        logging.info("Updating channels")
        with switch_db(StreamDefinitionModel, 'hyperstream'):
            for s in StreamDefinitionModel.objects():
                try:
                    stream_id = StreamId(name=s.stream_id.name, meta_data=s.stream_id.meta_data)
                except AttributeError as e:
                    raise e
                logging.debug("Processing {}".format(stream_id))
                channel = self.get_channel(s.channel_id)
                # calculated_intervals = TimeIntervals(map(lambda x: (x.start, x.end), s.calculated_intervals))
                last_accessed = utcnow()
                last_updated = s.last_updated if s.last_updated else utcnow()

                if stream_id in channel.streams:
                    if isinstance(channel, (AssetsChannel, AssetsChannel2)):
                        continue
                    raise StreamAlreadyExistsError(stream_id)

                from . import MemoryChannel, DatabaseChannel
                if isinstance(channel, MemoryChannel):
                    channel.create_stream(stream_id)
                elif isinstance(channel, DatabaseChannel):
                    if channel == self.assets:
                        stream_type = AssetStream
                    else:
                        stream_type = DatabaseStream

                    channel.streams[stream_id] = stream_type(
                        channel=channel,
                        stream_id=stream_id,
                        calculated_intervals=None,  # Not required since it's initialised from mongo_model in __init__
                        last_accessed=last_accessed,
                        last_updated=last_updated,
                        sandbox=s.sandbox,
                        mongo_model=s
                    )
                else:
                    logging.warn("Unable to parse stream {}".format(stream_id))

    def populate_assets(self, tool_id):
        """
        TODO: Unused?
        """
        tool_stream_view = None

        # Look in the main tool channel first
        if tool_id in self.tools:
            tool_stream_view = self.tools[tool_id].window((MIN_DATE, self.tools.up_to_timestamp))
        else:
            # Otherwise look through all the channels in the order they were defined
            for tool_channel in self.tool_channels:
                if tool_channel == self.tools:
                    continue
                if tool_id in tool_channel:
                    # noinspection PyTypeChecker
                    tool_stream_view = tool_channel[tool_id].window((MIN_DATE, tool_channel.up_to_timestamp))

        if tool_stream_view is None:
            raise ToolNotFoundError(tool_id)

        # TODO: Use tool versions - here we just take the latest one
        return tool_stream_view.last().value

    def get_tool_class(self, tool):
        """
        Gets the actual class which can then be instantiated with its parameters

        :param tool: The tool name or id
        :type tool: str | unicode | StreamId
        :rtype: Tool | MultiOutputTool
        :return: The tool class
        """
        if isinstance(tool, (str, unicode)):
            tool_id = StreamId(tool)
        elif isinstance(tool, StreamId):
            tool_id = tool
        else:
            raise TypeError(tool)

        tool_stream_view = None

        # Look in the main tool channel first
        if tool_id in self.tools:
            tool_stream_view = self.tools[tool_id].window((MIN_DATE, self.tools.up_to_timestamp))
        else:
            # Otherwise look through all the channels in the order they were defined
            for tool_channel in self.tool_channels:
                if tool_channel == self.tools:
                    continue
                if tool_id in tool_channel:
                    # noinspection PyTypeChecker
                    tool_stream_view = tool_channel[tool_id].window((MIN_DATE, tool_channel.up_to_timestamp))

        if tool_stream_view is None:
            raise ToolNotFoundError(tool)

        # TODO: Use tool versions - here we just take the latest one
        return tool_stream_view.last().value

    def get_tool(self, name, parameters, version=None):
        """
        Gets the tool object from the tool channel(s), and instantiates it using the tool parameters

        :param name: The name or stream id for the tool in the tool channel
        :param parameters: The parameters for the tool
        :param version: The string representation of the version
        :return: The instantiated tool object
        """
        # TODO: use the version
        if version is not None:
            logging.warn("Tool versions not yet supported")

        tool_class = self.get_tool_class(name)

        # Check that the number of arguments is correct for this tool
        arg_spec = inspect.getargspec(tool_class.__init__)
        max_expected = len(arg_spec[0])
        if arg_spec.defaults:
            min_expected = max_expected - len(arg_spec.defaults)
        else:
            min_expected = max_expected
        num_parameters = len(parameters) if parameters is not None else 0
        if not (min_expected <= num_parameters + 1 <= max_expected):
            message = "Tool {} takes between {} and {} arguments ({} given)".format(
                tool_class.__name__, min_expected, max_expected, num_parameters + 1)
            raise ToolInitialisationError(message)

        # Instantiate tool
        try:
            tool = tool_class(**parameters) if parameters is not None else tool_class()
        except TypeError:
            raise ToolInitialisationError(name, parameters)

        if not tool:
            raise ToolNotFoundError(name, parameters)

        tool.name = name

        return tool
