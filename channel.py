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
from channels import ToolChannel, SphereChannel, MemoryChannel, DatabaseChannel
from errors import StreamNotFoundError, StreamAlreadyExistsError
from models import StreamDefinitionModel
from stream import StreamId, StreamReference
from modifiers import Identity

import logging


class ChannelCollection(Printable):
    def __init__(self, tool_path):
        self.tool_channel = ToolChannel("tools", tool_path, up_to_timestamp=utcnow())
        self.sphere_channel = SphereChannel("sphere")
        self.memory_channel = MemoryChannel("memory")
        self.database_channel = DatabaseChannel("mongo")

        # TODO: find a more elegant solution for this
        self.channels = {
            'tools': self.tool_channel,
            'sphere': self.sphere_channel,
            'memory': self.memory_channel,
            'database': self.database_channel
        }

        self.update_channels()
        for channel in self.channels:
            logging.debug(channel)
            logging.debug(self.channels[channel])
            logging.debug("")

    def get_stream(self, stream_id):
        if stream_id in self.memory_channel:
            return self.memory_channel[stream_id]
        if stream_id in self.database_channel:
            return self.database_channel[stream_id]
        if stream_id in self.tool_channel:
            return self.tool_channel[stream_id]
        if stream_id in self.sphere_channel:
            return self.sphere_channel[stream_id]
        raise StreamNotFoundError("{} not found in channels".format(stream_id))

    def update_channels(self):
        for s in StreamDefinitionModel.objects():
            stream_id = StreamId(name=s.stream_id.name, meta_data=s.stream_id.meta_data)
            channel = self.channels[s.channel]

            # TODO: Do we need a TimeInterval defined here?
            # TODO: Do we need anything other than Identity() for the modifier here?

            if stream_id in channel.streams:
                raise StreamAlreadyExistsError(stream_id)

            channel.streams[stream_id] = StreamReference(
                channel_id=s.channel,
                stream_id=stream_id,
                time_interval=None,
                modifier=Identity(),
                get_results_func=channel.get_results
            )
