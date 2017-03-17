# The MIT License (MIT)
#  Copyright (c) 2014-2017 University of Bristol
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#  # The above copyright notice and this permission notice shall be included in all
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
Assets channel module.
"""
from database_channel import DatabaseChannel
from hyperstream.utils.errors import StreamAlreadyExistsError, StreamNotFoundError
from ..stream import AssetStream, StreamInstance
from ..utils import utcnow


class AssetsChannel(DatabaseChannel):
    """
    Assets Channel. Special kind of database channel for static assets and user input data (workflow parameters etc)
    """
    def __init__(self, channel_id):
        """
        Initialise this channel
        :param channel_id: The channel identifier
        """
        super(AssetsChannel, self).__init__(channel_id=channel_id)
        # self.update_streams(utcnow())

    def update_streams(self, up_to_timestamp):
        """
        Update the streams
        :param up_to_timestamp:
        :return:
        """
        raise NotImplementedError

    def purge_stream(self, stream_id, sandbox=None):
        """
        Purge the stream
        :param stream_id: The stream identifier
        :param sandbox: The sandbox for this stream
        :return: None
        """
        super(AssetsChannel, self).purge_stream(stream_id=stream_id, sandbox=sandbox)

    def create_stream(self, stream_id, sandbox=None):
        """
        Create the stream
        :param stream_id: The stream identifier
        :param sandbox: The sandbox for this stream
        :return: None
        :raises: NotImplementedError
        """
        if sandbox is not None:
            raise NotImplementedError

        if stream_id in self.streams:
            raise StreamAlreadyExistsError("Stream with id '{}' already exists".format(stream_id))

        stream = AssetStream(channel=self, stream_id=stream_id, calculated_intervals=None,
                             last_accessed=utcnow(), last_updated=utcnow(), sandbox=sandbox)
        self.streams[stream_id] = stream
        return stream

    def write_to_stream(self, stream_id, data, sandbox=None):
        """
        Create the stream
        :param stream_id: The stream identifier
        :param data: The stream instances
        :param sandbox: The sandbox for this stream
        :type stream_id: StreamId
        :return: None
        :raises: NotImplementedError
        """
        if sandbox is not None:
            raise NotImplementedError

        if stream_id not in self.streams:
            raise StreamNotFoundError("Stream with id '{}' does not exist".format(stream_id))

        writer = self.get_stream_writer(self.streams[stream_id])

        if isinstance(data, StreamInstance):
            data = [data]

        for instance in data:
            if not isinstance(instance, StreamInstance):
                raise ValueError("Expected StreamInstance, got {}".format(str(type(instance))))
            writer(instance)
