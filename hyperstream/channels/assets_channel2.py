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
from . import FileChannel
from ..utils import UTC, utcnow
from ..utils.errors import StreamAlreadyExistsError, StreamNotFoundError
from ..stream import StreamId, AssetStream, StreamInstance

import os
import json
import logging
from collections import namedtuple
from dateutil.parser import parse


cls = namedtuple("LongFilenameTimestamp", "long_filename timestamp")


class AssetsChannel2(FileChannel):
    """
    Assets Channel. Special kind of file channel for static assets and user input data (workflow parameters etc)
    """

    def __init__(self, channel_id, path, up_to_timestamp):
        """
        Initialise this channel
        :param channel_id: The channel identifier
        :param up_to_timestamp: The time that this channel is valid up to
        """
        super(AssetsChannel2, self).__init__(channel_id=channel_id, path=path, up_to_timestamp=up_to_timestamp)
        # self.update_streams(utcnow())

    def update_streams(self, up_to_timestamp):
        path = self.path
        for (long_path, dir_names, file_names) in os.walk(path):
            file_names = filter(lambda ff: ff != '__init__.py', file_names)
            if len(file_names) == 0:
                continue

            name = long_path[len(path) + 1:]
            if not name:
                # Empty folder
                continue

            stream_id = StreamId(name=name)
            self.create_stream(stream_id, sandbox=None)

    def file_filter(self, sorted_file_names):
        for file_long_name in sorted_file_names:
            if file_long_name == '__init__.py':
                continue

            filename_no_extension, extension = os.path.splitext(file_long_name)
            if extension != ".json":
                logging.debug('Filename in incorrect format {0}'.format(file_long_name))

            if filename_no_extension != '__init__' and extension == '.json':
                try:
                    yield cls(file_long_name, parse(filename_no_extension).replace(tzinfo=UTC))
                except ValueError as e:
                    logging.warn("Failed to load {}: {}".format(file_long_name, e.message))

    def data_loader(self, short_path, file_info):
        filename = os.path.join(self.path, short_path, file_info.long_filename)
        data = json.load(open(filename))
        return data

    def get_stream_writer(self, stream):
        def writer(document_collection):
            if stream.stream_id not in self:
                raise RuntimeError("Data slot does not exist for {}, perhaps create_stream was not used?"
                                   .format(stream))
            if isinstance(document_collection, StreamInstance):
                document_collection = [document_collection]

            for t, doc in document_collection:
                t_str = t.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                filename = os.path.join(self.path, stream.stream_id.name, t_str + ".json")

                try:
                    with open(filename, 'w') as f:
                        json.dump(doc, f, indent=4, separators=(',', ': '), sort_keys=True)
                except IOError as e:
                    # Something wrong with writing the document - log the error
                    logging.error(e)

        return writer

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

        logging.debug("Creating asset stream {}".format(stream_id))

        if stream_id in self.streams:
            raise StreamAlreadyExistsError("Stream with id '{}' already exists".format(stream_id))

        stream = AssetStream(channel=self, stream_id=stream_id, calculated_intervals=None,
                             last_accessed=utcnow(), last_updated=utcnow(), sandbox=sandbox)
        self.streams[stream_id] = stream
        return stream

    def write_to_stream(self, stream_id, data, sandbox=None):
        """
        Write an instance to the stream
        
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
