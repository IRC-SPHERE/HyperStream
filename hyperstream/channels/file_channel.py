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

from memory_channel import ReadOnlyMemoryChannel
from ..stream import StreamId, Stream, StreamInstance
from ..utils import Printable, MIN_DATE, UTC

import ciso8601
import os
from semantic_version import Version
import logging


class FileDateTimeVersion(Printable):
    """
    Simple class to hold file details along with the timestamp and version number from the filename.
    Uses semantic version.
    """
    def __init__(self, filename, split_char='_'):
        self.long_filename = filename
        self.filename_no_extension, self.extension = os.path.splitext(filename)
        self.timestamp, self.version = self.filename_no_extension.split(split_char, 1)
        self.timestamp = ciso8601.parse_datetime(self.timestamp)
        if not self.timestamp:
            raise ValueError("Invalid timestamp {}".format(self.timestamp))
        self.timestamp = self.timestamp.replace(tzinfo=UTC)
        self.version = Version(self.version[1:])

    @property
    def is_python(self):
        return self.extension == '.py'


class FileChannel(ReadOnlyMemoryChannel):
    """
    An abstract stream channel where the streams are recursive sub-folders under a given path and documents correspond to
    all those files which have a timestamp as their prefix in the format yyyy_mm_dd_hh_mm_ss_mmm_*.
    All the derived classes must override the function data_loader(short_path,file_long_name) which determines how the
    data are loaded into the document of the stream.
    The files of the described format must never be deleted.
    The call update(up_to_timestamp) must not be called unless it is guaranteed that later no files with earlier
    timestamps are added.
    """
    path = ""

    def __init__(self, channel_id, path, up_to_timestamp=MIN_DATE):
        self.path = path
        super(FileChannel, self).__init__(channel_id=channel_id, up_to_timestamp=up_to_timestamp)

    def file_filter(self, sorted_file_names):
        for file_long_name in sorted_file_names:
            if file_long_name[:11] != '__init__.py' and file_long_name[-3:] == '.py':
                try:
                    tool_info = FileDateTimeVersion(file_long_name)

                    yield tool_info

                except ValueError as e:
                    logging.warn('Filename in incorrect format {0}, {1}'.format(file_long_name, e.message))

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
            stream = Stream(channel=self, stream_id=stream_id, calculated_intervals=None, sandbox=None)
            self.streams[stream_id] = stream

    def data_loader(self, short_path, file_info):
        raise NotImplementedError

    def get_results(self, stream, time_interval):
        # TODO: Make this behave like the other channels
        # if relative_time_interval.end > self.up_to_timestamp:
        #     raise ValueError(
        #         'The stream is not available after ' + str(self.up_to_timestamp) + ' and cannot be calculated')

        result = []
        module_path = os.path.join(self.path, stream.stream_id.name)

        for file_info in self.file_filter(sorted(os.listdir(module_path))):
            if file_info.timestamp in time_interval and file_info.timestamp <= self.up_to_timestamp:
                result.append(StreamInstance(
                    timestamp=file_info.timestamp,
                    value=self.data_loader(stream.stream_id.name, file_info)
                ))

        result.sort(key=lambda x: x.timestamp)
        return result

