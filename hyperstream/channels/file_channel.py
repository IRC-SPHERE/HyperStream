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
from memory_channel import ReadOnlyMemoryChannel
from ..modifiers import Identity
from dateutil.parser import parse
import os
from datetime import datetime


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

    def get_stream_writer(self, stream_id):
        raise NotImplementedError()

    def create_stream(self, stream_def):
        raise NotImplementedError()

    def __init__(self, base_id, path, up_to_timestamp=datetime.min):
        self.path = path
        super(FileChannel, self).__init__(base_id, up_to_timestamp)

    def repr_stream(self, stream_id):
        s = 'externally defined by the file system, read-only stream'
        s = s + ', currently holding ' + str(len(self.streams[stream_id])) + ' files'
        return s

    def file_filter(self, sorted_file_names):
        for file_long_name in sorted_file_names:
            date_string, version = file_long_name.split('_')
            yield (parse(date_string), version, file_long_name)

    def update_streams(self, up_to_timestamp):
        path = self.path
        for (long_path, dir_names, file_names) in os.path.walk(path):
            short_path = long_path[len(path) + 1:]
            stream_id = short_path
            stream = []
            self.streams[stream_id] = stream
            for (file_timestamp, file_short_name, file_long_name) in self.file_filter(sorted(file_names)):
                if file_timestamp <= up_to_timestamp:
                    file_timestamp_str = file_long_name[:23]
                    stream.append((file_timestamp, self.data_loader(short_path, file_long_name)))

    def data_loader(self, short_path, file_long_name):
        raise NotImplementedError

    def get_default_ref(self):
        return {'start': datetime.min, 'end': self.up_to_timestamp, 'modifier': Identity()}
