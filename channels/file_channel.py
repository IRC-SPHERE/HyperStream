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
from ..utils import Printable

from dateutil.parser import parse
import os
from semantic_version import Version
from datetime import datetime
import logging
import pytz


class FileDateTimeVersion(Printable):
    def __init__(self, filename, split_char='_'):
        self.long_filename, self.extension = os.path.splitext(filename)
        
        self.datetime, self.version = self.long_filename.split(split_char)
        
        self.datetime = parse(self.datetime)
        self.version = Version(self.version[1:])
    
    @property
    def is_python(self):
        return self.extension == '.py'
    
    def __repr__(self):
        return (
            "Parsing '{self.long_filname}': \n" +
            " Extension: {self.extension} \n" +
            " Date time: {self.datetime} \n" +
            "   Version: {self.version} \n"
        ).format(**self)


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
    
    def __init__(self, channel_id, path, up_to_timestamp=datetime.min.replace(tzinfo=pytz.utc)):
        self.path = path
        super(FileChannel, self).__init__(channel_id, up_to_timestamp)
    
    def repr_stream(self, stream_id):
        s = 'externally defined by the file system, read-only stream'
        s = s + ', currently holding ' + str(len(self.streams[stream_id])) + ' files'
        return s
    
    def file_filter(self, sorted_file_names):
        for file_long_name in sorted_file_names:
            if file_long_name != '__init__.py':
                try:
                    file_datetime_version = FileDateTimeVersion(file_long_name)
                    
                    yield file_datetime_version
                    # date_string, version = file_long_name.split('_')
                    # # Assume version is v*.py
                    # yield (parse(date_string), version[1:-3], file_long_name)
                except ValueError as e:
                    logging.warn('Filename in incorrect format {0}, {1}', file_long_name, e.message)
    
    def update_streams(self, up_to_timestamp):
        path = self.path
        for (long_path, dir_names, file_names) in os.walk(path):
            short_path = long_path[len(path) + 1:]
            stream_id = short_path
            self.streams[stream_id] = []
            for (file_timestamp, file_short_name, file_long_name) in self.file_filter(sorted(file_names)):
                if file_timestamp <= up_to_timestamp:
                    self.streams[stream_id].append((file_timestamp, self.data_loader(short_path, file_long_name)))
    
    def data_loader(self, short_path, file_long_name):
        raise NotImplementedError
    
    def get_default_ref(self):
        return {'start': datetime.min.replace(tzinfo=pytz.utc), 'end': self.up_to_timestamp, 'modifier': Identity()}
