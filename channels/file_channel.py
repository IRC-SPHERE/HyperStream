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
from ..stream import StreamId, StreamReference

from ..modifiers import Identity, Last
from ..utils import Printable, UTC, MIN_DATE
from ..time_interval import TimeInterval

from dateutil.parser import parse
import os
from semantic_version import Version
from datetime import datetime
import logging


class FileDateTimeVersion(Printable):
    def __init__(self, filename, split_char='_'):
        self.long_filename = filename
        self.filename_no_extension, self.extension = os.path.splitext(filename)
        
        self.timestamp, self.version = self.filename_no_extension.split(split_char, 1)
        
        self.timestamp = parse(self.timestamp)
        
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
    
    def repr_stream(self, stream_id):
        # TODO: self.streams no longer a list!
        s = 'externally defined by the file system, read-only stream'
        s = s + ', currently holding ' + str(len(self.streams[stream_id])) + ' files'
        return s
    
    def file_filter(self, sorted_file_names):
        for file_long_name in sorted_file_names:
            if file_long_name[:11] != '__init__.py':
                try:
                    tool_info = FileDateTimeVersion(file_long_name)
                    
                    yield tool_info
                
                except ValueError as e:
                    logging.warn('Filename in incorrect format {0}, {1}'.format(file_long_name, e.message))
    
    def update_streams(self, up_to_timestamp):
        path = self.path
        for (long_path, dir_names, file_names) in os.walk(path):
            # TODO: Add this condition to overcome 0-length IDs
            # file_names = filter(lambda ff: ff != '__init__.py', file_names)
            # if len(file_names) == 0:
            #     continue

            name = long_path[len(path) + 1:]
            if not name:
                # Empty folder
                continue

            stream_id = StreamId(name=name)

            self.streams[stream_id] = []
            
            # def f(stream_ref, *args, **kwargs):
            for tool_info in self.file_filter(sorted(file_names)):
                if tool_info.timestamp <= up_to_timestamp:
                    self.streams[stream_id].append((tool_info, self.data_loader(stream_id.name, tool_info)))
                    # yield tool_info.timestamp, self.data_loader(short_path, tool_info)
                    
                    # self.streams[stream_id] = StreamReference(
                    #     channel_id=self.state.channel_id,
                    #     stream_id=stream_id,
                    #     time_interval=TimeInterval(start=datetime.min.replace(tzinfo=pytz.UTC), end=up_to_timestamp),
                    #     modifier=Last(),
                    #     get_results_func=f
                    # )
    
    def data_loader(self, short_path, file_long_name):
        raise NotImplementedError
    
    def get_default_ref(self):
        return {'start': MIN_DATE, 'end': self.up_to_timestamp, 'modifier': Identity()}
