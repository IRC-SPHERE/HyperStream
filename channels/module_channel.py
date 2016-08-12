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
from file_channel import FileChannel
from ..modifiers import Last, IData
from datetime import datetime
import pytz
import logging


class ModuleChannel(FileChannel):
    """
    A channel of module streams, the documents in the streams contain functions that can be called to import the
    respective module
    """

    def create_stream(self, stream_def):
        pass

    def get_stream_writer(self, stream_id):
        pass

    versions = None

    def update_state(self, up_to_timestamp):
        super(ModuleChannel, self).update_state(up_to_timestamp)
        versions = {}
        self.versions = versions
        for stream_id in self.streams.keys():
            for (timestamp, (version, module_importer)) in self.streams[stream_id]:
                name = stream_id.replace("/", "_").replace(".", "_")
                name_version = name + "_" + version.replace("/", "_").replace(".", "_")
                versions[name_version] = self[stream_id, datetime.min.replace(tzinfo=pytz.utc), timestamp]
                versions[name] = versions[name_version]

    def file_filter(self, sorted_file_names):
        for (file_timestamp, file_short_name, file_long_name) in super(ModuleChannel, self).file_filter(
                sorted_file_names):
            if file_short_name[-3:] == '.py':
                yield (file_timestamp, file_short_name, file_long_name)

    def data_loader(self, short_path, file_long_name):
        version = file_long_name[24:-3]
        module_file = '/'.join([self.path, short_path, file_long_name])
        module_file_components = module_file[:-3].split('/')

        def module_importer():
            logging.debug('importing ' + module_file)
            module = __import__(module_file[:-3].replace('/', '.'))
            for component in module_file_components[1:]:
                module = module.__dict__[component]
            return module

        return version, module_importer

    def get_default_ref(self):
        return {'start': datetime.min.replace(tzinfo=pytz.utc), 'end': self.up_to_timestamp, 'modifier': Last() + IData()}
