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

from .file_channel import FileChannel

from os.path import join
from re import sub
import logging
import imp


class ModuleChannel(FileChannel):
    """
    A channel of module streams, the documents in the streams contain functions that can be called to import the
    respective module
    """
    versions = None
    
    def update_state(self, up_to_timestamp):
        super(ModuleChannel, self).update_state(up_to_timestamp)
    
    def file_filter(self, sorted_file_names):
        for tool_info in super(ModuleChannel, self).file_filter(sorted_file_names):
            if tool_info.is_python:
                yield tool_info
    
    def data_loader(self, short_path, tool_info):
        module_file = join(self.path, short_path, tool_info.long_filename)
        module_file_components = module_file[:-3].split('/')
        
        def module_importer():
            with open(module_file, 'rb') as fp:
                # logging.debug('importing: ' + module_file)
                module_name = '_'.join(map(lambda pp: sub(r'[^a-zA-Z0-9]', '_', pp), module_file_components))

                module = imp.load_module(
                    module_name, fp, module_file,
                    ('.py', 'rb', imp.PY_SOURCE)
                )

                return module

        return tool_info.version, module_importer
