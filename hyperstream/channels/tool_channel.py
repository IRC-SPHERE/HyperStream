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
from module_channel import ModuleChannel
from ..stream import StreamDefinition
import logging


class ToolChannel(ModuleChannel):
    def create_stream(self, stream_def):
        raise NotImplementedError("Read-only stream")

    def get_results(self, stream_ref, args, kwargs):
        (version, module_importer) = super(ToolChannel, self).get_results(stream_ref, args, kwargs)
        module = module_importer()
        class_name = stream_ref.stream_id
        class_name = class_name[0].upper() + class_name[1:]
        tool_class = getattr(module, class_name)
        tool = tool_class()
        (args, kwargs) = tool.process_params(*args, **kwargs)
        stream_definition = StreamDefinition(tool, *args, **kwargs)
        logging.debug(stream_definition)
        return stream_definition

