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
import os
import logging
from ..utils import Printable
from ..models import ToolDefinitionModel
import imp
# from ..interface import StandardInput, StandardOutput, RawInput


# INPUT_TYPES = {
#     "raw": RawInput,
#     "standard": StandardInput
# }
#
# OUTPUT_TYPES = {
#     "standard": StandardOutput
# }


class ToolCollection(Printable):
    tools = {}

    def __init__(self, tool_path):
        for t in ToolDefinitionModel.objects:
            try:
                src = imp.load_source(t.tool_id, os.path.join(tool_path, t.tool_id, t.version, "runner.py"))
                self.tools[t.tool_id] = src
            except (IOError, ImportError):
                logging.error("No tool found with appropriate version: " + t.tool_id)
                # raise

                # if d['input_type'] not in INPUT_TYPES:
                #     raise NotImplementedError("Unknown input type: " + d['input_type'])
                # input_interface = INPUT_TYPES[d['input_type']]().get_data
                #
                # if d['output_type'] not in OUTPUT_TYPES:
                #     raise NotImplementedError("Unknown output type: " + d['output_type'])
                # output_interface = OUTPUT_TYPES[d['output_type']]().put_data
                #
                # del d['input_type']
                # del d['output_type']
                #
                # self.tools[tool_id] = Tool(src.Runner(input_interface, output_interface), tool_id, **d)
