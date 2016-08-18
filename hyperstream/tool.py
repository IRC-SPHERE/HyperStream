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
from models import ToolDefinitionModel
import imp
import os
import logging
from utils import Printable


class ToolCollection(Printable):
    def __init__(self, tool_path):
        self.tools = {}

        # TODO: Rather than trying to load in modules here - this needs to search the tool channel
        for t in ToolDefinitionModel.objects:
            try:
                src = imp.load_source(t.tool_id, os.path.join(tool_path, t.tool_id, t.version, "runner.py"))
                self.tools[t.tool_id, t.tool_version] = src
            except (IOError, ImportError):
                logging.error("No tool found with appropriate version: " + t.tool_id)

    def __getitem__(self, item):
        """
        This is so we can do tool = tools[tool_name, tool_version]
        :param item: tuple of name and version
        :return: the tool
        """
        if not isinstance(item, (list, tuple)) or len(item) != 2 \
                or not isinstance(item[0], (str, unicode)) or not isinstance(item[1], (str, unicode)):
            raise KeyError("Expecting tuple of (tool_name, tool_version), got: " + str(item))
        return self.tools[item]


class Tool(Printable):
    def process_params(self, *args, **kwargs):
        logging.info('Defining a {} stream'.format(self.__class__.__name__))

        return args, kwargs

    def normalise_args(self, args):
        return args

    def normalise_kwargs(self, kwargs):
        return kwargs

    def normalise_tool(self):
        # alternatively, could return e.g.:
        #  return 'tools.test.2016-07-10T10:15:54.932_v1'
        return self.__class__.__module__

    def normalise_stream_def(self, stream_def):
        nt = self.normalise_tool()
        na = self.normalise_args(stream_def.args)
        nk = self.normalise_kwargs(stream_def.kwargs)

        keys = tuple(sorted(nk.keys()))
        values = tuple([nk[k] for k in keys])

        return nt, tuple(na), keys, values

    def __str__(self):
        # TODO: @Meelis: Should this return __name__ or self.__class__.__name__ ?
        return self.__class__.__name__

    def __hash__(self):
        # TODO: @Meelis: should this return hash(__name__) or hash(self.__class__.__name__) ?
        return hash(self.__class__.__name__)

    def execute(self, *args, **kwargs):
        # Expecting at least: stream_def, start, end, writer
        # Then expecting whatever parameters the tool requires.
        raise NotImplementedError()

    @staticmethod
    def _normalise_kwargs(without, **kwargs):
        return dict(filter(lambda (kk, vv): kk not in without, dict(kwargs).items()))
