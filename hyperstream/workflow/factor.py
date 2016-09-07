# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.
from ..utils import Printable
from node import Node
from ..tool import Tool
import logging


class Factor(Printable):
    """
    """

    def __init__(self, tool, sources, sink, plates):
        """
        Initialise this factor
        :param tool: The tool
        :param sources: The source nodes
        :param sink: The sink node
        :param plates: The plates over which this factor is defined
        :type tool: Tool
        :type sink: Node
        """
        if not isinstance(tool, Tool):
            raise ValueError("Expected node, got {}".format(type(tool)))
        self.tool = tool
        if sources:
            for source in sources:
                if not isinstance(source, Node):
                    raise ValueError("Expected node, got {}".format(type(source)))
        self.sources = sources
        if not isinstance(sink, Node):
            raise ValueError("Expected node, got {}".format(type(sink)))
        self.sink = sink

        self.plates = plates

    def execute(self, time_interval):
        # return self.tool.get_results(self)

        sources = []
        sink = None

        if self.plates:
            for plate in self.plates:
                for pv in plate.values:
                    if self.sources:
                        for source in self.sources:
                            if pv in source.streams:
                                sources.append(source.streams[pv])
                            else:
                                logging.warn("Plate {} with value {} not valid for source {}".format(plate, pv, source))
                    sink = self.sink.streams[pv]
        else:
            # The following is done below
            # sources = [source.streams[None] for source in self.sources] if self.sources else None
            sink = self.sink.streams[None]

        if not sink:
            raise RuntimeError("Sink undefined")

        # Also add streams that live outside of the plates
        if self.sources:
            for source in self.sources:
                if None in source.streams:
                    sources.append(source.streams[None])

        self.tool.execute(input_streams=sources, interval=time_interval, writer=sink.writer)
