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
    A factor in the graph. This defines the element of computation: the tool along with the source and sink nodes.
    """
    def __init__(self, tool, source_nodes, sink_node, alignment_node, plates):
        """
        Initialise this factor
        :param tool: The tool
        :param source_nodes: The source nodes
        :param sink_node: The sink node
        :param alignment_node: The node (possibly a clock node) that the output node will have data aligned to
        :param plates: The plates over which this factor is defined
        :type tool: Tool
        :type sink_node: Node
        """
        if not isinstance(tool, Tool):
            raise ValueError("Expected node, got {}".format(type(tool)))
        self.tool = tool
        if source_nodes:
            for source in source_nodes:
                if not isinstance(source, Node):
                    raise ValueError("Expected node, got {}".format(type(source)))
        self.sources = source_nodes
        if not isinstance(sink_node, Node):
            raise ValueError("Expected node, got {}".format(type(sink_node)))
        self.sink = sink_node

        self.plates = plates

        if alignment_node and not isinstance(alignment_node, Node):
            raise ValueError("Expected node, got {}".format(type(alignment_node)))
        if alignment_node and alignment_node.plate_ids:
            # TODO: Need to implement alignment nodes that live inside plates
            raise NotImplementedError("Currently only alignment nodes outside of plates are supported")
        self.alignment_node = alignment_node

    def execute(self, time_interval):
        if self.plates:
            for plate in self.plates:
                for pv in plate.values:
                    sources = self.get_global_sources()
                    if self.sources:
                        for source in self.sources:
                            if pv in source.streams:
                                sources.append(source.streams[pv])
                            else:
                                logging.warn("Plate {} with value {} not valid for source {}".format(plate, pv, source))
                    sink = self.sink.streams[pv]
                    self.tool.execute(sources=sources, sink=sink, interval=time_interval,
                                      alignment_stream=self.get_alignment_stream(None, None))
        else:
            # sources = [source.streams[None] for source in self.sources] if self.sources else None
            sources = self.get_global_sources()
            sink = self.sink.streams[None]
            self.tool.execute(sources=sources, sink=sink, interval=time_interval, alignment_stream=
                              self.get_alignment_stream(None, None))

    def get_global_sources(self):
        # Also add streams that live outside of the plates
        sources = []
        if self.sources:
            for source in self.sources:
                if None in source.streams:
                    sources.append(source.streams[None])
        return sources

    def get_alignment_stream(self, plate=None, plate_value=None):
        if not self.alignment_node:
            return None
        if plate is not None or plate_value is not None:
            # TODO: Need to implement alignment nodes that live inside plates
            raise NotImplementedError("Currently only alignment nodes outside of plates are supported")
        return self.alignment_node.streams[plate][plate_value]
