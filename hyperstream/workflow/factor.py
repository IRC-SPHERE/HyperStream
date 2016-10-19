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
from ..tool import Tool, MultiOutputTool
from plate import Plate
from ..time_interval import TimeIntervals

import logging
import itertools


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
        :type alignment_node: Node | None
        :type plates: list[Plate] | tuple[Plate]
        """
        if not isinstance(tool, Tool):
            raise ValueError("Expected tool, got {}".format(type(tool)))
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
        return self

    def get_global_sources(self):
        """
        Gets streams that live outside of the plates
        :return: Global streams
        """
        sources = []
        if self.sources:
            for source in self.sources:
                if None in source.streams:
                    sources.append(source.streams[None])
        return sources

    def get_alignment_stream(self, plate=None, plate_value=None):
        """
        Gets the alignment stream for a particular plate value
        :param plate: The plate on which the alignment node lives
        :param plate_value: The plate value to select the stream from the node
        :return: The alignment stream
        """
        if not self.alignment_node:
            return None
        if plate is not None or plate_value is not None:
            # TODO: Need to implement alignment nodes that live inside plates
            raise NotImplementedError("Currently only alignment nodes outside of plates are supported")
        return self.alignment_node.streams[plate]


class MultiOutputFactor(Printable):
    """
    A multi-output factor in the graph.
    As with a factor, this links source nodes to sink nodes. However in this case the source node is being split onto
    multiple plates.
    Note there is no concept of an alignment node here.
    """
    def __init__(self, tool, source_node, sink_node, input_plate, output_plates):
        """
        Initialise this factor
        :param tool: The tool
        :param source_node: The source node
        :param sink_node: The sink nodes
        :param input_plate: The plate over which this factor is defined
        :param output_plates: The plates, the last of which will be the new one created
        :type tool: MultiOutputTool
        :type sink_node: Node
        :type input_plate: Plate | None
        :type output_plates: list[Plate] | tuple[Plate] | Plate
        """
        if not isinstance(tool, MultiOutputTool):
            raise ValueError("Expected tool, got {}".format(type(tool)))
        self.tool = tool

        if not isinstance(source_node, Node):
            raise ValueError("Expected node, got {}".format(type(source_node)))
        self.source = source_node

        if not isinstance(sink_node, Node):
            raise ValueError("Expected node, got {}".format(type(sink_node)))
        self.sink = sink_node

        # TODO: The input plate should be a parent of the output plate
        self.input_plate = input_plate

        if isinstance(output_plates, Plate):
            output_plates = (output_plates, )

        self.output_plates = output_plates

    def execute(self, time_interval):
        """
        Execute the factor over the given time interval. Note that this is normally done by the workspace,
        but can also be done on the factor directly
        :param time_interval: The time interval
        :return: self (for chaining)
        """
        output_plate_values = self.sink.plate_values
        sinks = [self.sink.streams[opv] for opv in output_plate_values]

        if self.input_plate:
            for ipv in self.input_plate.values:
                if ipv in self.source.streams:
                    source = self.source.streams[ipv]
                else:
                    logging.warn("Plate {} with value {} not valid for source {}".format(
                        self.input_plate, ipv, self.source))
                    continue

                if len(self.output_plates) == 1:
                    if self.output_plates[0].parent.plate_id == self.input_plate.plate_id:
                        pass
                        # raise NotImplementedError
                    else:
                        # ipv = output_plate_values
                        raise NotImplementedError

                self.tool.execute(source=source, sinks=sinks, interval=time_interval,
                                  input_plate_value=ipv, output_plate=self.output_plates[-1])
        else:
            sources = self.source.streams[None]
            if len(self.output_plates) != 1:
                raise ValueError("Should be a single output plate if there is no input plate")
            self.tool.execute(source=sources, sinks=sinks, interval=time_interval,
                              input_plate_value=None, output_plate=self.output_plates[0])

        # Update computed intervals
        for sink in sinks:
            sink.calculated_intervals += TimeIntervals([time_interval])
            required_intervals = TimeIntervals([time_interval]) - sink.calculated_intervals
            if not required_intervals.is_empty:
                raise RuntimeError('Tool execution did not cover the time interval {}.'.format(required_intervals))
        return self

    def get_alignment_stream(self, plate=None, plate_value=None):
        """
        Gets the alignment stream for a particular plate value
        :param plate: The plate on which the alignment node lives
        :param plate_value: The plate value to select the stream from the node
        :return: The alignment stream
        """
        if not self.alignment_node:
            return None
        if plate is not None or plate_value is not None:
            # TODO: Need to implement alignment nodes that live inside plates
            raise NotImplementedError("Currently only alignment nodes outside of plates are supported")
        return self.alignment_node.streams[plate]
