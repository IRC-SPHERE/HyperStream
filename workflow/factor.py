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
from ..tool import BaseTool, MultiOutputTool, AggregateTool, SelectorTool, PlateCreationTool
from plate import Plate
from ..time_interval import TimeIntervals

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
        :type alignment_node: Node | None
        :type plates: list[Plate] | tuple[Plate]
        """
        if not isinstance(tool, BaseTool):
            raise ValueError("Expected tool, got {}".format(type(tool)))
        self.tool = tool
        if source_nodes:
            for source in source_nodes:
                if not isinstance(source, Node):
                    raise ValueError("Expected node, got {}".format(type(source)))
                source.is_leaf = False
        self.sources = source_nodes
        if not isinstance(sink_node, Node):
            raise ValueError("Expected node, got {}".format(type(sink_node)))
        self.sink = sink_node
        sink_node._factor = self
        
        self.plates = plates
        
        if alignment_node and not isinstance(alignment_node, Node):
            raise ValueError("Expected node, got {}".format(type(alignment_node)))
        
        if alignment_node and alignment_node.plates:
            # TODO: Need to implement alignment nodes that live inside plates
            raise NotImplementedError("Currently only alignment nodes outside of plates are supported")
        
        self.alignment_node = alignment_node
    
    def execute(self, time_interval):
        """
        Execute the factor over the given time interval
        :param time_interval:
        :return:
        """
        if self.plates:
            if isinstance(self.tool, AggregateTool):
                if len(self.sources) != 1:
                    raise ValueError("Currently only a single source node is valid for an Aggregate Tool")
                if self.alignment_node:
                    raise ValueError("Currently an alignment node cannot be used with an Aggregate Tool")
                
                all_sources = self.sources[0]
                
                # Here we should loop through the plate values of the sink, and get the sources that are appropriate for
                # that given plate value, and pass only those sources to the tool. This is cleaner than making the tool
                # deal with all of the sources
                for pv in self.sink.plate_values:
                    sources = [all_sources.streams[s] for s in all_sources.streams if all([v in s for v in pv])]
                    sink = self.sink.streams[pv]
                    self.tool.execute(sources=sources, sink=sink, interval=time_interval, alignment_stream=None)
            elif isinstance(self.tool, SelectorTool):
                if len(self.sources) != 1:
                    raise ValueError("Currently only a single source node is valid for an Aggregate Tool")
                if self.alignment_node:
                    raise ValueError("Currently an alignment node cannot be used with an Aggregate Tool")
                
                diff, counts, is_sub_plate = self.sources[0].difference(self.sink)
                if is_sub_plate:
                    # Special case of tools that are performing sub-selection
                    self.tool.execute(sources=self.sources[0].streams.values(),
                                      sinks=self.sink.streams.values(),
                                      interval=time_interval)
                else:
                    raise ValueError("Source and sink plates do not match")
            else:
                # TODO: This loop over plates is probably not correct:
                # What we probably want is to take the cartesian product of plate values
                for plate in self.plates:
                    for pv in plate.values:
                        sources = self.get_sources(plate, pv)
                        sink = self.sink.streams[pv]
                        self.tool.execute(sources=sources, sink=sink, interval=time_interval,
                                          alignment_stream=self.get_alignment_stream(None, None))
        else:
            if isinstance(self.tool, AggregateTool):
                raise ValueError("Cannot execute an AggregateTool if no plates are defined for the factor")
            
            # sources = [source.streams[None] for source in self.sources] if self.sources else None
            sources = self.get_global_sources()
            sink = self.sink.streams[None]
            self.tool.execute(sources=sources, sink=sink, interval=time_interval, alignment_stream=
            self.get_alignment_stream(None, None))
        return self
    
    def get_sources(self, plate, plate_value, sources=None):
        """
        Gets the source streams for a given plate value on a plate.
        Also populates with source streams that are valid for the parent plates of this plate,
        with the appropriate meta-data for the parent plate.
        :param plate: The plate being operated on
        :param plate_value: The specific plate value of interest
        :param sources: The currently found sources (for recursion)
        :return: The appropriate source streams
        :type plate: Plate
        :type plate_value: tuple
        :type sources: list[Stream] | None
        """
        if sources is None:
            sources = []
        
        if self.sources:
            for si, source in enumerate(self.sources):
                if len(source.streams) == 1 and None in source.streams:
                    sources.append(source.streams[None])
                elif plate_value in source.streams:
                    sources.append(source.streams[plate_value])
                else:
                    # TODO - determine whether this should raise an exception or not
                    logging.warn("Plate {} with value {} not valid for source {}"
                                 .format(plate, plate_value, source))
        
        if not plate.is_root:
            # Populate with sources defined on parent plate
            parent_plate_value = tuple(pv for pv in plate_value if pv[0] != plate.meta_data_id)
            sources = self.get_sources(plate.parent, parent_plate_value, sources)
        
        # sources.extend(self.get_global_sources())
        
        return sources
    
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
        sink_node.factor = self
        
        # TODO: The input plate should be a parent of the output plate
        self.input_plate = input_plate
        
        if isinstance(output_plates, Plate):
            output_plates = (output_plates,)
        
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
                        # ipv = ipv + output_plate_values
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


class PlateCreationFactor(Printable):
    def __init__(self, tool, source_node, input_plate, output_plate, plate_manager):
        """
        Initialise this factor
        :param tool: The tool
        :param source_node: The source node
        :param input_plate: The plate over which this factor is defined
        :param output_plate: The details of the plate that will be created (dict)
        :param plate_manager: The hyperstream plate manager
        :type tool: PlateOutputTool
        :type input_plate: Plate | None
        :type output_plate: dict
        :type plate_manager: PlateManager
        """
        if not isinstance(tool, PlateCreationTool):
            raise ValueError("Expected tool, got {}".format(type(tool)))
        self.tool = tool

        if not isinstance(source_node, Node):
            raise ValueError("Expected node, got {}".format(type(source_node)))

        if input_plate is not None and not isinstance(input_plate, Plate):
            raise ValueError("Expected plate, got {}".format(type(source_node)))

        self.source = source_node
        self.sink = None
        self.input_plate = input_plate
        self.output_plate = output_plate
        self._plate_manager = plate_manager
        self._meta_data_manager = plate_manager.meta_data_manager

    def execute(self, time_interval):
        """
        Execute the factor over the given time interval. Note that this is normally done by the workspace,
        but can also be done on the factor directly
        :param time_interval: The time interval
        :return: self (for chaining)
        """
        # Execute the tool to produce the output plate values
        output_plate_values = {}
        if self.input_plate:
            for ipv in self.input_plate.values:
                if ipv in self.source.streams:
                    source = self.source.streams[ipv]
                else:
                    logging.warn("Plate {} with value {} not valid for source {}".format(
                        self.input_plate, ipv, self.source))
                    continue

                output_plate_values[ipv] = self.tool.execute(
                    source=source, interval=time_interval, input_plate_value=ipv)

        # Ensure that the output plate values exist
        for ipv, opv in output_plate_values.items():
            input_plate_value = ipv[-1][1]
            for pv in opv:
                identifier = input_plate_value + "." + self.output_plate["meta_data_id"] + "_" + pv
                if not self._meta_data_manager.contains(identifier):
                    self._meta_data_manager.insert(
                        tag=self.output_plate["meta_data_id"],
                        identifier=identifier,
                        parent=input_plate_value,
                        data=pv
                    )

        if self.output_plate["use_provided_values"]:
            raise NotImplementedError("Currently only support using empty set and complement=True for the new plate")

        if self.output_plate["plate_id"] not in self._plate_manager.plates:
            # Create the output plate
            self._plate_manager.create_plate(
                plate_id=self.output_plate["plate_id"],
                description=self.output_plate["description"],
                meta_data_id=self.output_plate["meta_data_id"],
                values=[],
                complement=True,
                parent_plate=self.input_plate.plate_id
            )

            logging.info("Plate with ID {} created".format(self.output_plate["plate_id"]))

        return self
