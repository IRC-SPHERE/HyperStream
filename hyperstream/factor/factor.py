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

import logging
import itertools

from ..node import Node
from ..plate import Plate
from ..time_interval import TimeIntervals
from ..tool import BaseTool, MultiOutputTool, AggregateTool, SelectorTool, PlateCreationTool
from ..utils import Printable, IncompatibleToolError, IncompatiblePlatesError


class FactorBase(Printable):
    def __init__(self, tool):
        self.tool = tool

    @property
    def factor_id(self):
        return "{}(tool={})".format(self.__class__.__name__, self.tool)


class Factor(FactorBase):
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

        if isinstance(tool, MultiOutputTool):
            raise IncompatibleToolError("Use MultiOutputFactor for MultiOutputTool")

        if isinstance(tool, PlateCreationTool):
            raise IncompatibleToolError("Use NodeCreationFactor for PlateCreationTool")

        super(Factor, self).__init__(tool)

        if source_nodes:
            for source in source_nodes:
                if not isinstance(source, Node):
                    raise ValueError("Expected node, got {}".format(type(source)))
                source.is_leaf = False
        self.sources = source_nodes if source_nodes else []
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
        logging.info('{} with sink node {} running from {} to {}'.format(
            self.tool.__class__.__name__, self.sink.node_id, time_interval.start, time_interval.end))

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
                if len(self.sources) == 1:
                    sources = self.sources[0].streams.values()
                elif len(self.sources) == 2:
                    selector_node = self.sources[0]
                    if len(selector_node.streams) != 1:
                        raise ValueError("Selector node should only have one stream")
                    sources = [self.sources[0].streams[None], self.sources[1].streams.values()]
                else:
                    raise ValueError("Currently only one or twos source nodes are valid for a Selector Tool")
                if self.alignment_node:
                    raise ValueError("Currently an alignment node cannot be used with a Selector Tool")
                
                diff, counts, is_sub_plate = self.sources[-1].difference(self.sink)
                # TODO: This sub-plate selection is deprecated

                if (counts == [1, 1] and is_sub_plate) or \
                    (len(self.sink.plates)==1 and counts == [1, 0] and is_sub_plate) or \
                    (next(p.is_root for p in self.sources[-1].plates)
                     and len(self.sink.plates) == 1
                     and self.sink.plates[0] in self.sources[-1].plates):
                        # Special case of tools that are performing sub-selection
                        self.tool.execute(sources=sources,
                                          sinks=self.sink.streams.values(),
                                          interval=time_interval)
                else:
                    raise ValueError("Source and sink plates do not match within a Selector Tool")
            else:
                # TODO: This loop over plates is probably not correct:
                # What we probably want is to take the cartesian product of plate values
                if len(self.plates) == 1:
                    plate = self.plates[0]
                    for pv in plate.values:
                        sources = self.get_sources(plate, pv)
                        sink = self.sink.streams[pv]
                        self.tool.execute(sources=sources, sink=sink, interval=time_interval,
                                          alignment_stream=self.get_alignment_stream(None, None))
                else:
                    if len(self.sources) != 1 and not all(s.plates == self.plates for s in self.sources):
                        source_plates = sorted(p.plate_id for s in self.sources for p in s.plates)
                        self_plates = sorted(p.plate_id for p in self.plates)
                        if source_plates == self_plates:
                            # This is the case where the sources are all on separate plates and the sink is the
                            # combination
                            for pv in sorted(itertools.product(*([x[0] for x in p.values] for p in self.plates))):
                                # Here we're selecting the streams that have the partial match of the plate value
                                sources = [source.streams[s] for source in self.sources
                                           for s in source.streams if (s[0] in pv)]
                                sink = self.sink.streams[pv]
                                self.tool.execute(sources=sources, sink=sink, interval=time_interval,
                                                  alignment_stream=self.get_alignment_stream(None, None))
                        else:
                            raise NotImplementedError
                    for pv in Plate.get_overlapping_values(self.plates):
                        sources = [source.streams[s] for source in self.sources for s in source.streams if pv == s]
                        sink = self.sink.streams[pv]
                        self.tool.execute(sources=sources, sink=sink, interval=time_interval,
                                          alignment_stream=self.get_alignment_stream(None, None))
        else:
            if isinstance(self.tool, AggregateTool):
                raise ValueError("Cannot execute an AggregateTool if no plates are defined for the factor")
            
            # sources = [source.streams[None] for source in self.sources] if self.sources else None
            sources = self.get_global_sources()
            sink = self.sink.streams[None]
            self.tool.execute(sources=sources, sink=sink, interval=time_interval,
                              alignment_stream=self.get_alignment_stream(None, None))
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
                    # # TODO - determine whether this should raise an exception or not, or even log a warning
                    # logging.warn("{} with value {} not valid for source {}"
                    #              .format(plate, plate_value, source))
                    pass
        
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


class MultiOutputFactor(FactorBase):
    """
    A multi-output factor in the graph.
    As with a factor, this links source nodes to sink nodes. However in this case the source node is being split onto
    multiple plates.
    Note there is no concept of an alignment node here.
    """
    
    def __init__(self, tool, source_node, splitting_node, sink_node, input_plate, output_plates):
        """
        Initialise this factor

        :param tool: The tool
        :param source_node: The source node
        :param splitting_node: The node over which to split
        :param sink_node: The sink nodes
        :param input_plate: The plate over which this factor is defined
        :param output_plates: The plates, the last of which will be the new one created
        :type tool: MultiOutputTool
        :type sink_node: Node
        :type source_node: Node | None
        :type input_plate: Plate | None
        :type output_plates: list[Plate] | tuple[Plate] | Plate
        """
        if not isinstance(tool, MultiOutputTool):
            raise ValueError("Expected tool, got {}".format(type(tool)))

        super(MultiOutputFactor, self).__init__(tool)

        if source_node and not isinstance(source_node, Node):
            raise ValueError("Expected node, got {}".format(type(source_node)))
        self.source = source_node

        if source_node:
            source_node.is_leaf = False

        if splitting_node and not isinstance(splitting_node, Node):
            raise ValueError("Expected node, got {}".format(type(splitting_node)))
        self.splitting_node = splitting_node

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
        Execute the factor over the given time interval. Note that this is normally done by the workflow,
        but can also be done on the factor directly

        :param time_interval: The time interval
        :return: self (for chaining)
        """
        logging.info('{} running from {} to {}'.format(
            self.tool.__class__.__name__, time_interval.start, time_interval.end))

        input_plate_values = self.input_plate.values if self.input_plate else [None]
        output_plate_values = self.sink.plate_values
        meta_data_ids = [p.meta_data_id for p in self.sink.plates]

        def belongs(plate_value):
            return all(ii in plate_value for ii in input_plate_value)

        def meta_data_matches(plate_value):
            return filter(lambda x: x[0] in meta_data_ids, plate_value)

        for input_plate_value in input_plate_values:
            if input_plate_value:
                # Select only the valid output plate values based on the input plate value
                # filtered = filter(belongs, output_plate_values)
                filtered = filter(belongs, output_plate_values)
            else:
                filtered = output_plate_values

            sinks = [self.sink.streams[s] for s in filtered]

            sub_plate_values_only = map(meta_data_matches, filtered)

            if not self.source:
                source = None
            elif input_plate_value in self.source.streams:
                source = self.source.streams[input_plate_value]
            else:
                logging.warn("{} with value {} not valid for source {}".format(
                    self.input_plate, input_plate_value, self.source))
                continue

            if self.input_plate:
                if len(self.output_plates) == 1:
                    if self.output_plates[0].parent.plate_id != self.input_plate.plate_id:
                        raise IncompatiblePlatesError("Parent plate of output plate does not match input plate")
            else:
                if len(self.output_plates) != 1:
                    raise ValueError("Should be a single output plate if there is no input plate")

            if len(self.output_plates) > 1:
                raise NotImplementedError

            splitting_stream = self.get_splitting_stream(input_plate_value)

            self.tool.execute(
                source=source,
                sinks=sinks,
                interval=time_interval,
                splitting_stream=splitting_stream,
                meta_data_id=self.output_plates[0].meta_data_id,
                output_plate_values=sub_plate_values_only)
            self.update_computed_intervals(sinks, time_interval)

        return self

    def get_splitting_stream(self, input_plate_value):
        """
        Get the splitting stream

        :param input_plate_value: The input plate value
        :return: The splitting stream
        """
        if not self.splitting_node:
            return None

        if len(self.splitting_node.plates) == 0:
            # Use global plate value
            return self.splitting_node.streams[None]

        if len(self.splitting_node.plates) > 1:
            raise ValueError("Splitting node cannot live on multiple plates for factor {}"
                             .format(self.factor_id))

        # now len(self.splitting_node.plates) == 1:
        if not self.input_plate and len(self.splitting_node.plates) > 0:
            raise ValueError("Splitting node cannot live on a plate if there is no input plate")

        splitting_plate = self.splitting_node.plates[0]
        if self.input_plate == splitting_plate:
            # Use matching plate value
            splitting_stream = self.splitting_node.streams[input_plate_value]
        else:
            # First check if it's a direct child
            if splitting_plate.is_child(self.input_plate):
                ppv = filter(lambda x: all(p in input_plate_value for p in x), self.input_plate.parent.values)
                if len(ppv) != 1:
                    raise ValueError("Parent plate value not found")
                splitting_stream = self.splitting_node.streams[ppv[0]]
            # Then more generally if it's a descendant
            elif splitting_plate.is_descendant(self.input_plate):
                # Here we need to find the splitting plate value that is valid for the
                # current input plate value
                # TODO: This needs checking - is the logic still the same as for the case above?
                ppv = filter(lambda x: all(p in input_plate_value for p in x), self.input_plate.parent.values)
                if len(ppv) != 1:
                    raise ValueError("Parent plate value not found")
                splitting_stream = self.splitting_node.streams[ppv]
            else:
                raise IncompatiblePlatesError(
                    "Splitting node plate {} does not match input plate {} for factor {}"
                    .format(self.input_plate, self.splitting_node.plates[0], self.factor_id))
        return splitting_stream

    @staticmethod
    def update_computed_intervals(sinks, time_interval):
        """
        Update computed intervals

        :param sinks: The streams to update
        :param time_interval: The time interval
        """
        for sink in sinks:
            sink.calculated_intervals += time_interval
            required_intervals = TimeIntervals([time_interval]) - sink.calculated_intervals
            if not required_intervals.is_empty:
                raise RuntimeError('Tool execution did not cover the time interval {}'
                                   .format(required_intervals))

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


class NodeCreationFactor(FactorBase):
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

        super(NodeCreationFactor, self).__init__(tool)

        if source_node is not None and not isinstance(source_node, Node):
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
        Execute the factor over the given time interval. Note that this is normally done by the workflow,
        but can also be done on the factor directly

        :param time_interval: The time interval
        :return: self (for chaining)
        """
        logging.info('{} running from {} to {}'.format(
            self.tool.__class__.__name__, time_interval.start, time_interval.end))

        # Execute the tool to produce the output plate values
        output_plate_values = {}
        if self.input_plate:
            for ipv in self.input_plate.values:
                if ipv in self.source.streams:
                    source = self.source.streams[ipv]
                else:
                    logging.warn("{} with value {} not valid for source {}".format(
                        self.input_plate, ipv, self.source))
                    continue

                output_plate_values[ipv] = self.tool.execute(
                    source=source, interval=time_interval, input_plate_value=ipv)
        else:
            source = self.source.streams[None] if self.source else None
            if "parent_plate" in self.output_plate:
                # Get the parent plate values
                parent_plate = self._plate_manager.plates[self.output_plate["parent_plate"]]
                for ppv in parent_plate.values:
                    output_plate_values[ppv] = self.tool.execute(
                        source=source, interval=time_interval, input_plate_value=ppv)
            else:
                output_plate_values[None] = self.tool.execute(
                    source=source, interval=time_interval, input_plate_value=None)

        # Ensure that the output plate values exist
        for ipv, opv in output_plate_values.items():
            input_plate_value = ".".join("_".join(i) for i in ipv) if ipv else None
            for pv in opv:
                if ipv:
                    identifier = input_plate_value + "." + self.output_plate["meta_data_id"] + "_" + pv
                else:
                    identifier = self.output_plate["meta_data_id"] + "_" + pv
                if not self._meta_data_manager.contains(identifier):
                    self._meta_data_manager.insert(
                        tag=self.output_plate["meta_data_id"],
                        identifier=identifier,
                        parent=input_plate_value if ipv else "root",
                        data=pv
                    )

        if self.output_plate["use_provided_values"]:
            raise NotImplementedError("Currently only support using empty set and complement=True for the new plate")

        if self.output_plate["plate_id"] not in self._plate_manager.plates:
            # Create the output plate
            if self.input_plate:
                parent_plate = self.input_plate.plate_id
            else:
                if "parent_plate" in self.output_plate:
                    parent_plate = self.output_plate['parent_plate']
                else:
                    parent_plate = None
            self._plate_manager.create_plate(
                plate_id=self.output_plate["plate_id"],
                description=self.output_plate["description"],
                meta_data_id=self.output_plate["meta_data_id"],
                values=[],
                complement=True,
                parent_plate=parent_plate
            )

            logging.info("Plate with ID {} created".format(self.output_plate["plate_id"]))

        return self
