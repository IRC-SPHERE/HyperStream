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
"""
Workflow and WorkflowManager definitions.
"""

import itertools
import logging

from factor import Factor, MultiOutputFactor
from node import Node, get_overlapping_plate_values
from ..stream import StreamId
from ..tool import BaseTool, Tool, MultiOutputTool, AggregateTool, SelectorTool
from ..utils import Printable, TypedFrozenKeyDict, IncompatiblePlatesError, FactorDefinitionError, NodeDefinitionError


class Workflow(Printable):
    """
    Workflow.
    This defines the graph of operations through "nodes" and "factors".
    """
    def __init__(self, channels, plates, workflow_id, name, description, owner):
        """
        Initialise the workflow

        :param channels: The channels used by this workflow
        :param plates: All of the plates used by this workflow
        :param workflow_id: The workflow id
        :param name: The name of the workflow
        :param description: A human readable description
        :param owner: The owner/author of the workflow
        """
        self.channels = channels
        self.plates = plates
        self.workflow_id = workflow_id
        self.name = name
        self.description = description
        self.owner = owner
        self.nodes = {}
        self.execution_order = []
        self.factor_collections = TypedFrozenKeyDict(str)

        logging.info("New workflow created with id {}".format(workflow_id))
    
    def execute(self, time_interval):
        """
        Here we execute the factors over the streams in the workflow

        :return: None
        """
        # TODO: Currently expects the factors to be declared sequentially
        # for tool in self.execution_order:
        #     for factor in self.factor_collections[tool.name]:
        #         logging.debug("Executing factor {}".format(factor))
        #         factor.execute(time_interval)
        if len(self.execution_order) > 1:
            self.factor_collections[self.execution_order[-1].name][-1].execute(time_interval)
            
    def create_node(self, stream_name, channel, plate_ids):
        """
        Create a node in the graph. Note: assumes that the streams already exist

        :param stream_name: The name of the stream
        :param channel: The channel where this stream lives
        :param plate_ids: The plate ids. The stream meta-data will be auto-generated from these
        :return: The streams associated with this node
        """
        streams = {}

        plates = [self.plates[p] for p in plate_ids] if plate_ids else None

        plate_values = get_overlapping_plate_values(plates)

        if plate_ids:
            if not plate_values:
                raise NodeDefinitionError("No overlapping plate values found")
            for pv in plate_values:
                # Construct stream id
                stream_id = StreamId(name=stream_name, meta_data=pv)

                # Now try to locate the stream and add it (raises StreamNotFoundError if not found)
                streams[pv] = channel.get_or_create_stream(stream_id=stream_id)

        else:
            streams[None] = channel.get_or_create_stream(stream_id=StreamId(name=stream_name))

        if not streams:
            raise NodeDefinitionError("No streams created for node with id {}".format(stream_name))

        node = Node(stream_name, streams, plates)
        self.nodes[stream_name] = node
        logging.info("Added node with id {}".format(stream_name))

        return node

    def create_factor(self, tool, sources, sink, alignment_node=None):
        """
        Creates a factor. Instantiates a single tool for all of the plates, and connects the source and sink nodes with
        that tool.

        Note that the tool parameters these are currently fixed over a plate. For parameters that vary over a plate,
        an extra input stream should be used

        :param alignment_node:
        :param tool: The tool to use. This is either an instantiated Tool object or a dict with "name" and "parameters"
        :param sources: The source nodes
        :param sink: The sink node
        :return: The factor object
        :type tool: Tool | dict
        :type sources: list[Node] | tuple[Node] | None
        :type sink: Node
        :type alignment_node: Node | None
        :rtype: Factor
        """
        if isinstance(tool, dict):
            tool = self.channels.get_tool(**tool)

        if not isinstance(tool, BaseTool):
            raise ValueError("Expected Tool, got {}".format(type(tool)))

        if sink.plates:
            if isinstance(tool, (AggregateTool, SelectorTool)):
                if not sources or len(sources) != 1:
                    raise FactorDefinitionError("Aggregate tools require a single source node")

                if not sources[0].plates:
                    raise FactorDefinitionError("Aggregate tool source must live on a plate")

                if len(sources[0].plates) != 1:
                    # Make sure that there are exactly two plates that don't match: one from each side
                    diff, counts, is_sub_plate = sources[0].difference(sink)
                    if counts == [1, 1]:
                        if not is_sub_plate:
                            raise IncompatiblePlatesError("Sink plate is not a simplification of source plate")
                    else:
                        raise IncompatiblePlatesError("Sink plate is not a simplification of source plate")
                else:
                    # Check if the parent plate is valid instead
                    source_plate = sources[0].plates[0]
                    sink_plate = sink.plates[0]

                    error = self.check_plate_compatibility(tool, source_plate, sink_plate)
                    if error is not None:
                        raise IncompatiblePlatesError(error)
            else:
                if sources:
                    # Check that the plates are compatible
                    source_plates = itertools.chain(*(source.plate_ids for source in sources))
                    for p in sink.plate_ids:
                        if p not in set(source_plates):
                            raise IncompatiblePlatesError("{} not in source plates".format(p))
                    for p in source_plates:
                        if p not in set(sink.plate_ids):
                            raise IncompatiblePlatesError("{} not in sink plates".format(p))
            plates = [self.plates[plate_id] for plate_id in sink.plate_ids]
        else:
            plates = None

        factor = Factor(tool=tool, source_nodes=sources,
                        sink_node=sink, alignment_node=alignment_node,
                        plates=plates)

        if tool.name not in self.factor_collections:
            self.factor_collections[tool.name] = []

        self.factor_collections[tool.name].append(factor)
        self.execution_order.append(tool)

        return factor

    def create_multi_output_factor(self, tool, source, sink):
        """
        Creates a multi-output factor.
        This takes a single node, applies a MultiOutputTool to create multiple nodes on a new plate
        Instantiates a single tool for all of the input plate values,
        and connects the source and sink nodes with that tool.

        Note that the tool parameters these are currently fixed over a plate. For parameters that vary over a plate,
        an extra input stream should be used

        :param tool: The tool to use. This is either an instantiated Tool object or a dict with "name" and "parameters"
        :param source: The source node
        :param sink: The sink node
        :return: The factor object
        :type tool: MultiOutputTool | dict
        :type source: Node
        :type sink: Node
        :rtype: Factor
        """
        if isinstance(tool, dict):
            tool = self.channels.get_tool(name=tool["name"], parameters=["parameters"])

        if not isinstance(tool, MultiOutputTool):
            raise ValueError("Expected MultiOutputTool, got {}".format(type(tool)))

        # Check that the input_plate are compatible - note this is the opposite way round to a normal factor
        input_plates = [self.plates[plate_id] for plate_id in source.plate_ids]
        output_plates = [self.plates[plate_id] for plate_id in sink.plate_ids]

        if len(input_plates) > 1:
            raise NotImplementedError

        if len(output_plates) == 0:
            raise ValueError("No output plate found")

        if len(output_plates) == 1:
            if not self.check_multi_output_plate_compatibility(input_plates, output_plates[0]):
                raise IncompatiblePlatesError("Parent plate does not match input plate")

            factor = MultiOutputFactor(tool=tool, source_node=source, sink_node=sink,
                                       input_plate=input_plates[0], output_plates=output_plates[0])
        else:
            # The output plates should be the same as the input plates, except for one
            # additional plate. Since we're currently only supporting one input plate,
            # we can safely assume that there is a single matching plate.
            # Finally, note that the output plate must either have no parents
            # (i.e. it is at the root of the tree), or the parent plate is somewhere
            # in the input plate's ancestry
            if len(output_plates) > 2:
                raise NotImplementedError
            if len(input_plates) != 1:
                raise IncompatiblePlatesError("Require an input plate to match all but one of the output plates")
            if output_plates[0] == input_plates[0]:
                # Found a match, so the output plate should be the other plate
                output_plate = output_plates[1]
            else:
                if output_plates[1].plate_id != input_plates[0].plate_id:
                    raise IncompatiblePlatesError("Require an input plate to match all but one of the output plates")
                output_plate = output_plates[0]
                # Swap them round so the new plate is the last plate - this is required by the factor
                output_plates[1], output_plates[0] = output_plates[0], output_plates[1]

            # We need to walk up the input plate's parent tree
            match = False
            parent = input_plates[0].parent
            while parent is not None:
                if parent.plate_id == output_plate.parent.plate_id:
                    match = True
                    break
                parent = parent.parent
            if not match:
                raise IncompatiblePlatesError("Require an input plate to match all but one of the output plates")

            factor = MultiOutputFactor(
                tool=tool, source_node=source, sink_node=sink,
                input_plate=input_plates[0], output_plates=output_plates)

        if tool.name not in self.factor_collections:
            self.factor_collections[tool.name] = []

        self.factor_collections[tool.name].append(factor)
        self.execution_order.append(tool)

        return factor

    @staticmethod
    def check_plate_compatibility(tool, source_plate, sink_plate):
        """
        Checks whether the source and sink plate are compatible given the tool

        :param tool: The tool
        :param source_plate: The source plate
        :param sink_plate: The sink plate
        :return: Either an error, or None
        :type tool: Tool
        :type source_plate: Plate
        :type sink_plate: Plate
        :rtype: None | str
        """
        if sink_plate == source_plate.parent:
            return None

        # could be that they have the same meta data, but the sink plate is a simplification of the source
        # plate (e.g. when using IndexOf tool)
        if sink_plate.meta_data_id == source_plate.meta_data_id:
            if sink_plate.is_sub_plate(source_plate):
                return None
            return "Sink plate {} is not a simplification of source plate {}".format(
                sink_plate.plate_id, source_plate.plate_id)

        # Also check to see if the meta data differs by only one value
        meta_data_diff = set(source_plate.ancestor_meta_data_ids) - set(sink_plate.ancestor_meta_data_ids)
        if len(meta_data_diff) == 1:
            # Is the diff value the same as the aggregation meta id passed to the aggregate tool
            if tool.aggregation_meta_data not in meta_data_diff:
                return "Aggregate tool meta data ({}) " \
                       "does not match the diff between source and sink plates ({})".format(
                        tool.aggregation_meta_data, list(meta_data_diff)[0])
        else:
            return "{} not in source's parent plates".format(sink_plate.plate_id)

    @staticmethod
    def check_multi_output_plate_compatibility(source_plates, sink_plate):
        if len(source_plates) == 0:
            if sink_plate.parent is not None:
                return False
        else:
            if sink_plate.parent is None:
                return False
            else:
                if sink_plate.parent.plate_id != source_plates[0].plate_id:
                    return False
        return True
