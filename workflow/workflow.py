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
from node import Node
from ..stream import StreamId
from ..tool import Tool, MultiOutputTool, AggregateTool
from ..utils import Printable, TypedFrozenKeyDict, IncompatiblePlatesError, FactorDefinitionError


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
        # for factor_collection in self.factor_collections.values()[::-1]:
        #     for factor in factor_collection:
        for tool in self.execution_order:
            for factor in self.factor_collections[tool.name]:
                logging.debug("Executing factor {}".format(factor))
                factor.execute(time_interval)
            
    def create_node(self, stream_name, channel, plate_ids):
        """
        Create a node in the graph. Note: assumes that the streams already exist
        :param stream_name: The name of the stream
        :param channel: The channel where this stream lives
        :param plate_ids: The plate ids. The stream meta-data will be auto-generated from these
        :return: The streams associated with this node
        """
        streams = {}

        plate_values = self.get_overlapping_plate_values(plate_ids)

        if plate_ids:
            for pv in plate_values:
                # Construct stream id
                stream_id = StreamId(name=stream_name, meta_data=pv)

                # Now try to locate the stream and add it (raises StreamNotFoundError if not found)
                # streams.append(self.channels.get_stream(stream_id))
                streams[pv] = channel.get_or_create_stream(stream_id=stream_id)

        else:
            streams[None] = channel.get_or_create_stream(stream_id=StreamId(name=stream_name))

        node = Node(stream_name, streams, plate_ids, plate_values)
        self.nodes[stream_name] = node
        logging.info("Added node with id {}".format(stream_name))

        return node

    def get_overlapping_plate_values(self, plate_ids):
        """
        Need to find where in the tree the two plates intersect, e.g.

        We are given as input plates D, E, whose positions in the tree are:

        root -> A -> B -> C -> D
        root -> A -> B -> E

        The results should then be the cartesian product between C, D, E looped over A and B

        If there's a shared plate in the hierarchy, we need to join on this shared plate, e.g.:

        [self.plates[p].values for p in plate_ids][0] =
          [(('house', '1'), ('location', 'hallway'), ('wearable', 'A')),
           (('house', '1'), ('location', 'kitchen'), ('wearable', 'A'))]
        [self.plates[p].values for p in plate_ids][1] =
          [(('house', '1'), ('scripted', '15')),
           (('house', '1'), ('scripted', '13'))]

        Result should be one stream for each of:
          [(('house', '1'), ('location', 'hallway'), ('wearable', 'A'), ('scripted', '15)),
           (('house', '1'), ('location', 'hallway'), ('wearable', 'A'), ('scripted', '13)),
           (('house', '1'), ('location', 'kitchen'), ('wearable', 'A'), ('scripted', '15)),
           (('house', '1'), ('location', 'kitchen'), ('wearable', 'A'), ('scripted', '13))]

        :param plate_ids:
        :return: The plate values
        :type plate_ids: list[str] | list[unicode]
        """
        if not plate_ids:
            return None

        if len(plate_ids) == 1:
            return self.plates[plate_ids[0]].values

        if len(plate_ids) > 2:
            raise NotImplementedError

        # Need to walk up the tree from each plate to find a matching position (possibly the root)
        s0 = set(self.plates[plate_ids[0]].ancestor_plate_ids)
        s1 = self.plates[plate_ids[1]].ancestor_plate_ids
        intersection = list(s0.intersection(s1))
        difference = list(s0.symmetric_difference(s1))

        if len(intersection) == 0:
            # No match, so we simply take the cartesian product
            return self.cartesian_product(plate_ids)

        for i, p in enumerate(intersection):
            for plate_id in plate_ids:
                # First of all check that the intersection is at the beginning of the lists,
                # otherwise something has gone wrong
                if p != self.plates[plate_id].ancestor_plate_ids[i]:
                    raise ValueError("Error in plate specification. Intersection between plates is not valid.")

        # Now we need the cartesian product of all of the intersection values
        intersection_plate_values = self.cartesian_product(intersection)

        # The expected cardinality will be the length of the intersection values,
        # plus the sum of the difference values
        intersection_cardinality = len(intersection_plate_values)
        difference_cardinality = sum(map(lambda x: self.plates[x].cardinality - intersection_cardinality, plate_ids))
        expected_cardinality = difference_cardinality + intersection_cardinality

        plate_values = set()

        for ipv in intersection_plate_values:
            # We want to create the cartesian product for the plate values that are valid for this intersected
            # plate value.
            for d in difference:
                for v in self.plates[d].values:
                    if all(vv in v for vv in ipv):
                        # Here we have a plate value on the difference that is also on the intersection
                        # Next we get the complement of this
                        separate_values = [self.plates[dd].values for dd in set(difference).difference([d])]
                        difference_values = list(x for x in itertools.chain.from_iterable(separate_values)
                                                 if len(x) == difference_cardinality)
                        for dv in difference_values:
                            if all(vv in dv for vv in ipv):
                                # This complement is also on the intersection, now we can add a plate value
                                plate_value = sorted(set(itertools.chain.from_iterable(itertools.product(v, dv))))
                                if len(set(x[0] for x in plate_value)) != expected_cardinality:
                                    # raise ValueError("Incorrect cardinality")
                                    continue
                                plate_values.add(tuple(plate_value))

        return tuple(plate_values)

    def cartesian_product(self, plate_ids):
        """
        Gets the plate values that are the cartesian product of the plate values of the lists of plates given
        :param plate_ids: The list of plate ids
        :return: The plate values
        :type plate_ids: list[str] | list[unicode] | set[str] | set[unicode]
        """
        return [tuple(itertools.chain(*pv)) for pv in
                itertools.product(*(sorted(set(self.plates[p].values)) for p in plate_ids))]

    def create_factor(self, tool, sources, sink, alignment_node=None):
        """
        Creates a factor. Instantiates a single tool for all of the plates, and connects the source and sink nodes with
        that tool.
        :param alignment_node:
        :param tool: The tool to use. This is either an instantiated Tool object or a dict with "name" and "parameters"
        Note that the tool parameters these are currently fixed over a plate. For parameters that vary over a plate,
        an extra input stream should be used
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

        if not isinstance(tool, Tool):
            raise ValueError("Expected Tool, got {}".format(type(tool)))

        if sink.plate_ids:
            if isinstance(tool, AggregateTool):
                if not sources or len(sources) != 1:
                    raise FactorDefinitionError("Aggregate tools require a single source node")

                if not sources[0].plate_ids or len(sources[0].plate_ids) != 1:
                    raise FactorDefinitionError("Aggregate tools require source node to live on a single plate")

                if len(sink.plate_ids) != 1:
                    raise FactorDefinitionError("Aggregate tools require sink node to live on a single plate")

                # Check if the parent plate is valid instead
                source_plate = self.plates[sources[0].plate_ids[0]]
                sink_plate = self.plates[sink.plate_ids[0]]

                if sink_plate != source_plate.parent:
                    # Also check to see if the meta data differs by only one value
                    meta_data_diff = set(source_plate.ancestor_meta_data_ids) - set(sink_plate.ancestor_meta_data_ids)
                    if len(meta_data_diff) == 1:
                        # Is the diff value the same as the aggregation meta id passed to the aggregate tool
                        if tool.aggregation_meta_data not in meta_data_diff:
                            raise IncompatiblePlatesError(
                                "Aggregate tool meta data ({}) does not match the diff "
                                "between source and sink plates ({})".format(
                                    tool.aggregation_meta_data, list(meta_data_diff)[0]))
                    else:
                        raise IncompatiblePlatesError("{} not in source's parent plates".format(sink_plate))
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
        :param tool: The tool to use. This is either an instantiated Tool object or a dict with "name" and "parameters"
        Note that the tool parameters these are currently fixed over a plate. For parameters that vary over a plate,
        an extra input stream should be used
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
            if not self.check_plate_compatibility(input_plates, output_plates[0]):
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
    def check_plate_compatibility(input_plates, output_plate):
        if len(input_plates) == 0:
            if output_plate.parent is not None:
                return False
        else:
            if output_plate.parent is None:
                return False
            else:
                if output_plate.parent.plate_id != input_plates[0].plate_id:
                    return False
        return True
