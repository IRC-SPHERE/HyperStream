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

from ..utils import Printable, FrozenKeyDict
from node import Node
from factor import Factor, MultiOutputFactor
from ..tool import Tool, MultiOutputTool
from ..models import WorkflowDefinitionModel, FactorDefinitionModel, NodeDefinitionModel
from ..stream import StreamId
from ..errors import StreamNotFoundError, IncompatiblePlatesError

import logging
from collections import defaultdict
import itertools


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
        self.factor_collections = defaultdict(list)

        logging.info("New workflow created with id {}".format(workflow_id))
    
    def execute(self, time_interval):
        """
        Here we execute the factors over the streams in the workflow
        :return: None
        """
        # TODO: Currently expects the factors to be declared sequentially
        # for factor_collection in self.factor_collections.values()[::-1]:
        #     for factor in factor_collection:
        for factor_name in self.execution_order:
            for factor in self.factor_collections[factor_name]:
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

        if plate_ids:
            for plate_id in plate_ids:
                plate = self.plates[plate_id]
                # streams[plate] = {}

                # Currently allowing multiple plates here

                for pv in plate.values:
                    # Construct stream id
                    stream_id = StreamId(name=stream_name, meta_data=pv)

                    # Now try to locate the stream and add it (raises StreamNotFoundError if not found)
                    # streams.append(self.channels.get_stream(stream_id))
                    streams[pv] = channel.get_or_create_stream(stream_id=stream_id)

        else:
            streams[None] = channel.get_or_create_stream(stream_id=StreamId(name=stream_name))

        node = Node(stream_name, streams, plate_ids)
        self.nodes[stream_name] = node
        logging.info("Added node with id {}".format(stream_name))
        
        return node
    
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
            if sources:
                # Check that the plate_manager are compatible
                source_plates = itertools.chain(*(source.plate_ids for source in sources))
                for p in sink.plate_ids:
                    if p not in set(source_plates):
                        raise IncompatiblePlatesError("{} not in source plates".format(p))
                for p in source_plates:
                    if p not in set(sink.plate_ids):
                        raise IncompatiblePlatesError("{} not in sink plate_manager".format(p))
            plates = [self.plates[plate_id] for plate_id in sink.plate_ids]
        else:
            plates = None

        factor = Factor(tool=tool, source_nodes=sources,
                        sink_node=sink, alignment_node=alignment_node,
                        plates=plates)
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
        output_plates = [self.plates[plate_id] for plate_id in sink.plate_ids]

        if len(output_plates) > 1:
            raise NotImplementedError

        input_plates = [self.plates[plate_id] for plate_id in source.plate_ids]

        if len(input_plates) > 1:
            raise NotImplementedError

        if output_plates[0].parent is None and len(input_plates) == 1:
            raise IncompatiblePlatesError("Parent plate does not match input plate")

        if output_plates[0].parent is not None and len(input_plates) == 0:
            raise IncompatiblePlatesError("Parent plate does not match input plate")

        if output_plates[0].parent is not None and len(input_plates) == 1 \
                and output_plates[0].parent != input_plates[0]:
            raise IncompatiblePlatesError("Parent plate does not match input plate")

        factor = MultiOutputFactor(tool=tool, source_node=source, sink_node=sink,
                                   input_plate=input_plates[0], output_plate=output_plates[0])
        self.factor_collections[tool.name].append(factor)
        self.execution_order.append(tool.name)

        return factor


class WorkflowManager(Printable):
    """
    Workflow manager. Responsible for reading and writing workflows to the database, and can execute all of the
    workflows
    """
    def __init__(self, channel_manager, plate_manager):
        """
        Initialise the workflow object
        :param channel_manager: The channel manager
        :param plate_manager: All defined plates
        """
        self.channels = channel_manager
        self.plates = plate_manager
        
        self.workflows = FrozenKeyDict()
        self.uncommitted_workflows = set()
        
        for workflow_definition in WorkflowDefinitionModel.objects():
            try:
                workflow = Workflow(
                    channels=channel_manager,
                    plates=plate_manager,
                    workflow_id=workflow_definition.workflow_id,
                    name=workflow_definition.name,
                    description=workflow_definition.description,
                    owner=workflow_definition.owner
                )
                
                for n in workflow_definition.nodes:
                    workflow.create_node(
                        stream_name=n.stream_name,
                        channel=channel_manager.get_channel(n.channel_id),
                        plate_ids=n.plate_ids)
                
                # NOTE that we have to replicate the factor over the plate
                # This is fairly simple in the case of
                # 1. a single plate.
                # However we can have:
                # 2. nested plates
                # 3. intersecting plates
                # 4. a combination of 2. and 3.

                # TODO: alignment node
                for f in workflow_definition.factors:
                    continue
                    source_nodes = [workflow.nodes[s] for s in f.sources]
                    sink_node = workflow.nodes[f.sink]
                    
                    # sort the plate lists by increasing length
                    factor_plates = sorted([plate_manager[plate_id] for plate_id in
                                            list(itertools.chain.from_iterable(s.plate_ids for s in source_nodes))],
                                           key=lambda x: len(x))
                    
                    # TODO: Here we need to get the sub-graph of the plate tree so that we can build our for loops later
                    # TODO: One for loop for each level of nesting
                    tool = channel_manager.get_tool(
                        name=f.tool.name,
                        parameters=f.tool.parameters)
                    workflow.create_factor(tool, source_nodes, sink_node, None)
                
                self.add_workflow(workflow, False)
            except StreamNotFoundError as e:
                logging.error(str(e))
    
    def add_workflow(self, workflow, commit=False):
        """
        Add a new workflow and optionally commit it to the database
        :param workflow: The workflow
        :param commit: Whether to commit the workflow to the database
        :type workflow: Workflow
        :type commit: bool
        :return: None
        """
        self.workflows[workflow.workflow_id] = workflow
        logging.info("Added workflow {} to workflow manager".format(workflow.workflow_id))
        
        # Optionally also save the workflow to database
        if commit:
            self.commit_workflow(workflow.workflow_id)
        else:
            self.uncommitted_workflows.add(workflow.workflow_id)
    
    def commit_workflow(self, workflow_id):
        """
        Commit the workflow to the database
        :param workflow_id: The workflow id
        :return: None
        """
        # TODO: We should also be committing the Stream definitions if there are new ones
        
        workflow = self.workflows[workflow_id]
        
        workflow_definition = WorkflowDefinitionModel(
            workflow_id=workflow.workflow_id,
            name=workflow.name,
            description=workflow.description,
            nodes=[NodeDefinitionModel(stream_name=n.stream_name, plate_ids=n.plate_ids) for n in workflow.nodes],
            factors=[FactorDefinitionModel(tool=f.tool, sources=[s.stream_id for s in f.sources], sink=f.sink.stream_id)
                     for f in workflow.factors],
            owner=workflow.owner
        )
        
        workflow_definition.save()
        self.uncommitted_workflows.remove(workflow_id)
        logging.info("Committed workflow {} to database".format(workflow_id))
    
    def commit_all(self):
        """
        Commit all workflows to the database
        :return: None
        """
        for workflow_id in self.uncommitted_workflows:
            self.commit_workflow(workflow_id)
    
    # def execute_all(self):
    #     """
    #     Execute all workflows
    #     """
    #     for workflow in self.workflows:
    #         self.workflows[workflow].execute()
    
    def create_plate(self, plate, commit=False):
        """
        Create a plate. Make sure all workflows can use it, and optionally commit it to database
        :param plate: The plate
        :param commit: Commit to database
        :return: None
        """
        # TODO: Add the plate, make sure all workflows can use it, and optionally commit it to database
        raise NotImplementedError
