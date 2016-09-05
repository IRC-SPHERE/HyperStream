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

from utils import Printable, FrozenKeyDict
from models import WorkflowDefinitionModel, FactorDefinitionModel, NodeDefinitionModel
from stream import StreamId
import logging
from errors import StreamNotFoundError
from collections import defaultdict

import itertools


class Node(Printable):
    """
    A node in the graph. This consists of a set of streams defined over a set of plates
    """
    
    def __init__(self, node_id, streams, plate_ids):
        """
        Initialise the node
        :param node_id: The node id
        :param streams: The streams
        :param plate_ids: The plate ids
        """
        self.node_id = node_id
        self.streams = streams
        self.plate_ids = plate_ids
        
        """
        When defining streams, it will be useful to be able to query node objects
        to determine the streams that have metadata of a particular value.
        Use Node.reverse_lookup as follows:
            meta_data = {'a': 1, 'b': 1}
            
        """
    
    def modify(self, modifier):
        """
        Applies a modifier to all streams
        :param modifier
        :return: self (for chaining)
        """
        
        for stream in self.streams:
            stream.modify(modifier)
        
        return self
    
    def window(self, time_interval):
        """
        Sets the time window for all the streams
        :param time_interval: either a TimeInterval object or (start, end) tuple of type str or datetime
        :type time_interval: Iterable, TimeInterval
        :return: self (for chaining)
        """
        for stream in self.streams:
            stream.window(time_interval)
        return self
    
    def relative_window(self, time_interval):
        """
        Sets the time window for all the streams
        :param time_interval: either a TimeInterval object or (start, end) tuple of type str or datetime
        :type time_interval: Iterable, TimeInterval
        :return: self (for chaining)
        """
        for stream in self.streams:
            stream.relative_window(time_interval)
        return self
    
    def execute(self):
        """
        Execute all of the streams for this node
        :return: self (for chaining)
        """
        for stream in self.streams:
            # TODO: This is where the execution logic for the streams goes (e.g. add to Queuing system)
            logging.info("Executing stream {}".format(stream.stream_id))
            stream.execute()
        
        return self
    
    def __iter__(self):
        return iter(self.streams)
    
    def intersection(self, meta):
        keys = self.streams[0].stream_id.meta_data.keys()
        
        return StreamId(self.node_id, dict(*zip((kk, meta[kk]) for kk in keys)))


class Factor(Printable):
    """
    """
    
    def __init__(self, tool, sources, sink):
        """
        Initialise this factor
        :param tool: The tool
        :param sources: The source streams
        :param sink: The sink stream
        """
        self.tool = tool
        self.sources = sources
        self.sink = sink


class Workflow(Printable):
    """
    Workflow. This defines the graph of operations through "nodes" and "factors".
    """
    
    def __init__(self, channels, plates, workflow_id, name, description, owner):
        """
        Initalise the workflow
        :param channels:
        :param plates:
        :param workflow_id:
        :param name:
        :param description:
        :param owner:
        """
        self.channels = channels
        self.plates = plates
        self.workflow_id = workflow_id
        self.name = name
        self.description = description
        self.owner = owner
        self.nodes = {}
        self.factors = {}
        
        logging.info("New workflow created with id {}".format(workflow_id))
    
    def execute(self):
        """
        Here we execute the streams in the workflow
        :return:
        """
        for node in self.nodes:
            logging.debug("Executing node {}".format(node))
            node.execute()
    
    def create_streams(self, channel, stream_name, plate_ids, tool_stream=None):
        """
        Create a node in the graph, using the stream name and plate
        :param channel: the channel containing the stream
        :param stream_name: The name of the stream
        :param plate_ids: The plate ids. The stream meta-data will be auto-generated from these
        :param tool_stream:
        :return: The streams associated with this node
        """
        streams = []
        
        for plate_id in plate_ids:
            # Currently allowing multiple plates here
            plate_values = self.plates[plate_id]
            
            for pv in plate_values:
                # Construct stream id
                stream_id = StreamId(name=stream_name, meta_data=pv)
                
                streams.append(
                    channel.create_stream(
                        stream_id=stream_id,
                        tool_stream=tool_stream
                    )
                )
        
        node = Node(stream_name, streams, plate_ids)
        self.nodes[stream_name] = node
        logging.info("Added node with id {}".format(stream_name))
        
        return node
    
    def create_factors(self, tooling_callback, output_channel, output_stream_name, plate_ids, node_names):
        streams = []
        
        # for node_name in node_names:
        #     for stream in self.nodes[node_name].streams:
        for plate_id in plate_ids:
            for pv in self.plates[plate_id]:
                tool_stream = tooling_callback(
                    [node.intersection(pv) for node_name, node in self.nodes.iteritems()]
                )
                
                streams.append(
                    output_channel.create_stream(
                        stream_id=StreamId(output_stream_name, pv),
                        tool_stream=tool_stream
                    )
                )
        
        node = Node(
            node_id=output_stream_name,
            streams=streams,
            plate_ids=None
        )
        
        self.nodes[output_stream_name] = node
        
        return node
    
    def iteritems(self):
        return self.nodes.iteritems()
    
    def __getitem__(self, stream_name):
        return self.nodes[stream_name]


class WorkflowManager(Printable):
    """
    Workflow manager
    """
    
    def __init__(self, channels, plates):
        """

        :param channels:
        :param plates:
        """
        self.channels = channels
        self.plates = plates
        
        self.workflows = FrozenKeyDict()
        self.uncommitted_workflows = set()
        
        for workflow_definition in WorkflowDefinitionModel.objects():
            try:
                workflow = Workflow(
                    channels=channels,
                    plates=plates,
                    workflow_id=workflow_definition.workflow_id,
                    name=workflow_definition.name,
                    description=workflow_definition.description,
                    owner=workflow_definition.owner
                )
                
                for node_id in workflow_definition.nodes:
                    n = workflow_definition.nodes[node_id]
                    workflow.create_node(node_id, n.stream_name, n.plate_ids)
                
                # NOTE that we have to replicate the factor over the plate
                # This is fairly simple in the case of
                # 1. a single plate.
                # However we can have:
                # 2. nested plates
                # 3. intersecting plates
                # 4. a combination of 2. and 3.
                for f in workflow_definition.factors:
                    continue
                    source_nodes = [workflow.nodes[s] for s in f.sources]
                    sink_node = workflow.nodes[f.sink]
                    
                    # sort the plate lists by increasing length
                    factor_plates = sorted([plates[plate_id] for plate_id in
                                            list(itertools.chain.from_iterable(s.plate_ids for s in source_nodes))],
                                           key=lambda x: len(x))
                    
                    # TODO: Here we need to get the subgraph of the plate tree so that we can build our for loops later
                    # TODO: One for loop for each level of nesting
                    # TODO: populate input streams
                    tool = channels.get_tool(f.tool.name, f.tool.parameters, input_streams=[])
                    workflow.create_factor(tool, source_nodes, sink_node)
                
                self.add_workflow(workflow, False)
            except StreamNotFoundError as e:
                logging.error(str(e))
    
    def add_workflow(self, workflow, commit=False):
        """

        :param workflow:
        :param commit:
        :return:
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

        :param workflow_id:
        :return:
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
        :return:
        """
        for workflow_id in self.uncommitted_workflows:
            self.commit_workflow(workflow_id)
    
    def execute_all(self):
        """
        Execute all workflows
        """
        for workflow in self.workflows:
            self.workflows[workflow].execute()
    
    def create_plate(self, plate, commit=False):
        """
        Create a plate. Make sure all workflows can use it, and optionally commit it to database
        :param plate: The plate
        :param commit: Commit to database
        :return: None
        """
        # TODO: Add the plate, make sure all workflows can use it, and optionally commit it to database
        raise NotImplementedError
