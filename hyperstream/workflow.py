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
        self.factors = []
        logging.info("New workflow created with id {}".format(workflow_id))

    def execute(self):
        """
        Here we execute the streams in the workflow
        :return:
        """
        for node in self.nodes:
            logging.debug("Executing node {}".format(node))
            node.execute()

    def create_node(self, node_id, stream_name, plate_ids):
        """
        Create a node in the graph, using the stream name and plate
        :param node_id: The node id
        :param stream_name: The name of the stream
        :param plate_ids: The plate ids. The stream meta-data will be auto-generated from these
        :return: The streams associated with this node
        """
        streams = []

        for plate_id in plate_ids:
            # Currently allowing multiple plates here
            plate_values = self.plates[plate_id]

            for pv in plate_values:
                # Construct stream id
                stream_id = StreamId(name=stream_name, meta_data=pv)

                # Now try to locate the stream and add it (raises StreamNotFoundError if not found)
                streams.append(self.channels.get_stream(stream_id))

        node = Node(node_id, streams, plate_ids)
        self.nodes[node_id] = node
        logging.info("Added node with id {}".format(node_id))
        return node

    def add_factor(self, tool, sources, sink):
        """

        Args:
            tool:
            sources:
            sink:
        """
        sources = [self.nodes[s] for s in sources]
        sink = self.nodes[sink]
        # TODO: Check that the tool exists
        self.factors.append(Factor(tool, sources, sink))
        logging.info("Added factor with tool name {}".format(tool))


class WorkflowManager(Printable):
    """
    """
    def __init__(self, channels, plates):
        """

        Args:
            channels:
            plates:
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

                for f in workflow_definition.factors:
                    workflow.add_factor(f.tool, f.sources, f.sink)

                self.add_workflow(workflow, False)
            except StreamNotFoundError as e:
                logging.error(str(e))

    def add_workflow(self, workflow, commit=False):
        """

        Args:
            workflow:
            commit:
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

        Args:
            workflow_id:
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

        """
        for workflow_id in self.uncommitted_workflows:
            self.commit_workflow(workflow_id)

    def execute_all(self):
        """

        """
        for workflow in self.workflows:
            self.workflows[workflow].execute()

    def add_plate(self, plate, commit=False):
        """

        Args:
            plate:
            commit:
        """
        # TODO: Add the plate, make sure all workflows can use it, and optionally commit it to database
        raise NotImplementedError

