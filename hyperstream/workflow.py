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
    def __init__(self, node_id, streams, plate_ids):
        self.node_id = node_id
        self.streams = streams
        self.plate_ids = plate_ids


class Factor(Printable):
    def __init__(self, tool, sources, sink):
        self.tool = tool
        self.sources = sources
        self.sink = sink


class Workflow(Printable):
    def __init__(self, channels, plates, workflow_id, name, description, owner):
        self.channels = channels
        self.plates = plates
        self.workflow_id = workflow_id
        self.name = name
        self.description = description
        self.owner = owner
        self.nodes = {}
        self.factors = []

    def execute(self):
        """
        Here we execute the streams in the workflow
        :return:
        """
        for node in self.nodes:
            for stream in self.nodes[node].streams:
                # TODO: This is where the execution logic for the streams goes (e.g. add to Queuing system)
                stream.execute()

    def add_node(self, node_id, stream_name, plate_ids):
        streams = []

        for plate_id in plate_ids:
            # Currently allowing multiple plates here
            plate_values = self.plates[plate_id]

            for pv in plate_values:
                # Construct stream id
                stream_id = StreamId(name=stream_name, meta_data=pv)

                # Now try to locate the stream and add it (raises StreamNotFoundError if not found)
                streams.append(self.channels.get_stream(stream_id))

        self.nodes[node_id] = Node(node_id, streams, plate_ids)

    def add_factor(self, tool, sources, sink):
        sources = [self.nodes[s] for s in sources]
        sink = self.nodes[sink]
        # TODO: Check that the tool exists
        self.factors.append(Factor(tool, sources, sink))


class WorkflowManager(Printable):
    def __init__(self, channels, plates):
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
                    workflow.add_node(node_id, n.stream_name, n.plate_ids)

                for f in workflow_definition.factors:
                    workflow.add_factor(f.tool, f.sources, f.sink)

                self.workflows[workflow.workflow_id] = workflow
            except StreamNotFoundError as e:
                logging.error(str(e))

    def add_workflow(self, workflow, commit=False):
        self.workflows[workflow.workflow_id] = workflow

        # Optionally also save the workflow to database
        if commit:
            self.commit_workflow(workflow.workflow_id)
        else:
            self.uncommitted_workflows.add(workflow.workflow_id)

    def commit_workflow(self, workflow_id):
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

    def commit_all(self):
        for workflow_id in self.uncommitted_workflows:
            self.commit_workflow(workflow_id)

    def execute_all(self):
        for workflow in self.workflows:
            self.workflows[workflow].execute()
