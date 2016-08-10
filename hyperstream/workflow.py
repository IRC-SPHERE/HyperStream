"""
The MIT License (MIT)
Copyright (c) 2014-2017 University of Bristol

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.
"""
from utils import Printable
from models import WorkflowDefinitionModel, PlateDefinitionModel


class Node(Printable):
    def __init__(self, node_id, stream_id, plates):
        self.node_id = node_id
        self.stream_id = stream_id
        self.plate_ids = plates


class Factor(Printable):
    def __init__(self, tool, sources, sink):
        self.tool = tool
        self.sources = sources
        self.sink = sink


class Plate(Printable):
    def __init__(self, plate_definition):
        self.plate_definition = plate_definition


class Workflow(Printable):
    def __init__(self, channels, plates, workflow_definition):
        self.channels = channels

        self.workflow_id = workflow_definition.workflow_id
        self.name = workflow_definition.name
        self.owner = workflow_definition.owner

        self.nodes = {}
        for node_id in workflow_definition.nodes:
            node = workflow_definition.nodes[node_id]
            # TODO: Check stream ID exists in stream_ids
            p = [plates[plate_id] for plate_id in node.plate_ids]
            self.nodes[node_id] = Node(node_id, node.stream_id, p)

        self.factors = []
        for f in workflow_definition.factors:
            sources = [self.nodes[s] for s in f.sources]
            sink = self.nodes[f.sink]
            # TODO: Check that the tool exists
            self.factors.append(Factor(f.tool, sources, sink))

    def execute(self):
        """
        Here we need to work out in which channels each of the streams in the workflow live
        :return:
        """
        pass


class WorkflowManager(Printable):
    workflows = {}
    plates = {}

    def __init__(self, channels):
        self.channels = channels

        # Plate definitions (arrays of streams)
        for p in PlateDefinitionModel.objects:
            self.plates[p.plate_id] = Plate(p)

        # TODO: Make sure all of the plates in the workflow definitions exist in the plate definitions
        for f in WorkflowDefinitionModel.objects:
            self.workflows[f.workflow_id] = Workflow(channels, self.plates, f)

    def execute_all(self):
        for workflow in self.workflows:
            self.workflows[workflow].execute()
