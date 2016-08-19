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
from models import WorkflowDefinitionModel, PlateDefinitionModel2
from stream import StreamId
import logging
from errors import StreamNotFoundError
from utils import MetaDataTree as Tree


class Node(Printable):
    def __init__(self, node_id, streams, plates):
        self.node_id = node_id
        self.streams = streams
        self.plates = plates


class Factor(Printable):
    def __init__(self, tool, sources, sink):
        self.tool = tool
        self.sources = sources
        self.sink = sink


class Workflow(Printable):
    def __init__(self, channels, plates, workflow_definition):
        self.channels = channels

        self.workflow_id = workflow_definition.workflow_id
        self.name = workflow_definition.name
        self.owner = workflow_definition.owner

        # print(repr(channels.database_channel))

        self.nodes = {}
        for node_id in workflow_definition.nodes:
            node_definition = workflow_definition.nodes[node_id]

            streams = []

            for plate_id in node_definition.plate_ids:
                # Currently allowing multiple plates here
                plate_values = plates[plate_id]

                for pv in plate_values:
                    # Construct stream id
                    stream_id = StreamId(name=node_definition.stream_name, meta_data=pv)

                    # Now try to locate the stream and add it
                    streams.append(channels[stream_id])

            self.nodes[node_id] = Node(node_id, streams)

        self.factors = []
        for f in workflow_definition.factors:
            sources = [self.nodes[s] for s in f.sources]
            sink = self.nodes[f.sink]
            # TODO: Check that the tool exists
            self.factors.append(Factor(f.tool, sources, sink))

    def execute(self):
        """
        Here we execute the streams in the workflow
        :return:
        """
        for node in self.nodes:
            for plate in self.nodes[node].plates:
                # Foreach over this plate
                for value in plate.values:
                    # TODO: Get correct stream with this meta_data
                    # node.stream()
                    pass


class WorkflowManager(Printable):
    def __init__(self, channels, meta_data):
        self.channels = channels

        self.workflows = {}
        self.plates = {}

        self.global_plate_definitions = Tree()

        # Populate the global plate definitions from dict given in the config file
        for item in meta_data:
            self.global_plate_definitions.create_node(**item)

        logging.info("Global plate definitions: ")
        logging.info(self.global_plate_definitions)

        # Plate definitions (arrays of streams) version 2
        # First we want to pull out all parent plates
        # TODO: want to write for p in PlateDefinitionModel2.objects(parent_plate=''):
        for p in PlateDefinitionModel2.objects():
            if not p.values and not p.complement:
                raise ValueError("Empty values in plate definition and complement=False")

            """
            Want to get meta-data dictionaries for all plate combinations
            e.g.

            H plate, want

            {'H': [
              {'house': '1'},
              {'house': '2'}
            ]}

            H1 plate, want

            {'H1': [{'house': '1'}]}

            H.R plate, want

            {'H.R': [
              {'house': '1', 'resident': '1'},
              {'house': '1': 'resident': '2'},
              {'house': '2': 'resident': '1'}
            }
            """

            def get_parent_data(tree, node, d):
                """
                Recurse up the tree getting parent data
                :param tree: The tree
                :param node: The current node
                :param d: The initial dictionary
                :return: The hierarchical dictionary
                """
                parent = tree.parent(node.identifier)
                if parent.is_root():
                    return d
                d[parent.tag] = parent.data
                return get_parent_data(tree, parent, d)

            if p.parent_plate:
                values = []

                if not p.values:
                    # Empty list: choose all values
                    for n in self.global_plate_definitions.all_nodes():
                        if n.tag == p.meta_data_id:
                            values.insert(0, get_parent_data(self.global_plate_definitions, n, {n.tag: n.data}))
                else:
                    # Non-empty list - select only valid values
                    # TODO: Gracefully skips invalid values but doesn't raise an error
                    for n in self.global_plate_definitions.all_nodes():
                        if n.tag == p.meta_data_id and n.data in p.values:
                            values.insert(0, get_parent_data(self.global_plate_definitions, n, {n.tag: n.data}))
            else:
                if not p.values:
                    # Empty list: choose all values
                    values = [{n.tag: n.data} for n in self.global_plate_definitions.all_nodes()
                              if n.tag == p.meta_data_id]
                else:
                    # Non-empty list - select only valid values
                    # TODO: Gracefully skips invalid values but doesn't raise an error
                    values = [{n.tag: n.data} for n in self.global_plate_definitions.all_nodes()
                              if n.tag == p.meta_data_id and n.data in p.values]

            self.plates[p.plate_id] = values

        for workflow_definition in WorkflowDefinitionModel.objects():
            try:
                self.workflows[workflow_definition.workflow_id] = Workflow(channels, self.plates, workflow_definition)
            except StreamNotFoundError as e:
                logging.error(str(e))

    def execute_all(self):
        for workflow in self.workflows:
            self.workflows[workflow].execute()
