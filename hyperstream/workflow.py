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
import logging


class Node(Printable):
    def __init__(self, node_id, stream, plates):
        self.node_id = node_id
        self.stream = stream
        self.plates = plates


class Factor(Printable):
    def __init__(self, tool, sources, sink):
        self.tool = tool
        self.sources = sources
        self.sink = sink


class Plate(Printable):
    def __init__(self, plate_id, meta_data_id, description, values, parent_plate):
        self.plate_id = plate_id
        self.meta_data_id = meta_data_id
        self.values = values
        self.parent_plate = parent_plate
        self.description = description


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
        for node in self.nodes:
            for plate in self.nodes[node].plates:
                # Foreach over this plate
                pass


def get_plate_values(plate_definition, possible_values):
    if plate_definition.values:
        values = []
        for v in plate_definition.values:
            if isinstance(v, int):
                if 0 <= v < len(possible_values):
                    values.append(possible_values[v])
                else:
                    raise ValueError("Plate ID {}, Requested value {} not in possible values".format(
                        plate_definition.plate_id, v, possible_values))
            elif isinstance(v, (str, unicode)):
                if v in possible_values:
                    values.append(v)
                else:
                    raise ValueError("Plate ID {}, Requested value {} not in possible values".format(
                        plate_definition.plate_id, v, possible_values))
            else:
                raise TypeError("Expect str or int, got {}".format(str(type(v))))
        return values
    else:
        if plate_definition.complement:
            return possible_values
        else:
            raise ValueError("Plate ID {} Empty values and complement set to false".format(plate_definition.plate_id))


class WorkflowManager(Printable):
    workflows = {}
    plates = {}

    def __init__(self, channels, meta_data_lists):
        self.channels = channels

        # Plate definitions (arrays of streams) version 2

        # First we want to pull out all parent plates
        # TODO: want to write for p in PlateDefinitionModel2.objects(parent_plate=''):
        for p in PlateDefinitionModel2.objects():
            if not p.parent_plate:
                possible_values = meta_data_lists[p.meta_data_id]
                values = get_plate_values(p, list(possible_values))
                self.plates[p.plate_id] = Plate(p.plate_id, p.meta_data_id, p.description, values, None)

        # Now we want to pull out all child plates
        # TODO: want to write for p in PlateDefinitionModel2.objects(parent_plate__ne=''):
        for p in PlateDefinitionModel2.objects():
            if p.parent_plate:
                # First pull out the parent plate.
                # TODO: Note that for double nesting this might not exist yet!
                parent_plate = self.plates[p.parent_plate]
                values = {}
                for v in meta_data_lists[parent_plate.meta_data_id]:
                    # TODO: This only works for one level of recursion
                    possible_values = meta_data_lists[parent_plate.meta_data_id][v][p.meta_data_id]
                    values[v] = get_plate_values(p, possible_values)

                self.plates[p.plate_id] = Plate(p.plate_id, p.meta_data_id, p.description, values, parent_plate)


        # Plate definitions (arrays of streams)
        # for p in PlateDefinitionModel.objects:
        #     parent_list = meta_data_lists
        #     components = []
        #     # TODO: Check for list complements
        #     for pc in p.components:
        #         key = pc.meta_data_id
        #         try:
        #             possible_values = list(parent_list[pc.meta_data_id])
        #         except KeyError as e:
        #             logging.warn("Plate ID {}, meta data ID {} not found in lists".format(p.plate_id, key))
        #             raise e
        #         values = []
        #         for v in pc.values:
        #             if isinstance(v, int):
        #                 if 0 <= v < len(possible_values):
        #                     values.append(possible_values[v])
        #                 else:
        #                     raise ValueError("Plate ID {}, Requested value {} not in possible values".format(
        #                         p.plate_id, v, possible_values))
        #             elif isinstance(v, (str, unicode)):
        #                 if v in possible_values:
        #                     values.append(v)
        #                 else:
        #                     raise ValueError("Plate ID {}, Requested value {} not in possible values".format(
        #                         p.plate_id, v, possible_values))
        #             else:
        #                 raise TypeError("Expect str or int, got {}".format(str(type(v))))
        #         components.append(PlateComponent(key, values))
        #         if isinstance(parent_list[pc.meta_data_id], dict):
        #             parent_list = parent_list[pc.meta_data_id]
        #     self.plates[p.plate_id] = Plate(p.plate_id, components)

        # TODO: Make sure all of the plates in the workflow definitions exist in the plate definitions
        for f in WorkflowDefinitionModel.objects:
            self.workflows[f.workflow_id] = Workflow(channels, self.plates, f)

    def execute_all(self):
        for workflow in self.workflows:
            self.workflows[workflow].execute()
