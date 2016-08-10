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


class Plate(Printable):
    def __init__(self, plate_definition):
        self.plate_definition = plate_definition


class Workflow(Printable):
    def __init__(self, workflow_definition):
        self.workflow_definition = workflow_definition

    def execute(self, sphere_connector):
        """
        Here we need to work out in which channels each of the streams in the workflow live
        :param sphere_connector:
        :return:
        """
        pass


class WorkflowManager(Printable):
    workflows = {}
    plates = {}

    def __init__(self):
        for p in PlateDefinitionModel.objects:
            self.plates[p.plate_id] = Plate(p)

        # TODO: Make sure all of the plates in the workflow definitions exist in the plate definitions
        for f in WorkflowDefinitionModel.objects:
            self.workflows[f.workflow_id] = Workflow(f)

    def execute_all(self, sphere_connector):
        for workflow in self.workflows:
            self.workflows[workflow].execute(sphere_connector)
