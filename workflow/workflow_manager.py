# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

import logging
import itertools
from mongoengine.context_managers import switch_db

from . import Workflow
from ..models import WorkflowDefinitionModel, FactorDefinitionModel, NodeDefinitionModel, ToolModel
from ..utils import Printable, FrozenKeyDict, StreamNotFoundError
from ..tool import MultiOutputTool

class WorkflowManager(Printable):
    """
    Workflow manager. Responsible for reading and writing workflows to the database, and can execute all of the
    workflows
    """

    def __init__(self, channel_manager, plates):
        """
        Initialise the workflow object
        :param channel_manager: The channel manager
        :param plates: All defined plates
        """
        self.channels = channel_manager
        self.plates = plates

        self.workflows = FrozenKeyDict()
        self.uncommitted_workflows = set()

        with switch_db(WorkflowDefinitionModel, db_alias='hyperstream'):
            for workflow_definition in WorkflowDefinitionModel.objects():
                try:
                    workflow = Workflow(
                        channels=channel_manager,
                        plates=plates,
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
                        factor_plates = sorted([plates[plate_id] for plate_id in
                                                list(itertools.chain.from_iterable(s.plate_ids for s in source_nodes))],
                                               key=lambda x: len(x))

                        # TODO: Here we need to get the sub-graph of the plate tree so that we can build our for loops
                        # later
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
        if workflow.workflow_id in self.workflows:
            raise KeyError("Workflow with id {} already exists".format(workflow.workflow_id))

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

        with switch_db(WorkflowDefinitionModel, "hyperstream"):
            factors = []
            for f in itertools.chain(*workflow.factor_collections.values()):
                tool = ToolModel(name=f.tool.name, version="", parameters=f.tool.parameters)
                raise NotImplementedError("sources and sinks should actually just be references to the nodes by node id")
                if isinstance(f.tool, MultiOutputTool):
                    factor = FactorDefinitionModel(tool=tool, sources=f.sources, sinks=f.sinks)
                else:
                    factor = FactorDefinitionModel(tool=tool, sources=f.sources, sinks=[f.sink])
                factors.append(factor)

            workflow_definition = WorkflowDefinitionModel(
                workflow_id=workflow.workflow_id,
                name=workflow.name,
                description=workflow.description,
                nodes=[NodeDefinitionModel(stream_name=n.node_id, plate_ids=n.plate_ids) for n in workflow.nodes.values()],
                factors=factors,
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

    def execute_all(self):
        """
        Execute all workflows
        """
        for workflow in self.workflows:
            self.workflows[workflow].execute()
