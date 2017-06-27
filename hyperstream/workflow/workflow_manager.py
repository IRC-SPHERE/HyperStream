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
import copy_reg
import marshal
import types
from copy import deepcopy

from mongoengine.context_managers import switch_db

from . import Workflow
from ..time_interval import TimeIntervals
from ..models import WorkflowDefinitionModel, FactorDefinitionModel, NodeDefinitionModel, WorkflowStatusModel
from ..utils import Printable, FrozenKeyDict, StreamNotFoundError, utcnow, ToolInitialisationError, ToolNotFoundError, \
    IncompatibleToolError
from ..factor import Factor, NodeCreationFactor, MultiOutputFactor
from ..tool import Tool


def code_unpickler(data):
    return marshal.loads(data)


def code_pickler(code):
    return code_unpickler, (marshal.dumps(code),)


# Register the code_pickler and code_unpickler handlers for code objects. See http://effbot.org/librarybook/copy-reg.htm
copy_reg.pickle(types.CodeType, code_pickler, code_unpickler)


class WorkflowManager(Printable):
    """
    Workflow manager. Responsible for reading and writing workflows to the database, and can execute all of the
    workflows
    """

    def __init__(self, channel_manager, plate_manager):
        """
        Initialise the workflow object
        :param channel_manager: The channel manager
        :param plate_manager: The plate manager
        """
        self.channel_manager = channel_manager
        self.plate_manager = plate_manager

        self.workflows = FrozenKeyDict()
        self.uncommitted_workflows = set()

        with switch_db(WorkflowDefinitionModel, db_alias='hyperstream'):
            for workflow_definition in WorkflowDefinitionModel.objects():
                try:
                    self.load_workflow(workflow_definition.workflow_id)
                except (StreamNotFoundError, ToolInitialisationError, ToolNotFoundError, IncompatibleToolError) as e:
                    logging.warn(str(e))

    def load_workflow(self, workflow_id):
        """
        Load workflow from the database and store in memory
        :param workflow_id: The workflow id
        :return: The workflow
        """
        with switch_db(WorkflowDefinitionModel, db_alias='hyperstream'):
            workflow_definition = WorkflowDefinitionModel.objects.get(workflow_id=workflow_id)
            if not workflow_definition:
                logging.warn("Attempted to load workflow with id {}, but not found".format(workflow_id))

            workflow = Workflow(
                channels=self.channel_manager,
                plate_manager=self.plate_manager,
                workflow_id=workflow_id,
                name=workflow_definition.name,
                description=workflow_definition.description,
                owner=workflow_definition.owner,
                online=workflow_definition.online,
                monitor=workflow_definition.monitor
            )

            for n in workflow_definition.nodes:
                workflow.create_node(
                    stream_name=n.stream_name,
                    channel=self.channel_manager.get_channel(n.channel_id),
                    plate_ids=n.plate_ids)

            for f in workflow_definition.factors:
                source_nodes = [workflow.nodes[node_id] for node_id in f.sources] if f.sources else []
                sink_nodes = [workflow.nodes[node_id] for node_id in f.sinks] if f.sinks else []
                alignment_node = workflow.nodes[f.alignment_node] if f.alignment_node else None
                splitting_node = workflow.nodes[f.splitting_node] if f.splitting_node else None
                output_plate = f.output_plate

                parameters = Tool.parameters_from_model(f.tool.parameters)
                tool = dict(name=f.tool.name, parameters=parameters)

                if f.factor_type == "Factor":
                    if len(sink_nodes) != 1:
                        raise ValueError(
                            "Standard factors should have a single sink node, received {}"
                            .format(len(sink_nodes)))

                    if splitting_node is not None:
                        raise ValueError(
                            "Standard factors do not support splitting nodes")

                    if output_plate is not None:
                        raise ValueError(
                            "Standard factors do not support output plates")

                    workflow.create_factor(
                        tool=tool,
                        sources=source_nodes,
                        sink=sink_nodes[0],
                        alignment_node=alignment_node
                    )

                elif f.factor_type == "MultiOutputFactor":
                    if len(source_nodes) > 1:
                        raise ValueError(
                            "MultiOutputFactor factors should have at most one source node, received {}"
                            .format(len(source_nodes)))

                    if len(sink_nodes) != 1:
                        raise ValueError(
                            "MultiOutputFactor factors should have a single sink node, received {}"
                            .format(len(sink_nodes)))

                    if alignment_node is not None:
                        raise ValueError(
                            "MultiOutputFactor does not support alignment nodes")

                    if output_plate is not None:
                        raise ValueError(
                            "MultiOutputFactor does not support output plates")

                    workflow.create_multi_output_factor(
                        tool=tool,
                        source=source_nodes[0] if source_nodes else None,
                        splitting_node=splitting_node,
                        sink=sink_nodes[0]
                    )

                elif f.factor_type == "NodeCreationFactor":
                    if len(source_nodes) > 1:
                        raise ValueError(
                            "NodeCreationFactor factors should no more than one source node, received {}"
                            .format(len(source_nodes)))

                    if len(sink_nodes) != 0:
                        raise ValueError(
                            "NodeCreationFactor factors should not have sink nodes"
                            .format(len(sink_nodes)))

                    if output_plate is None:
                        raise ValueError(
                            "NodeCreationFactor requires an output plate definition")

                    workflow.create_node_creation_factor(
                        tool=tool,
                        source=source_nodes[0] if source_nodes else None,
                        output_plate=output_plate.to_mongo().to_dict(),
                        plate_manager=self.plate_manager
                    )

                else:
                    raise NotImplementedError("Unsupported factor type {}".format(f.factor_type))

            self.add_workflow(workflow, False)
            return workflow

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

    def delete_workflow(self, workflow_id):
        """
        Delete a workflow from the database
        :param workflow_id:
        :return: None
        """
        deleted = False

        with switch_db(WorkflowDefinitionModel, "hyperstream"):
            workflows = WorkflowDefinitionModel.objects(workflow_id=workflow_id)
            if len(workflows) == 1:
                workflows[0].delete()
                deleted = True
            else:
                logging.debug("Workflow with id {} does not exist".format(workflow_id))

        with switch_db(WorkflowStatusModel, "hyperstream"):
            workflows = WorkflowStatusModel.objects(workflow_id=workflow_id)
            if len(workflows) == 1:
                workflows[0].delete()
                deleted = True
            else:
                logging.debug("Workflow status with id {} does not exist".format(workflow_id))

        if workflow_id in self.workflows:
            del self.workflows[workflow_id]
            deleted = True

        if deleted:
            logging.info("Deleted workflow with id {}".format(workflow_id))

    def commit_workflow(self, workflow_id):
        """
        Commit the workflow to the database
        :param workflow_id: The workflow id
        :return: None
        """
        # TODO: We should also be committing the Stream definitions if there are new ones

        workflow = self.workflows[workflow_id]

        with switch_db(WorkflowDefinitionModel, "hyperstream"):
            workflows = WorkflowDefinitionModel.objects(workflow_id=workflow_id)
            if len(workflows) > 0:
                logging.warn("Workflow with id {} already exists in database".format(workflow_id))
                return

            factors = []
            for f in workflow.factors:
                tool = f.tool.get_model()

                if isinstance(f, Factor):
                    sources = [s.node_id for s in f.sources] if f.sources else []
                    sinks = [f.sink.node_id]
                    alignment_node = f.alignment_node.node_id if f.alignment_node else None
                    splitting_node = None
                    output_plate = None

                elif isinstance(f, MultiOutputFactor):
                    sources = [f.source.node_id] if f.source else []
                    sinks = [f.sink.node_id]
                    alignment_node = None
                    splitting_node = f.splitting_node.node_id if f.splitting_node else None
                    output_plate = None

                elif isinstance(f, NodeCreationFactor):
                    sources = [f.source.node_id] if f.source else []
                    sinks = []
                    alignment_node = None
                    splitting_node = None
                    output_plate = f.output_plate

                else:
                    raise NotImplementedError("Unsupported factor type")

                if output_plate:
                    output_plate_copy = deepcopy(output_plate)
                    if 'parent_plate' in output_plate_copy:
                        del output_plate_copy['parent_plate']
                else:
                    output_plate_copy = None

                factor = FactorDefinitionModel(
                    tool=tool,
                    factor_type=f.__class__.__name__,
                    sources=sources,
                    sinks=sinks,
                    alignment_node=alignment_node,
                    splitting_node=splitting_node,
                    output_plate=output_plate_copy
                )

                factors.append(factor)

            nodes = []
            for n in workflow.nodes.values():
                nodes.append(NodeDefinitionModel(
                    stream_name=n.node_id,
                    plate_ids=n.plate_ids,
                    channel_id=n._channel.channel_id
                ))

            workflow_definition = WorkflowDefinitionModel(
                workflow_id=workflow.workflow_id,
                name=workflow.name,
                description=workflow.description,
                nodes=nodes,
                factors=factors,
                owner=workflow.owner,
                online=workflow.online,
                monitor=workflow.monitor
            )

            workflow_definition.save()

        with switch_db(WorkflowStatusModel, db_alias='hyperstream'):
            workflow_status = WorkflowStatusModel(
                workflow_id=workflow.workflow_id,
                last_updated=utcnow(),
                last_accessed=utcnow(),
                requested_intervals=[]
            )

            workflow_status.save()

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
        for workflow_id in self.workflows:
            if self.workflows[workflow_id].online:
                for interval in self.workflows[workflow_id].requested_intervals:
                    logging.info("Executing workflow {} over interval {}".format(workflow_id, interval))
                    self.workflows[workflow_id].execute(interval)
                    # self.workflows[workflow_id].requested_intervals -= interval

    def set_requested_intervals(self, workflow_id, requested_intervals):
        """
        Sets the requested intervals for a given workflow
        :param workflow_id: The workflow id
        :param requested_intervals: The requested intervals
        :return: None
        :type requested_intervals: TimeIntervals
        """
        if workflow_id not in self.workflows:
            raise ValueError("Workflow {} not found".format(workflow_id))

        self.workflows[workflow_id].requested_intervals = requested_intervals

    def set_all_requested_intervals(self, requested_intervals):
        """
        Sets the requested intervals for all workflow
        :param requested_intervals: The requested intervals
        :return: None
        :type requested_intervals: TimeIntervals
        """
        for workflow_id in self.workflows:
            if self.workflows[workflow_id].online:
                self.workflows[workflow_id].requested_intervals = requested_intervals
