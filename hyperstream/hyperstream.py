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
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.
"""
Main HyperStream class
"""

from . import ChannelManager, HyperStreamConfig, PlateManager, WorkflowManager, Client, Workflow
from .version import __version__
from .utils import HyperStreamLogger, ToolContainer, PluginContainer, PluginWrapper, FactorContainer, Singleton
from .session import Session
from .tool import Tool, MultiOutputTool, SelectorTool, AggregateTool, PlateCreationTool

import logging
from contextlib import contextmanager
from six import add_metaclass


@add_metaclass(Singleton)
class HyperStream(object):
    # noinspection PyUnresolvedReferences
    """
    HyperStream class: can be instantiated simply with hyperstream = HyperStream() for default operation.
    Use in the following way to create a session (and store history of execution etc).
    >>> with Hyperstream():
    >>>    pass

    Note that HyperStream uses the singleton pattern described here:
        https://stackoverflow.com/a/33201/1038264
    For py2k/py3k compatability we use the six decorator add_metaclass:
        https://pythonhosted.org/six/#six.add_metaclass
    """
    def __init__(self, loglevel=logging.INFO, file_logger=True, console_logger=True, mqtt_logger=None):
        """
        Initialise the HyperStream class. This starts the logger, loads the config files, connects to the main mongodb,
        and initialises the managers (channels, plates, workflows).

        :type console_logger: bool | dict | None
        :type file_logger: bool | dict | None
        :type mqtt_logger: dict | None
        :param loglevel: The default logging level
        :param file_logger: Whether to use a file logger. Either specify "True" in which case defaults are used,
        otherwise a dict optionally containing path, filename, loglevel
        :param console_logger: The console logger. Either specify "True" in which case defaults are used,
        otherwise a dict optionally containing loglevel
        :param mqtt_logger: Dict containing mqtt server, topic, and optionally loglevel
        """
        self._session = None

        self.parameters = dict(
            loglevel=loglevel,
            file_logger=file_logger,
            console_logger=console_logger,
            mqtt_logger=mqtt_logger
        )

        self.logger = HyperStreamLogger(
            default_loglevel=loglevel, file_logger=file_logger, console_logger=console_logger, mqtt_logger=mqtt_logger)
        self.config = HyperStreamConfig()
        self.client = Client(self.config.mongo)

        # Define some managers
        self.channel_manager = ChannelManager(self.config.plugins)
        self.plate_manager = PlateManager()
        self.workflow_manager = WorkflowManager(channel_manager=self.channel_manager, plate_manager=self.plate_manager)
        self.plugins = PluginContainer()

        # The following are to keep pep happy - will be populated below
        self.tools = None
        self.factors = None

        self.current_workflow = None  # Used in the new API - the current workflow being defined
        self.populate_tools_and_factors()

    def __repr__(self):
        """
        Get a string representation of this object that matches the creation syntax

        :return: The string representation
        """
        name = self.__class__.__name__
        # values = ", ".join("{}={}".format(k, repr(v)) for k, v in sorted(self.__dict__.items())
        #                    if k[0] != "_" and not k.endswith('manager'))
        values = ", ".join("{}={}".format(k, v) for k, v in self.parameters.items())
        return "{}({})".format(name, values)

    def __str__(self):
        """
        Get a string representation of this object

        :return: The string representation
        """
        return "HyperStream version {version}, connected to mongodb://{host}:{port}/{db}, session id {sid}".format(
            version=__version__,
            host=self.config.mongo['host'],
            port=self.config.mongo['port'],
            db=self.config.mongo['db'],
            sid=self.current_session.session_id if self.current_session else "<no session>"
        )

    def __del__(self):
        """
        Called when the hyperstream object is disposed of

        :return: None
        """
        self._cleanup()

    def __enter__(self):
        """
        Entry point. Using the "with" syntax starts a new session

        :return: self
        """
        self.new_session()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit point

        :param exc_type: exception type
        :param exc_value: exception value
        :param traceback: exception traceback
        :return: self
        """
        self._cleanup()

        # Re-raise the error
        if exc_type is not None:
            return None

        return self

    def _cleanup(self):
        """
        Clean-up operations. Closes the session and flushes and closes the loggers

        :return: None
        """
        if self.current_session is not None:
            self.current_session.close()
            self.current_session = None

        for handler in list(self.logger.root_logger.handlers):
            self.logger.root_logger.removeHandler(handler)
            handler.flush()
            handler.close()

    def new_session(self):
        """
        Start a new session to record computation history

        :return: the created session
        """
        self.current_session = Session(self, history_channel=self.config.history_channel)
        return self.current_session

    @property
    def sessions(self):
        """
        Get the list of sessions

        :return: the sessions
        """
        return list(Session.get_sessions(self))

    @property
    def current_session(self):
        """
        Get the current session

        :return: the current session
        """
        return self._session

    @current_session.setter
    def current_session(self, session):
        """
        Set the current session

        :param session: the session
        :return: None
        """
        if self._session is None:
            self._session = session
        else:
            if session is None or self._session.session_id != session.session_id:
                self._session.active = False
                self._session = session

    def clear_sessions(self, inactive_only=True, clear_history=False):
        """
        Clears all stored sessions, optionally excluding active sessions

        :param inactive_only: Whether to clear inactive sessions only
        :param clear_history: Whether to also clear session history
        :return: None
        """
        Session.clear_sessions(self, inactive_only, clear_history)

    @contextmanager
    def create_workflow(self, workflow_id, name, owner, description, online=False, monitor=False, safe=True):
        """
        Create a new workflow. Simple wrapper for creating a workflow and adding it to the workflow manager.

        :param workflow_id: The workflow id
        :param name: The workflow name
        :param owner: The owner/creator of the workflow
        :param description: A human readable description
        :param online: Whether this workflow should be executed by the online engine
        :param monitor: Whether the workflow computations should be monitored
        :param safe: If safe=True, will throw an error if the workflow already exists
        :return: The workflow

        """
        try:
            w = Workflow(
                workflow_id=workflow_id,
                name=name,
                owner=owner,
                description=description,
                online=online,
                monitor=monitor
            )

            self.workflow_manager.add_workflow(w)
            self.current_workflow = w
            yield w
        except KeyError as e:
            if safe:
                raise e
            else:
                self.current_workflow = self.workflow_manager.workflows[workflow_id]
                yield self.workflow_manager.workflows[workflow_id]
        finally:
            self.current_workflow = None

    def add_workflow(self, workflow):
        """
        Add the workflow to the workflow manager

        :param workflow: The workflow
        :type workflow: Workflow
        :return: None
        """
        self.workflow_manager.add_workflow(workflow)

    def populate_tools_and_factors(self):
        """
        Function to populate factory functions for the tools and factors for ease of access.

        :return: None
        """
        for tool_channel in self.channel_manager.tool_channels:
            if tool_channel.channel_id == "tools":
                # These are the core tools
                setattr(self, "tools", ToolContainer())
                setattr(self, "factors", FactorContainer())
                tool_container = getattr(self, "tools")
                factor_container = getattr(self, "factors")
            else:
                # This is a plugin, so ends in "_tools"
                plugin_name = "_".join(tool_channel.channel_id.split("_")[:-1])
                setattr(self.plugins, plugin_name, PluginWrapper())
                plugin = getattr(self.plugins, plugin_name)
                tool_container = plugin.tools
                factor_container = plugin.factors

            for tool_stream in tool_channel.streams:
                try:
                    # This is the tool initializer
                    tool_function = self.channel_manager.get_tool_class(tool_stream.name)
                    setattr(tool_container, tool_stream.name, tool_function)

                    def create_factory_function(tool_func):
                        """
                        This wrapper is needed to capture the tool_function closure

                        :param tool_func: The tool function
                        :return: The factory function

                        """
                        base = tool_function.__bases__[0]
                        if base == Tool:
                            def tool_factory_function(sources, alignment_node=None, **parameters):
                                """
                                Factory function for creating factors inside a workflow

                                :param sources: source nodes
                                :param alignment_node: alignment node
                                :return: the created factor
                                :type sources: list[Node] | tuple[Node] | None

                                """
                                if not self.current_workflow:
                                    raise ValueError("No workflow context - use create_workflow first")

                                # find matching tools (possibly different parameters)
                                matches = [f for f in self.current_workflow.factors if f.tool.__class__ == tool_func]
                                # make sure parameters are all the same
                                full_matches = [m for m in matches if m.sources == sources
                                                and m.alignment_node == alignment_node
                                                and dict(m.tool.parameters_dict) == parameters]

                                if len(full_matches) == 1:
                                    tool = full_matches[0].tool
                                else:
                                    tool = tool_func(**parameters)

                                return dict(
                                    workflow=self.current_workflow,
                                    tool=tool,
                                    sources=sources,
                                    alignment_node=alignment_node)

                            return tool_factory_function
                        elif base == MultiOutputTool:
                            def tool_factory_function(source, splitting_node=None, **parameters):
                                """
                                Factory function for creating factors inside a workflow

                                :param source: source node
                                :param splitting_node: splitting node
                                :return: the created factor
                                :type source: Node

                                """
                                if not self.current_workflow:
                                    raise ValueError("No workflow context - use create_workflow first")

                                # find matching tools (possibly different parameters)
                                matches = [f for f in self.current_workflow.factors if
                                           f.tool.__class__ == tool_func]
                                # make sure parameters are all the same
                                full_matches = [m for m in matches if m.source == source
                                                and m.splitting_node == splitting_node
                                                and dict(m.tool.parameters_dict) == parameters]

                                if len(full_matches) == 1:
                                    tool = full_matches[0].tool
                                else:
                                    tool = tool_func(**parameters)

                                return dict(
                                    workflow=self.current_workflow,
                                    tool=tool,
                                    source=source,
                                    splitting_node=splitting_node)

                            return tool_factory_function

                        elif base == AggregateTool:
                            def tool_factory_function(sources, alignment_node, aggregation_meta_data, **parameters):
                                """
                                Factory function for creating factors inside a workflow

                                :param aggregation_meta_data: the meta data to aggregate over
                                :param sources: source nodes
                                :param alignment_node: alignment node
                                :return: the created factor
                                :type sources: list[Node] | tuple[Node] | None

                                """
                                if not self.current_workflow:
                                    raise ValueError("No workflow context - use create_workflow first")

                                # find matching tools (possibly different parameters)
                                matches = [f for f in self.current_workflow.factors if
                                           f.tool.__class__ == tool_func]
                                # make sure parameters are all the same
                                full_matches = [m for m in matches if m.sources == sources
                                                and m.alignment_node == alignment_node
                                                and dict(m.tool.parameters_dict) == parameters]

                                if len(full_matches) == 1:
                                    tool = full_matches[0].tool
                                else:
                                    tool = tool_func(aggregation_meta_data=aggregation_meta_data, **parameters)

                                return dict(
                                    workflow=self.current_workflow,
                                    tool=tool,
                                    sources=sources,
                                    alignment_node=alignment_node)

                            return tool_factory_function
                        elif base == SelectorTool:
                            def tool_factory_function(sources, selector_meta_data, **parameters):
                                """
                                Factory function for creating factors inside a workflow

                                :param selector_meta_data: the meta data to select over
                                :param sources: source nodes
                                :return: the created factor
                                :type sources: list[Node] | tuple[Node] | None

                                """
                                if not self.current_workflow:
                                    raise ValueError("No workflow context - use create_workflow first")

                                # find matching tools (possibly different parameters)
                                matches = [f for f in self.current_workflow.factors if
                                           f.tool.__class__ == tool_func]
                                # make sure parameters are all the same
                                full_matches = [m for m in matches if m.sources == sources
                                                and m.selector_meta_data == selector_meta_data
                                                and dict(m.tool.parameters_dict) == parameters]

                                if len(full_matches) == 1:
                                    tool = full_matches[0].tool
                                else:
                                    tool = tool_func(selector_meta_data=selector_meta_data, **parameters)

                                return dict(
                                    workflow=self.current_workflow,
                                    tool=tool,
                                    sources=sources)

                            return tool_factory_function
                        elif base == PlateCreationTool:
                            def tool_factory_function(source, **parameters):
                                """
                                Factory function for creating factors inside a workflow

                                :param source: source node
                                :return: the created factor
                                :type source: Node

                                """
                                if not self.current_workflow:
                                    raise ValueError("No workflow context - use create_workflow first")

                                return dict(
                                    workflow=self.current_workflow,
                                    tool=tool_func(**parameters),
                                    source=source)

                            return tool_factory_function
                        else:
                            raise NotImplementedError

                    setattr(factor_container, tool_stream.name, create_factory_function(tool_function))

                except (NameError, AttributeError, ImportError) as e:
                    logging.warn('Unable to load tool {}: {}'.format(tool_stream.name, e))
