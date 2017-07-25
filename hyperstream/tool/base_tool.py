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

from ..models import ToolModel, ToolParameterModel
from ..utils import Printable, Hashable, func_dump, func_load, camel_to_snake

import logging
import pickle


class BaseTool(Printable, Hashable):
    """
    Base class for all tools
    """
    def __init__(self, **kwargs):
        """
        Base class initializer. Note that this performs setattr on all of the keyword arguments, so this does not have
        to be done manually inside the tools.

        :param kwargs: The keyword arguments
        """
        if kwargs:
            logging.debug('Defining a {} tool with parameters {}'.format(self.__class__.__name__, kwargs))
        else:
            logging.debug('Defining a {} tool'.format(self.__class__.__name__))
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def __eq__(self, other):
        """
        Equality test

        :param other: the other tool
        :return: whether self and other are equal
        """
        return isinstance(other, BaseTool) and hash(self) == hash(other)

    def message(self, interval):
        """
        Get the execution message

        :param interval: The time interval
        :return: The execution message
        """
        return '{} running from {} to {}'.format(self.__class__.__name__, str(interval.start), str(interval.end))

    @property
    def name(self):
        """
        Get the name of the tool, converted to snake format (e.g. "splitter_from_stream")

        :return: The name
        """
        return camel_to_snake(super(BaseTool, self).name)

    @name.setter
    def name(self, value):
        """
        Set the name

        :param value: The name
        :return: None
        """
        self._name = value

    @property
    def parameters_dict(self):
        """
        Get the tool parameters as a simple dictionary

        :return: The tool parameters
        """
        d = {}
        for k, v in self.__dict__.items():
            if not k.startswith("_"):
                d[k] = v
        return d

    @property
    def parameters(self):
        """
        Get the tool parameters

        :return: The tool parameters along with additional information (whether they are functions or sets)
        """
        parameters = []
        for k, v in self.__dict__.items():
            if k.startswith("_"):
                continue

            is_function = False
            is_set = False

            if callable(v):
                value = pickle.dumps(func_dump(v))
                is_function = True
            elif isinstance(v, set):
                value = list(v)
                is_set = True
            else:
                value = v

            parameters.append(dict(
                key=k,
                value=value,
                is_function=is_function,
                is_set=is_set
            ))

        return parameters

    @staticmethod
    def parameters_from_model(parameters_model):
        """
        Get the tool parameters model from dictionaries

        :param parameters_model: The parameters as a mongoengine model
        :return: The tool parameters as a dictionary
        """
        parameters = {}
        for p in parameters_model:
            if p.is_function:
                code, defaults, closure = pickle.loads(p.value)
                parameters[p.key] = func_load(code, defaults, closure, globs=globals())
            elif p.is_set:
                parameters[p.key] = set(p.value)
            else:
                parameters[p.key] = p.value
        return parameters

    @staticmethod
    def parameters_from_dicts(parameters):
        """
        Get the tool parameters model from dictionaries

        :param parameters: The parameters as dictionaries
        :return: The tool parameters model
        """
        return map(lambda p: ToolParameterModel(**p), parameters)

    def get_model(self):
        """
        Gets the mongoengine model for this tool, which serializes parameters that are functions

        :return: The mongoengine model. TODO: Note that the tool version is currently incorrect (0.0.0)
        """

        return ToolModel(
            name=self.name,
            version="0.0.0",
            parameters=self.parameters_from_dicts(self.parameters)
        )

    @staticmethod
    def write_to_history(**kwargs):
        """
        Write to the history of executions of this tool

        :param kwargs: keyword arguments describing the executions
        :return: None
        """
        from hyperstream import HyperStream
        hs = HyperStream(loglevel=logging.CRITICAL, file_logger=False, console_logger=False, mqtt_logger=None)
        if hs.current_session:
            hs.current_session.write_to_history(**kwargs)
