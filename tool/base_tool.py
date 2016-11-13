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

from ..utils import Printable, Hashable

import logging


class BaseTool(Printable, Hashable):
    """
    Base class for all tools
    """
    def __init__(self, **kwargs):
        if kwargs:
            logging.debug('Defining a {} tool with parameters {}'.format(self.__class__.__name__, kwargs))
        else:
            logging.debug('Defining a {} tool'.format(self.__class__.__name__))

    def __eq__(self, other):
        # TODO: requires a unit test
        return isinstance(other, BaseTool) and hash(self) == hash(other)

    def message(self, interval):
        return '{} running from {} to {}'.format(self.__class__.__name__, str(interval.start), str(interval.end))

    @property
    def name(self):
        # return self.__class__.__module__
        return super(BaseTool, self).name

    @name.setter
    def name(self, value):
        self._name = value
