"""
The MIT License (MIT)
Copyright (c) 2014-2016 University of Bristol

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

import os
import sys
# sys.path.insert(1, os.path.join(os.path.dirname(os.path.realpath(__file__)), "sphere_connector_package"))
import logging
from sphere_connector_package import sphere_connector as sc
from sphere_connector_package.sphere_connector.config import BasicConfig
from flow import FlowCollection
from code import CodeCollection
from client import Client
try:
    from pymongo.errors import ServerSelectionTimeoutError
except ImportError:
    ServerSelectionTimeoutError = None


def try_connect(basic_config):
    if ServerSelectionTimeoutError:
        try:
            return Client(basic_config.mongo)
        except ServerSelectionTimeoutError as e:
            logging.warn(e.message)
            sys.exit()
    else:
        return Client(basic_config.mongo)


class OnlineEngine(object):
    def __init__(self, hyperstream_config):
        sc.utils.initialise_logger(path='/tmp', filename='hyperstream_online')
        self.sphere_client = try_connect(BasicConfig(True, False))
        self.codes = CodeCollection(hyperstream_config.stream_path)
        self.flows = FlowCollection(self.codes, hyperstream_config.flow_path)
        self.client = Client(hyperstream_config.mongo)

    def execute(self):
        self.flows.execute_all()
