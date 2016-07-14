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

# import os
import sys
# sys.path.insert(1, os.path.join(os.path.dirname(os.path.realpath(__file__)), "sphere_connector_package"))
import logging
from collections import FlowCollection, KernelCollection, StreamCollection
from client import Client
from mongoengine import connect
try:
    from pymongo.errors import ServerSelectionTimeoutError
except ImportError:
    ServerSelectionTimeoutError = None


def double_connect(basic_config):
    """
    Connects to mongo twice - once using standard pymongo connection, and another using mongoengine. Currently can't
    be peformed in one step, see here: https://github.com/MongoEngine/mongoengine/issues/704
    :param basic_config: Basic configuration
    :return:
    """
    client = Client(basic_config.mongo)
    d = dict((k, v) for k, v in basic_config.mongo.items() if k not in ['modalities', 'summaries'])
    if 'authentication_database' in d:
        d['authentication_source'] = d['authentication_database']
        del d['authentication_database']
    session = connect(**d)
    return [client, session]


def try_connect(basic_config):
    if ServerSelectionTimeoutError:
        try:
            return double_connect(basic_config)
        except ServerSelectionTimeoutError as e:
            logging.warn(e.message)
            sys.exit()
    else:
        return double_connect(basic_config)


class OnlineEngine(object):
    def __init__(self, configs):
        [self.sphere_client, self.session] = try_connect(configs['sphere_connector'])
        self.client = Client(configs['hyperstream'].mongo)
        self.clients = {'hyperstream': self.client, 'sphere': self.sphere_client, 'mongoengine': self.session}
        self.configs = configs

        self.kernels = KernelCollection(configs['hyperstream'].kernel_path)
        self.streams = StreamCollection(self.kernels)
        self.flows = FlowCollection(self.streams, configs['hyperstream'].flow_path)

    def execute(self):
        self.flows.execute_all(self.clients, self.configs)
