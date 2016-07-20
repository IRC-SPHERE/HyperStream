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
# sys.path.insert(1, os.path.join(os.path.dirname(os.path.realpath(__file__)), "sphere_connector_package"))
from collections import FlowCollection, KernelCollection, StreamCollection
from client import Client


class OnlineEngine(object):
    def __init__(self, configs):
        self.sphere_client = Client(configs['sphere_connector'].mongo)
        self.client = Client(configs['hyperstream'].mongo)
        self.clients = {'hyperstream': self.client, 'sphere': self.sphere_client}
        self.configs = configs

        self.kernels = KernelCollection(configs['hyperstream'].kernel_path)
        self.streams = StreamCollection(self.kernels)
        self.flows = FlowCollection(self.streams, configs['hyperstream'].flow_path)

    def execute(self):
        self.flows.execute_all(self.clients, self.configs)
