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
import simplejson as json
import logging
from sphere_connector_package.sphere_connector.utils import Printable
from stream import Stream
from copy import deepcopy
from scope import Scope


class FlowCollection(Printable):
    flows = []

    def __init__(self, code_collection, path):
        for fname in os.listdir(path):
            if fname.endswith(".json") and fname != "skeleton.json":
                try:
                    logging.info('Reading ' + fname)
                    with open(os.path.join(path, fname), 'r') as f:
                        flow_definition = json.load(f)
                        flow = Flow(code_collection, **flow_definition)
                        self.flows.append(flow)
                except (OSError, IOError) as e:
                    logging.error(str(fname) + ' error: ' + str(e))

    def execute_all(self):
        for flow in self.flows:
            flow.execute()


class Flow(Printable):
    def __init__(self, code_collection, name, description, scopes, streams):
        self.name = name
        self.description = description
        self.scopes = {}
        self.streams = []

        for sc in scopes:
            scope = Scope(sc, **scopes[sc])
            self.scopes[sc] = scope

        for s in streams:
            code = deepcopy(code_collection.codes[s['code']])

            scope = self.scopes[s['scope']]

            sources = s['sources']
            if sources:
                logging.info("Parsing sources [ " + ", ".join(sources) + " ]")
                for src in sources:
                    if src not in code_collection.codes:
                        logging.error("Code not found: " + src)
                    else:
                        # logging.info("Found source: " + s)
                        sources.append(deepcopy(code_collection.codes[src]))

            stream = Stream(s['stream'], code, scope)
            self.streams.append(stream)

    def execute(self):
        print(self)
        for s in self.streams:
            s.execute()

    def __repr__(self):
        return str(self)
