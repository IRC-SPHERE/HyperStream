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
import logging
from copy import deepcopy
from sphere_connector_package.sphere_connector.utils import Printable
from scope import Scope


class Flow(Printable):
    def __init__(self, stream_collection, name, description, scopes, streams):
        self.name = name
        self.description = description
        self.scopes = {}
        self.streams = []

        for sc in scopes:
            scope = Scope(sc, **scopes[sc])
            self.scopes[sc] = scope

        for s in streams:
            stream = deepcopy(stream_collection.streams[s['stream_id']])
            stream.scope = self.scopes[s['scope']]
            self.streams.append(stream)

    def execute(self):
        print(self)
        for s in self.streams:
            s.execute()

    def __repr__(self):
        return str(self)
