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
from __future__ import absolute_import
import logging
from collections import OrderedDict
from copy import deepcopy
from .utils import Printable
from .scope import Scope


class Flow(Printable):
    def __init__(self, stream_collection, name, description, scopes, streams):
        self.name = name
        self.description = description
        self.scopes = {}
        self.streams = OrderedDict()
        # self.stream_scopes = {}

        for sc in scopes:
            scope = Scope(sc, **scopes[sc])
            self.scopes[sc] = scope

        for s in streams:
            stream = deepcopy(stream_collection.streams[s['stream_id']])
            stream.scope = self.scopes[s['scope']]

            # TODO: Trouble with this deep copy is that the sources are new objects, rather than pointing to the parents
            # Current workaround is to look for parents in the object
            self.update_sources(stream)

            # self.set_scopes(stream_collection, stream, s['scope'])
            self.streams[stream.stream_id] = stream
            # self.stream_scopes[stream] = self.scopes[s['scope']]

    def execute(self, clients, configs):
        print(self)
        for s in self.streams:
            self.streams[s].execute(clients, configs)

    def __repr__(self):
        return str(self)

    def update_sources(self, stream):
        for i in range(len(stream.sources)):
            if stream.sources[i].stream_id in self.streams:
                stream.sources[i] = self.streams[stream.sources[i].stream_id]
            self.update_sources(stream.sources[i])

    # def set_scopes(self, stream_collection, stream, scope_id):
    #     stream.scope = self.scopes[scope_id]
    #     for s in stream.sources:
    #         self.set_scopes(stream_collection, s, stream_collection.streams)
