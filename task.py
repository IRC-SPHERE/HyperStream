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
from sphere_connector.utils import Printable
from stream import Stream
from copy import deepcopy

class Tasks(Printable):
    tasks = []
    def __init__(self, all_streams, path):
        for fname in os.listdir(path):
            if fname.endswith(".json") and fname != "skeleton.json":
                try:
                    logging.info('Reading ' + fname)
                    with open(os.path.join(path, fname), 'r') as f:
                        taskObj = json.load(f)
                        task = Task(all_streams, **taskObj)
                        self.tasks.append(task)
                except (OSError, IOError) as e:
                    logging.error(str(fname) + ' error: ' + str(e))

class Task(Printable):
    def __init__(self, all_streams, name, description, scopes, streams):
        self.name = name
        self.description = description
        self.scopes = scopes
        self.streams = []
        for s in streams:
            stream = deepcopy(all_streams[s['streamId']])
            sources = s['sources']
            if sources:
                logging.info("Parsing sources [ " + ", ".join(sources) + " ]")
                for s in sources:
                    if s not in all_streams:
                        logging.error("Source not found: " + s)
                    else:
                        # logging.info("Found source: " + s)
                        stream.sources.append(deepcopy(all_streams[s]))
            self.streams.append(stream)

    def execute(self):
        print(self)
        for s in self.streams:
            s.execute()
