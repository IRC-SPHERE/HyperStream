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

import simplejson as json
import logging
from sphere_connector.utils import Printable
from . import Stream

TASK_PATH = 'tasks'

class Tasks(Printable):
    tasks = []
    def __init__(self):
        for fname in os.listdir(TASK_PATH):
            if fname.endswith(".json") and fname is not "skeleton.json":
                try:
                    logging.info('Reading ' + fname)
                    with open(fname, 'r') as f:
                        taskObj = json.load(f)
                        task = Task(**taskObj)
                        self.tasks.append(task)
                except (OSError, IOError) as e:
                    raise logging.error(fname + ' error: ' + str(e))

class Task(Printable):
    def __init__(self, scope, streams):
        self.scope = scope
        self.streams = [Stream(**s) for s in streams]
