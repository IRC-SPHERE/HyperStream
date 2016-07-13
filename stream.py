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
from utils import Printable


class Stream(Printable):
    scope = None
    completed = False

    def __init__(self, stream_id, kernel, sources, parameters, stream_type):
        self.stream_id = stream_id
        self.kernel = kernel
        self.sources = sources
        self.parameters = parameters
        self.stream_type = stream_type

    def execute(self, clients, configs):
        logging.info("Executing stream " + self.stream_id)

        if not self.scope:
            raise RuntimeError("No scope for stream " + self.stream_id)

        # Ensure all sources have been executed, if not, execute
        if self.sources:
            logging.info("Looping through sources")
            for s in self.sources:
                # TODO: more logic here
                if not self.completed:
                    s.execute(clients, configs)

        # Now execute the kernel
        self.kernel.runner.execute(self, clients, configs)

        # TODO: Clearly more logic needed here!
        self.completed = True

    def __repr__(self):
        return str(self)
