"""
The MIT License (MIT)
Copyright (c) 2014-2017 University of Bristol

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
from collections import OrderedDict
# from copy import deepcopy
from ..utils import Printable
from ..stream import Stream


class StreamCollection(Printable):
    streams = {}

    def __init__(self, kernel_collection):
        with open("stream_ids.json", 'r') as f:
            stream_ids = json.load(f, object_pairs_hook=OrderedDict)
            for stream_id in stream_ids:
                d = stream_ids[stream_id]
                kernel = kernel_collection.kernels[d['kernel_id']]

                sources = []
                if d['sources']:
                    logging.info("Parsing sources [ " + ", ".join(d['sources']) + " ]")
                    for source in d['sources']:
                        if source not in self.streams:
                            logging.error("Source stream not yet defined: " + source)
                            continue
                        else:
                            sources.append(self.streams[source])

                self.streams[stream_id] = Stream(
                    stream_id=stream_id,
                    kernel=kernel,
                    sources=sources,
                    parameters=d['parameters'],
                    stream_type=d['stream_type'],
                    filters=d['filters']
                )
