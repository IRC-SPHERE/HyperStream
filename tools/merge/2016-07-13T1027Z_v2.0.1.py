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
from hyperstream import Tool
import logging


class Merge(Tool):
    def execute(self, stream_def, start, end, writer, timer, data, func):
        logging.info('Merge running from ' + str(start) + ' to ' + str(end))
        rel_start = stream_def.kwargs['data'].start
        rel_end = stream_def.kwargs['data'].end
        window = []
        future = []
        for (t, _) in timer:
            while (len(window) > 0) and (window[0][0] <= t + rel_start):
                window = window[1:]
            while (len(future) > 0) and (future[0][0] <= t + rel_end):
                doc = future[0]
                future = future[1:]
                if t + rel_start < doc[0] <= t + rel_end:
                    window.append(doc)
            while True:
                try:
                    doc = next(data)
                    if t + rel_start < doc[0] <= t + rel_end:
                        window.append(doc)
                    elif doc[0] > t + rel_end:
                        future.append(doc)
                        break
                except StopIteration:
                    break
            # single-document case:
            writer([(t, func((doc for doc in window)))])
            # multi-document case:

# for x in func( (doc for doc in window) ):
#        writer([(t,x)])
