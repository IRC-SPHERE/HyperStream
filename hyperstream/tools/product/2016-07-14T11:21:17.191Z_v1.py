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


class Product(Tool):
    def normalise_kwargs(self, *args, **kwargs):
        pass

    def normalise_tool(self, *args, **kwargs):
        pass

    def __str__(self):
        return __name__

    def __hash__(self):
        return hash(__name__)

    def process_params(self, stream1, stream2):
        print('Defining a Product stream')
        return [], {'stream1': stream1, 'stream2': stream2}

    def __call__(self, stream_def, start, end, writer, stream1, stream2):
        print('Prod running from ' + str(start) + ' to ' + str(end))
        for (t, data1) in stream1:
            (_, data2) = next(stream2)
            writer([(t, data1 * data2)])
