# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

from hyperstream import Tool, StreamInstance
from hyperstream.utils import check_input_stream_count

from dateutil.parser import parse


class CsvReader(Tool):
    def __init__(self, filename):
        super(CsvReader, self).__init__(filename=filename)

    @check_input_stream_count(0)
    def _execute(self, sources, alignment_stream, interval):

        # Let's make the assumption that the first field is the timestamp

        first = True

        with open(self.filename, 'rU') as f:
            for line in f.readlines():
                if first:
                    first = False
                    continue
                elements = line.split(',')
                dt = parse(elements[0])
                if dt in interval:
                    yield StreamInstance(dt, map(float, elements[1:]))
