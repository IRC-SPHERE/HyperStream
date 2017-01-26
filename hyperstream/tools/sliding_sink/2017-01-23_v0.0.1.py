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

from hyperstream import TimeInterval
from hyperstream.tool import Tool, check_input_stream_count

class SlidingSink(Tool):
    def __init__(self):
        super(SlidingSink, self).__init__()

    # noinspection PyCompatibility
    @check_input_stream_count(2)
    def _execute(self, sources, alignment_stream, interval):
        sliding_window = sources[0].window(interval, force_calculation=True)
        result = sliding_window.first()
        if result is None:
            return
        sources[1].window(TimeInterval(interval.start, interval.end), force_calculation=True).first()
        return
        yield # required to make this function into a generator

