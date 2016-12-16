# The MIT License (MIT) # Copyright (c) 2014-2017 University of Bristol
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
#  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
#  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
#  OR OTHER DEALINGS IN THE SOFTWARE.

from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count
import numpy as np
from collections import Counter


def safe_key(item):
    k, v = item
    return str(k).replace(".", "__dot__").replace("$", "__dollar__"), v


class HistogramsToCsv(SelectorTool):
    """
    For each document assumed to be a list of numbers, calculate the histogram
    """
    def __init__(self, first_break=0, break_width=1, n_breaks=101, breaks=None, categorical=False):
        super(HistogramsToCsv, self).__init__(
            first_break=first_break,
            break_width=break_width,
            n_breaks=n_breaks,
            breaks=breaks,
            categorical=categorical)

    def _execute(self, sources, interval):
        if self.categorical:
            for t, d in sources[0].window(interval, force_calculation=True):
                yield StreamInstance(t, dict(map(safe_key, Counter(d).items())))
        else:
            if self.breaks is not None:
                breaks = self.breaks
            else:
                breaks = [self.first_break+i*self.break_width for i in range(self.n_breaks)]
            breaks = [-np.inf]+breaks+[np.inf]
            for t, d in sources[0].window(interval, force_calculation=True):
                yield StreamInstance(t, np.histogram(d, breaks)[0].tolist())
