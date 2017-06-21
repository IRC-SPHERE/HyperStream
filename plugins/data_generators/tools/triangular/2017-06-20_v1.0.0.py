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

from hyperstream import Tool, StreamInstance, ToolExecutionError
from hyperstream.utils import check_input_stream_count

import random


class Triangular(Tool):
    """
    Return a random floating point number N such that low <= N <= high and with the specified mode between those bounds. 
    The low and high bounds default to zero and one. The mode argument defaults to the midpoint between the bounds, 
    giving a symmetric distribution.
    
    Optionally initialize internal state of the random number generator using seed.
    """
    def __init__(self, low=0.0, high=1.0, mode=None, seed=None):
        super(Triangular, self).__init__(low=low, high=high, mode=mode, seed=seed)
        random.seed(self.seed)

    @check_input_stream_count(0)
    def _execute(self, sources, alignment_stream, interval):
        if alignment_stream is None:
            raise ToolExecutionError("Alignment stream expected")

        for ti, _ in alignment_stream.window(interval, force_calculation=True):
            yield StreamInstance(ti, random.triangular(low=self.low, high=self.high, mode=self.mode))
