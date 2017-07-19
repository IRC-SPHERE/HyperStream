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

from hyperstream import HyperStream, StreamId, StreamInstance, TimeInterval
from hyperstream.utils import utcnow, UTC
from datetime import timedelta
import os


os.chdir(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))


if __name__ == '__main__':
    hs = HyperStream(loglevel=20)
    M = hs.channel_manager.memory
    T = hs.channel_manager.tools
    A = hs.channel_manager.assets
    test_assets = StreamId(name="test_assets")
    A.get_or_create_stream(test_assets)
    A.write_to_stream(test_assets, StreamInstance(utcnow(), {'a', 'b', 'c'}))
    print(list(A[test_assets].window().tail(5)))
