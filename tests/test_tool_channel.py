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

import unittest

from hyperstream import HyperStream, Stream
from hyperstream.utils import MIN_DATE, utcnow
from helpers import *


class TestToolChannel(unittest.TestCase):
    def test_tool_channel(self):

        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        T = hs.channel_manager.tools

        # Load in the objects and print them
        clock_stream = T[clock]
        assert(isinstance(clock_stream, Stream))
        # assert(clock_stream.modifier == Last() + IData())

        agg = T[aggregate].window((MIN_DATE, utcnow())).items()
        assert(len(agg) > 0)
        # noinspection PyTypeChecker
        assert(agg[0].timestamp == datetime(2016, 10, 26, 0, 0, tzinfo=UTC))
        assert(isinstance(agg[0].value, type))
