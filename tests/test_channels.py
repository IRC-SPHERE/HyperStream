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
import sys

from hyperstream import Stream, StreamId, TimeInterval, StreamAlreadyExistsError, StreamNotFoundError
from hyperstream.utils import MIN_DATE, utcnow
from .helpers import *


class TestChannels(unittest.TestCase):
    def test_memory_channel(self):
        with HyperStream(file_logger=False, console_logger=False, mqtt_logger=None) as hs:
            M = hs.channel_manager.memory
            sid = StreamId(sys._getframe().f_code.co_name)
            M.create_stream(sid)
            self.assertRaises(StreamAlreadyExistsError, M.create_stream, sid)
            M.purge_stream(sid, remove_definition=True)
            self.assertRaises(StreamNotFoundError, M.find_stream, name=sid.name)

    def test_tool_channel(self):
        with HyperStream(file_logger=False, console_logger=False, mqtt_logger=None) as hs:
            T = hs.channel_manager.tools

            # Load in the objects and print them
            clock_stream = T["clock"]
            assert(isinstance(clock_stream, Stream))
            # assert(clock_stream.modifier == Last() + IData())

            agg = T["aggregate"].window((MIN_DATE, utcnow())).items()
            assert(len(agg) > 0)
            # noinspection PyTypeChecker
            assert(agg[0].timestamp == datetime(2016, 10, 26, 0, 0, tzinfo=UTC))
            assert(isinstance(agg[0].value, type))

    def test_tool_channel_new_api(self):
        with HyperStream(file_logger=False, console_logger=False, mqtt_logger=None) as hs:
            M = hs.channel_manager.memory

            # new way of loading tools
            clock_new = hs.tools.clock()

            # old way of loading tools
            clock_old = hs.channel_manager.tools["clock"].window((MIN_DATE, utcnow())).last().value()

            # TODO: NOTE THAT IF WE DO IT THE OLD WAY FIRST, THEN THE NEW WAY FAILS WITH:
            # TypeError: super(type, obj): obj must be an instance or subtype of type
            # which possibly relates to:
            # https://stackoverflow.com/questions/9722343/python-super-behavior-not-dependable

            ticker_old = M.get_or_create_stream("ticker_old")
            ticker_new = M.get_or_create_stream("ticker_new")

            now = utcnow()
            before = (now - timedelta(seconds=30)).replace(tzinfo=UTC)
            ti = TimeInterval(before, now)

            clock_old.execute(sources=[], sink=ticker_old, interval=ti)
            clock_new.execute(sources=[], sink=ticker_new, interval=ti)

            self.assertListEqual(ticker_old.window().values(), ticker_new.window().values())

    def test_plugins(self):
        with HyperStream(file_logger=False, console_logger=False, mqtt_logger=None) as hs:
            M = hs.channel_manager.memory

            clock_tool = hs.tools.clock()
            dummy_tool = hs.plugins.example.tools.dummy()

            ticker = M.get_or_create_stream("ticker")
            ticker_copy = M.get_or_create_stream("ticker_copy")

            before = now - timedelta(seconds=30)
            ti = TimeInterval(before, now)

            clock_tool.execute(sources=[], sink=ticker, interval=ti)
            dummy_tool.execute(sources=[ticker], sink=ticker_copy, interval=ti)

            assert (all(map(lambda pair: pair[0].value == pair[1].value, zip(ticker.window(), ticker_copy.window()))))
