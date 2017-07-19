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
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

import unittest
import logging

from hyperstream import HyperStream
from hyperstream.workflow.workflow import Workflow

from hyperstream import StreamId, TimeInterval
from pytz import UTC
from datetime import datetime, timedelta


class HyperStreamTests(unittest.TestCase):
    def setUp(self):
        self.hs = HyperStream(loglevel=logging.INFO, file_logger=False,
                              console_logger=False)

    def test___init__(self):
        self.assertIs(type(self.hs), HyperStream)

    def test___repr__(self):
        self.assertIs(type(self.hs.__repr__()), str)

    def test___str__(self):
        self.assertIs(type(self.hs.__str__()), str)

    def test_create_workflow(self):
        workflow_id = 1
        name = 'test_workflow'
        owner = 'unittest'
        description = 'test of workflow'
        w = self.hs.create_workflow(workflow_id, name, owner, description)
        self.assertIs(type(w), Workflow)
        self.assertEqual(w.workflow_id, workflow_id)
        self.assertEqual(w.name, name)
        self.assertEqual(w.owner, owner)
        self.assertEqual(w.description, description)

    def test_usecase_1(self):
        M = self.hs.channel_manager.memory
        T = self.hs.channel_manager.tools

        clock = StreamId(name="clock")
        clock_tool = T[clock].window().last().value()
        s_ticker = M.get_or_create_stream(stream_id=StreamId(name="ticker"))

        now = datetime.utcnow().replace(tzinfo=UTC)
        before = (now - timedelta(seconds=30)).replace(tzinfo=UTC)
        ti = TimeInterval(before, now)
        clock_tool.execute(sources=[], sink=s_ticker, interval=ti,
                           alignment_stream=None)
        items = s_ticker.window().items()
        timestamps, values = zip(*[(it.timestamp, it.value) for it in items])

        self.assertItemsEqual(timestamps, values)

        before_s = before.replace(microsecond=0)
        expected = [before_s + timedelta(seconds=i) for i in range(1, 31)]
        self.assertItemsEqual(values, expected)


if __name__ == "__main__":
    unittest.main()
