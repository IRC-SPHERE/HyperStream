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
import unittest
import logging
from datetime import datetime, timedelta

from hyperstream.utils import UTC
from hyperstream.config import HyperStreamConfig
from hyperstream.online_engine import OnlineEngine
from hyperstream.stream import StreamId
from hyperstream import modifiers
from sphere_connector_package.sphere_connector import SphereConnector, SphereLogger

# Various constants
t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
t2 = datetime(2016, 4, 29, 13, 0, 0, 0, UTC)
second = timedelta(seconds=1)
minute = timedelta(minutes=1)


# Hyperstream setup
sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.ERROR)
sphere_connector = SphereConnector(include_mongo=True, include_redcap=False, sphere_logger=sphere_logger)
hyperstream_config = HyperStreamConfig()
online_engine = OnlineEngine(hyperstream_config)


# Stream IDs
e = StreamId(name='environmental')
clock = StreamId('clock')
merge = StreamId('merge')
every30s = StreamId('every30s')
motion_kitchen_windowed = StreamId('motion_kitchen_windowed')
m_kitchen_30_s_window = StreamId('m_kitchen_30_s_window')
average = StreamId('average')
sum_ = StreamId('sum')


# Various channels
M = online_engine.channels.memory_channel
S = online_engine.channels.sphere_channel
T = online_engine.channels.tool_channel


class HyperStringTests(unittest.TestCase):
    def test_simple_query(self):
        # Simple querying
        el = S[e, t1, t1 + minute, modifiers.Component('electricity-04063') + modifiers.List()]()
        edl = S[e, t1, t1 + minute, modifiers.Component('electricity-04063') + modifiers.Data() + modifiers.List()]()
        #
        q1 = "\n".join("=".join(map(str, ee)) for ee in el)

        # print(q1)
        # print(edl)

        assert(q1 == '2016-04-28 20:00:00.159000+00:00=0.0\n'
                     '2016-04-28 20:00:06.570000+00:00=0.0\n'
                     '2016-04-28 20:00:12.732000+00:00=0.0\n'
                     '2016-04-28 20:00:25.125000+00:00=0.0\n'
                     '2016-04-28 20:00:31.405000+00:00=0.0\n'
                     '2016-04-28 20:00:50.132000+00:00=0.0')
        assert(edl == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

    def test_windowed_querying(self):
        # Window'd querying
        M[every30s] = T[clock](stride=30 * second)

        M[average] = T[merge](
            timer=M[every30s],
            data=S[e, -30 * second, timedelta(0), modifiers.Component('motion-S1_K')],
            func=modifiers.Data() + modifiers.Average()
        )

        M[sum_] = T[merge](
            timer=M[every30s],
            data=S[e, -30 * second, timedelta(0), modifiers.Component('motion-S1_K')],
            func=modifiers.Data() + modifiers.Sum()
        )

        # aa = M[average, t1, t1 + 5 * minute, modifiers.Data() + modifiers.List()]()
        cc = M[sum_, t1, t1 + 5 * minute, modifiers.Data() + modifiers.List()]()

        # print '\n'.join(map(str, aa))
        assert(cc == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

    # def test_upper(self):
    #     self.assertEqual('foo'.upper(), 'FOO')
    #
    # def test_isupper(self):
    #     self.assertTrue('FOO'.isupper())
    #     self.assertFalse('Foo'.isupper())
    #
    # def test_split(self):
    #     s = 'hello world'
    #     self.assertEqual(s.split(), ['hello', 'world'])
    #     # check that s.split fails when the separator is not a string
    #     with self.assertRaises(TypeError):
    #         s.split(2)

if __name__ == '__main__':
    unittest.main()
