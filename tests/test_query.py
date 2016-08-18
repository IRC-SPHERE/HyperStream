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

from hyperstream import StreamId, HyperStreamConfig, OnlineEngine, TimeInterval, TimeIntervals, modifiers, UTC
from sphere_connector_package.sphere_connector import SphereConnector, SphereLogger

# Various constants
t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
t2 = datetime(2016, 4, 29, 13, 0, 0, 0, UTC)
second = timedelta(seconds=1)
minute = timedelta(minutes=1)

# Hyperstream setup
sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.CRITICAL)
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
count = StreamId('count')

# Various channels
M = online_engine.channels.memory_channel
S = online_engine.channels.sphere_channel
T = online_engine.channels.tool_channel


class HyperStringTests(unittest.TestCase):
    def test_time_interval(self):
        now = datetime(2016, 1, 1, 0, 0, 0)
        minute = timedelta(minutes=1)
        hour = timedelta(hours=1)
        
        i1 = TimeIntervals([
            TimeInterval(now, now + hour),
            TimeInterval(now + 2 * hour, now + 3 * hour),
        ])
        
        i2 = TimeIntervals([
            TimeInterval(now + 30 * minute, now + 30 * minute + 2 * hour),
        ])
        
        # print(i1)
        assert (i1 == TimeIntervals(intervals=[TimeInterval(start=datetime(2016, 1, 1, 0, 0),
                                                            end=datetime(2016, 1, 1, 1, 0)),
                                               TimeInterval(start=datetime(2016, 1, 1, 2, 0),
                                                            end=datetime(2016, 1, 1, 3, 0))]))
        
        # print(i2)
        # print()
        s = i1 + i2
        assert (s == TimeIntervals(intervals=[TimeInterval(start=datetime(2016, 1, 1, 0, 0),
                                                           end=datetime(2016, 1, 1, 3, 0))]))
        
        d = i1 - i2
        
        assert (d == TimeIntervals(intervals=[TimeInterval(start=datetime(2016, 1, 1, 0, 0),
                                                           end=datetime(2016, 1, 1, 0, 30)),
                                              TimeInterval(start=datetime(2016, 1, 1, 2, 30),
                                                           end=datetime(2016, 1, 1, 3, 0))]))
        # print(s)
        # print(d)
        # print()
    
    def test_simple_query(self):
        # Simple querying
        el = S[e, t1, t1 + minute, modifiers.Component('electricity-04063') + modifiers.List()]()
        edl = S[e, t1, t1 + minute, modifiers.Component('electricity-04063') + modifiers.Data() + modifiers.List()]()
        
        q1 = "\n".join("=".join(map(str, ee)) for ee in el)
        
        # print(q1)
        # print(edl)
        
        assert (q1 == '2016-04-28 20:00:00.159000+00:00=0.0\n'
                      '2016-04-28 20:00:06.570000+00:00=0.0\n'
                      '2016-04-28 20:00:12.732000+00:00=0.0\n'
                      '2016-04-28 20:00:25.125000+00:00=0.0\n'
                      '2016-04-28 20:00:31.405000+00:00=0.0\n'
                      '2016-04-28 20:00:50.132000+00:00=0.0')
        assert (edl == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    
    def test_windowed_querying_average(self):
        M[every30s] = T[clock](stride=30 * second)
        mk_30s = S[e, -30 * second, timedelta(0), modifiers.Component('motion-S1_K')]
        
        M[average] = T[merge](
            timer=M[every30s],
            data=mk_30s,
            func=modifiers.Data() + modifiers.Average()
        )
        
        aa = M[average, t1, t1 + 5 * minute, modifiers.Data() + modifiers.List()]()
        
        assert (aa == [0.0, 0.25, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    
    def test_windowed_querying_count(self):
        M[every30s] = T[clock](stride=30 * second)
        mk_30s = S[e, -30 * second, timedelta(0), modifiers.Component('motion-S1_K')]
        
        M[count] = T[merge](
            timer=M[every30s],
            data=mk_30s,
            func=modifiers.Data() + modifiers.Count()
        )
        
        cc = M[count, t1, t1 + 5 * minute, modifiers.Data() + modifiers.List()]()
        
        assert (cc == [3, 4, 4, 3, 3, 3, 3, 3, 3, 3])


if __name__ == '__main__':
    unittest.main()
