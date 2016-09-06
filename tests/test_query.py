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

from hyperstream import TimeInterval, TimeIntervals, Workflow, StreamView
from hyperstream.itertools2 import online_average, count as online_count
from helpers import *


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
        env = S[environmental].execute((t1, t1 + minute))

        eid = StreamId('electricity')

        elec_tool = T[component].define(input_streams=[env], key='electricity-04063')
        M.create_stream(stream_id=eid, tool_stream=elec_tool)

        el = M[eid].execute((t1, t1 + minute))

        q1 = "\n".join("=".join(map(str, ee)) for ee in el)
        
        # print(q1)
        # print(edl)
        
        assert (q1 == '2016-04-28 20:00:00.159000+00:00=0.0\n'
                      '2016-04-28 20:00:06.570000+00:00=0.0\n'
                      '2016-04-28 20:00:12.732000+00:00=0.0\n'
                      '2016-04-28 20:00:25.125000+00:00=0.0\n'
                      '2016-04-28 20:00:31.405000+00:00=0.0\n'
                      '2016-04-28 20:00:50.132000+00:00=0.0')

        assert (el.values() == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    
    def test_windowed_querying_average(self):
        M[every30s] = M.create_stream(stream_id=every30s, tool_stream=clock_tool)
        motion_tool = T[component].define(input_streams=[S[environmental]], key='motion-S1_K')
        M.create_stream(stream_id=m_kitchen_30_s_window, tool_stream=motion_tool)

        mk_30s = M[m_kitchen_30_s_window].relative_window((-30 * second, 0))

        averager = T[aggregate].define(
            input_streams=[M[every30s], mk_30s],
            func=lambda x: online_average(map(lambda xi: xi.value, x))
        )

        M.create_stream(stream_id=average, tool_stream=averager)
        
        aa = M[average].execute((t1, t1 + 5 * minute)).values()
        logging.info(aa)
        assert (aa == [0.0, 0.25, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    
    def test_windowed_querying_count(self):
        mk_30s = M[m_kitchen_30_s_window].relative_window((-30 * second, 0))
        
        counter = T[aggregate].define(
            input_streams=[M[every30s], mk_30s],
            func=online_count
        )

        M.create_stream(stream_id=count, tool_stream=counter)

        cc = M[count].execute((t1, t1 + 5 * minute)).values()
        logging.info(cc)
        assert (cc == [3, 4, 4, 3, 3, 3, 3, 3, 3, 3])

    def test_database_channel(self):
        # TODO: some tests go here
        pass

    def test_simple_workflow(self):
        # Create a simple one step workflow for querying
        w = Workflow(
            channels=channels,
            plates=plates,
            workflow_id="simple_query_workflow",
            name="Simple query workflow",
            owner="TD",
            description="Just a test of creating workflows")

        time_interval = TimeInterval(t1, t1 + 1 * minute)

        # Create some streams (collected in a node)
        node = w.create_node(stream_name="environmental", channel=S, plate_ids=["H1"])  # .window((t1, t1 + 1 * minute))

        # Create a factor to produce some data
        w.create_factor(tool_name="sphere", tool_parameters=dict(modality="environmental"),
                        sources=None, sink=StreamView(stream=node.streams[0], time_interval=time_interval))

        # Execute the workflow
        w.execute(time_interval)

        # Check the values
        assert (node.streams[0].values()[:1] ==
                [{u'electricity-04063': 0.0, 'noise': None, 'door': None, 'uid': u'04063', 'electricity': None,
                  'light': None, 'motion': None, 'dust': None, 'cold-water': None, 'humidity': None, 'hot-water': None,
                  'temperature': None}])


if __name__ == '__main__':
    unittest.main()
