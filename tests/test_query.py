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

from hyperstream import TimeInterval, TimeIntervals, Workflow, RelativeTimeInterval  # , StreamView
from hyperstream.itertools2 import online_average, count as online_count
from hyperstream.utils import MIN_DATE
from helpers import *


# noinspection PyMethodMayBeStatic
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
        ti = TimeInterval(t1, t1 + minute)

        elec = M.create_stream(stream_id=StreamId('electricity'))
        env = S.create_stream(stream_id=StreamId('environmental_data'))

        env_tool = channels.get_tool2("sphere", dict(modality="environmental"))
        elec_tool = T[component].items()[-1].value(key='electricity-04063')

        env_tool.execute(input_streams=None, interval=ti, writer=env.writer)
        elec_tool.execute(input_streams=[env], interval=ti, writer=elec.writer)

        q1 = "\n".join("=".join(map(str, ee)) for ee in elec)
        
        # print(q1)
        # print(edl)
        
        assert (q1 == '2016-04-28 20:00:00.159000+00:00=0.0\n'
                      '2016-04-28 20:00:06.570000+00:00=0.0\n'
                      '2016-04-28 20:00:12.732000+00:00=0.0\n'
                      '2016-04-28 20:00:25.125000+00:00=0.0\n'
                      '2016-04-28 20:00:31.405000+00:00=0.0\n'
                      '2016-04-28 20:00:50.132000+00:00=0.0')

        assert (elec.values() == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    
    # def test_windowed_querying_average(self):
    #     ti = TimeInterval(t1, t1 + 5 * minute)
    #     rel = RelativeTimeInterval(-30 * second, 0)
    #
    #     # Create some memory streams
    #     M.create_stream(stream_id=every30s)
    #     M.create_stream(stream_id=m_kitchen_30_s_window)
    #     M.create_stream(stream_id=average)
    #
    #     # Create the tools
    #     clock_tool = channels.get_tool2(clock, dict(first=MIN_DATE, stride=30*second))
    #     motion_tool = channels.get_tool2(component, dict(key='motion-S1_K'))
    #     average_tool = channels.get_tool2(aggregate, dict(func=lambda x: online_average(map(lambda xi: xi.value, x))))
    #
    #     # Execute the tools
    #     clock_tool.execute(None, ti, M[every30s].writer)
    #     motion_tool.execute([S[environmental]], ti, M[m_kitchen_30_s_window].writer)
    #     average_tool.execute([M[every30s], M[m_kitchen_30_s_window]], rel, M[average].writer)
    #
    #     # mk_30s = M[m_kitchen_30_s_window].relative_window((-30 * second, 0))
    #     # aa = M[average].execute((t1, t1 + 5 * minute)).values()
    #     aa = M[average].values()
    #     logging.info(aa)
    #     assert (aa == [0.0, 0.25, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    
    # def test_windowed_querying_count(self):
    #     ti = TimeInterval(t1, t1 + 5 * minute)
    #
    #     M.create_stream(stream_id=count)
    #     counter = channels.get_tool2(aggregate, dict(func=online_count))
    #     counter.execute([M[every30s], M[m_kitchen_30_s_window]], ti, M[count].writer)
    #
    #     # cc = M[count].execute((t1, t1 + 5 * minute)).values()
    #     cc = M[count].values()
    #     logging.info(cc)
    #     assert (cc == [3, 4, 4, 3, 3, 3, 3, 3, 3, 3])

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
                        # sources=None, sink=StreamView(stream=node.streams[0], time_interval=time_interval))
                        sources=None, sink=node)

        # Execute the workflow
        w.execute(time_interval)

        # Check the values
        assert (node.streams[('house', '1'), ].values()[:1] ==
                [{u'electricity-04063': 0.0, 'noise': None, 'door': None, 'uid': u'04063', 'electricity': None,
                  'light': None, 'motion': None, 'dust': None, 'cold-water': None, 'humidity': None, 'hot-water': None,
                  'temperature': None}])

    def test_chained_query(self):
        interval = TimeInterval(t1, t1 + 5 * minute)
        
        # Define sliding window stream
        stream_memory_sliding_window = M.create_stream(StreamId('stream_id_memory_sliding_window'))
        
        stream_tool_sliding_window = channels.get_tool2(
            tool="sliding_window",
            tool_parameters=dict(
                first=MIN_DATE,
                lower=timedelta(seconds=-30),
                upper=timedelta(seconds=0),
                increment=timedelta(seconds=10)
            )
        )
        
        stream_tool_sliding_window.execute(
            input_streams=None,
            interval=interval,
            writer=stream_memory_sliding_window.writer
        )
        
        assert str(stream_memory_sliding_window.values()[0]) == '(2016-04-28 20:00:00+00:00, 2016-04-28 20:00:30+00:00]'
        
        # Define the motion in kitchen tool
        
        stream_sphere_environmental = S.create_stream(StreamId('stream_id_memory_environmental'))
        
        stream_tool_sphere_environmental = channels.get_tool2(
            tool='sphere',
            tool_parameters=dict(
                modality='environmental'
            )
        )
        
        stream_tool_sphere_environmental.execute(
            input_streams=None,
            interval=interval,
            writer=stream_sphere_environmental.writer
        )
        
        assert stream_sphere_environmental.values()[0] == {u'electricity-04063': 0.0, 'noise': None, 'door': None, 'uid': u'04063', 'electricity': None, 'light': None, 'motion': None, 'dust': None, 'cold-water': None, 'humidity': None, 'hot-water': None, 'temperature': None}

        
        # Filter the motion in kitchen
        stream_memory_motion = M.create_stream(StreamId('id_memory_m_kitchen'))
        
        tool_motion = channels.get_tool2(
            tool='component',
            tool_parameters=dict(
                key='motion-S1_K'
            )
        )
        
        tool_motion.execute(
            input_streams=[stream_sphere_environmental],
            interval=interval,
            writer=stream_memory_motion.writer
        )
        
        assert stream_memory_motion.values() == [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

        
        # Aggregate over the window
        stream_memory_m_kitchen_mean = M.create_stream(StreamId('id_memory_m_kitchen_mean'))
        
        tool_sliding_apply = channels.get_tool2(
            tool='sliding_apply',
            tool_parameters=dict(
                func=online_average
            )
        )
        
        tool_sliding_apply.execute(
            input_streams=[
                stream_memory_sliding_window,
                stream_memory_motion
            ],
            interval=interval,
            writer=stream_memory_m_kitchen_mean.writer
        )
        
        assert(stream_memory_m_kitchen_mean.values() == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.33333333333333337, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

if __name__ == '__main__':
    unittest.main()
