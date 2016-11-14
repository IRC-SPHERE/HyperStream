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
import logging

from hyperstream import TimeInterval, TimeIntervals, RelativeTimeInterval
from hyperstream.itertools2 import online_average
from hyperstream.utils import MIN_DATE, utcnow
from helpers import *


# noinspection PyMethodMayBeStatic
class HyperStreamQueryTests(unittest.TestCase):
    def test_time_interval(self):
        now = datetime(2016, 1, 1, 0, 0, 0)

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

        elec = M[StreamId('electricity')]
        env = S.create_stream(stream_id=StreamId('environmental_data'))

        env_tool = channels.get_tool("sphere", dict(modality="environmental"))
        elec_tool = T[component].window((MIN_DATE, utcnow())).last().value(key='electricity-04063')

        env_tool.execute(sources=None, sink=env, alignment_stream=None, interval=ti)
        elec_tool.execute(sources=[env], sink=elec, alignment_stream=None, interval=ti)

        q1 = "\n".join("=".join(map(str, ee)) for ee in elec.window(ti))
        
        print(q1)
        # print(edl)
        
        assert (q1 == '2016-04-28 20:00:00.159000+00:00=0.0\n'
                      '2016-04-28 20:00:06.570000+00:00=0.0\n'
                      '2016-04-28 20:00:12.732000+00:00=0.0\n'
                      '2016-04-28 20:00:25.125000+00:00=0.0\n'
                      '2016-04-28 20:00:31.405000+00:00=0.0\n'
                      '2016-04-28 20:00:50.132000+00:00=0.0')

        assert (elec.window(ti).values() == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    
    def test_alignment_window_querying_average(self):
        ti = TimeInterval(t1, t1 + 5 * minute)
        rel = RelativeTimeInterval(-30 * second, 0)

        # SPHERE stream
        S.get_or_create_stream(stream_id=environmental)

        # Create some memory streams
        M.get_or_create_stream(stream_id=every30s)
        M.get_or_create_stream(stream_id=kitchen)
        M.get_or_create_stream(stream_id=kitchen_motion)
        M.get_or_create_stream(stream_id=m_kitchen_30_s_window)
        M.get_or_create_stream(stream_id=average)
        M.get_or_create_stream(stream_id=clock)

        # Create the tools
        t_env = channels.get_tool("sphere", dict(modality="environmental"))
        t_clock = channels.get_tool(clock, dict(first=MIN_DATE, stride=30*second))
        t_kitchen = channels.get_tool(component_filter, dict(key='uid', values={'S1_K'}, complement=False))
        t_motion = channels.get_tool(component, dict(key='motion'))
        t_rw = channels.get_tool("relative_window", parameters=dict(relative_start=-30, relative_end=0))
        t_ra = channels.get_tool("relative_apply", parameters=dict(func=online_average))

        # Execute the tools
        t_clock.execute(
            alignment_stream=None,
            sources=None,
            sink=M[every30s],
            interval=ti)

        t_env.execute(
            alignment_stream=None,
            sources=None,
            sink=S[environmental],
            interval=ti)

        t_kitchen.execute(
            alignment_stream=None,
            sources=[S[environmental]],
            sink=M[kitchen],
            interval=ti)

        t_motion.execute(
            alignment_stream=None,
            sources=[M[kitchen]],
            sink=M[kitchen_motion],
            interval=ti)

        t_rw.execute(
            alignment_stream=M[every30s],
            sources=[M[kitchen_motion]],
            sink=M[m_kitchen_30_s_window],
            interval=ti)

        t_ra.execute(
            alignment_stream=None,
            sources=[M[m_kitchen_30_s_window]],
            sink=M[average],
            interval=ti)

        import logging
        for kv in M[m_kitchen_30_s_window].window(ti):
            logging.info(kv)

        # mk_30s = M[m_kitchen_30_s_window].relative_window((-30 * second, 0))
        # aa = M[average].execute((t1, t1 + 5 * minute)).values()
        aa = M[average].window(ti).values()
        logging.info(aa)
        assert (aa == [0.0, 0.25, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    
    # def test_windowed_querying_count(self):
    #     ti = TimeInterval(t1, t1 + 5 * minute)
    #
    #     M.create_stream(stream_id=count)
    #     t_count = channel_manager.get_tool(aggregate, dict(func=online_count))
    #     t_count.execute([M[every30s], M[m_kitchen_30_s_window]], M[count], ti)
    #
    #     # cc = M[count].execute((t1, t1 + 5 * minute)).values()
    #     cc = M[count].values()
    #     logging.info(cc)
    #     assert (cc == [3, 4, 4, 3, 3, 3, 3, 3, 3, 3])

    def test_database_channel(self):
        # Simple querying
        ti = TimeInterval(t1, t1 + minute)

        # Get a stream that lives in the database
        env = D[StreamId('environmental_db', {"house": "1"})]

        # Create stream whose source will be the above database stream
        elec = M.create_stream(StreamId('electricity'))

        env_tool = channels.get_tool("sphere", dict(modality="environmental"))
        elec_tool = T[component].window((MIN_DATE, utcnow())).last().value(key='electricity-04063')

        env_tool.execute(sources=None, sink=env, alignment_stream=None, interval=ti)
        elec_tool.execute(sources=[env], sink=elec, alignment_stream=None, interval=ti)

        q1 = "\n".join("=".join(map(str, ee)) for ee in elec.window(ti))

        # print(q1)
        # print(edl)

        assert (q1 == '2016-04-28 20:00:00.159000+00:00=0.0\n'
                      '2016-04-28 20:00:06.570000+00:00=0.0\n'
                      '2016-04-28 20:00:12.732000+00:00=0.0\n'
                      '2016-04-28 20:00:25.125000+00:00=0.0\n'
                      '2016-04-28 20:00:31.405000+00:00=0.0\n'
                      '2016-04-28 20:00:50.132000+00:00=0.0')

        assert (elec.window(ti).values() == [0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

    def test_simple_workflow(self):
        # Create a simple one step workflow for querying
        w = hyperstream.create_workflow(
            workflow_id="simple_query_workflow",
            name="Simple query workflow",
            owner="TD",
            description="Just a test of creating workflows")

        time_interval = TimeInterval(t1, t1 + 1 * minute)

        # Create some streams (collected in a node)
        node = w.create_node(stream_name="environmental", channel=S, plate_ids=["H1"])  # .window((t1, t1 + 1 * minute))

        # Create a factor to produce some data
        w.create_factor(tool=dict(name="sphere", parameters=dict(modality="environmental")),
                        sources=None, sink=node)

        # Execute the workflow
        w.execute(time_interval)

        # Check the values
        assert (node.streams[('house', '1'), ].window(time_interval).first().value ==
                {u'electricity': 0.0, 'uid': u'04063'})

    def test_chained_query(self):
        interval = TimeInterval(t1, t1 + 5 * minute)
        
        # Define sliding window stream
        stream_memory_sliding_window = M.create_stream(StreamId('stream_id_memory_sliding_window'))
        
        stream_tool_sliding_window = channels.get_tool(
            name="sliding_window",
            parameters=dict(
                first=MIN_DATE,
                lower=-30.0,
                upper=0.0,
                increment=10.0
            )
        )
        
        stream_tool_sliding_window.execute(
            sources=None,
            sink=stream_memory_sliding_window,
            alignment_stream=None,
            interval=interval
        )
        
        assert str(stream_memory_sliding_window.window(interval).first().value) == \
            '(2016-04-28 20:00:00+00:00, 2016-04-28 20:00:30+00:00]'
        
        # Define the motion in kitchen tool
        
        stream_sphere_environmental = S.create_stream(StreamId('stream_id_memory_environmental'))
        
        stream_tool_sphere_environmental = channels.get_tool(
            name='sphere',
            parameters=dict(
                modality='environmental'
            )
        )
        
        stream_tool_sphere_environmental.execute(
            sources=None,
            sink=stream_sphere_environmental,
            alignment_stream=None,
            interval=interval
        )

        result = stream_sphere_environmental.window(interval).first().value
        logging.debug(result)
        assert result == {u'electricity': 0.0, 'uid': u'04063'}

        # Filter the motion in kitchen
        stream_memory_kitchen = M.create_stream(StreamId('id_memory_kitchen'))
        stream_memory_motion = M.create_stream(StreamId('id_memory_m_kitchen'))

        tool_kitchen = channels.get_tool(
            name='component_filter',
            parameters=dict(
                key='uid', values={'S1_K'}, complement=False
            )
        )

        tool_kitchen.execute(
            sources=[stream_sphere_environmental],
            sink=stream_memory_kitchen,
            alignment_stream=None,
            interval=interval
        )

        tool_motion = channels.get_tool(
            name='component',
            parameters=dict(
                key='motion'
            )
        )
        
        tool_motion.execute(
            sources=[stream_memory_kitchen],
            sink=stream_memory_motion,
            alignment_stream=None,
            interval=interval
        )

        values = stream_memory_motion.window(interval).values()
        logging.debug(values)
        assert values == [
            0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

        # Aggregate over the window
        stream_memory_m_kitchen_mean = M.create_stream(StreamId('id_memory_m_kitchen_mean'))
        
        tool_sliding_apply = channels.get_tool(
            name='sliding_apply',
            parameters=dict(
                func=online_average
            )
        )

        tool_sliding_apply.execute(
            sources=[
                stream_memory_sliding_window,
                stream_memory_motion
            ],
            sink=stream_memory_m_kitchen_mean,
            alignment_stream=None,
            interval=interval
        )
        
        assert(stream_memory_m_kitchen_mean.window(interval).values() ==
               [0.0, 0.0, 0.0, 0.25, 0.25, 0.4, 0.25, 0.25, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

    def test_workflow_chain(self):
        interval = TimeInterval(t1, t1 + timedelta(minutes=10))
    
        w = hyperstream.create_workflow(
            workflow_id="nt_test",
            name="test",
            owner="nt",
            description="test")
    
        # Define the sliding window
        n_sliding_window = w.create_node(stream_name="sliding_window", channel=M, plate_ids=None)
        w.create_factor(tool=dict(name="sliding_window", parameters=dict(
            first=MIN_DATE,
            lower=-30.0,
            upper=0.0,
            increment=10.0
        )), sources=None, sink=n_sliding_window).execute(interval)
    
        # Define the environmental data
        n_environmental_data = w.create_node(stream_name="environmental", channel=M, plate_ids=["H1"])
        w.create_factor(tool=dict(name="sphere", parameters=dict(modality="environmental")),
                        sources=None, sink=n_environmental_data).execute(interval)

        # Pick out the kitchen sensor
        n_environmental_kitchen = w.create_node(stream_name="environmental_kitchen", channel=M, plate_ids=["H1"])
        w.create_factor(
            tool=dict(name="component_filter", parameters=dict(key="uid", values={"S1_K"}, complement=False)),
            sources=[n_environmental_data], sink=n_environmental_kitchen).execute(interval)

        # Define the kitchen motion data
        n_motion_data = w.create_node(stream_name="motion_data", channel=M, plate_ids=["H1"])
        w.create_factor(tool=dict(name="component", parameters=dict(key="motion")),
                        sources=[n_environmental_kitchen], sink=n_motion_data).execute(interval)

        # Take the mean of the motion stream over a sliding window
        n_average_motion = w.create_node(stream_name="average_motion", channel=M, plate_ids=["H1"])
        w.create_factor(tool=dict(name="sliding_apply", parameters=dict(func=online_average)),
                        sources=[n_sliding_window, n_motion_data], sink=n_average_motion).execute(interval)

        house_1 = (('house', '1'), )

        # Check the results
        ii = str(n_sliding_window.streams[None].window(interval).first().value)
        assert ii == "(2016-04-28 20:00:00+00:00, 2016-04-28 20:00:30+00:00]"

        env = n_environmental_data.streams[house_1].window(interval).first().value
        assert env == {u'electricity': 0.0, 'uid': u'04063'}
    
        motion = n_motion_data.streams[house_1].window(interval).values()[:10]
        assert motion == [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0]
    
        md = n_average_motion.streams[house_1].window(interval).values()[:10]
        assert md == [0.0, 0.0, 0.0, 0.25, 0.25, 0.4, 0.25, 0.25, 0.0, 0.0]

    def test_workflow_chain2(self):
        interval = TimeInterval(t1, t1 + timedelta(minutes=10))
    
        w = hyperstream.create_workflow(
            workflow_id="nt_test2",
            name="test",
            owner="nt",
            description="test")

        # Define the sliding window
        n_sliding_window = w.create_node(stream_name="sliding_window", channel=M, plate_ids=None)
        w.create_factor(
            tool=dict(name="sliding_window", parameters=dict(first=MIN_DATE, lower=-30, upper=0, increment=10)),
            sources=None, sink=n_sliding_window)
    
        # Define the environmental data
        n_environmental_data = w.create_node(stream_name="environmental", channel=M, plate_ids=["H1"])
        w.create_factor(
            tool=dict(name="sphere", parameters=dict(modality="environmental")),
            sources=None, sink=n_environmental_data)
    
        # Pick out the kitchen sensor
        n_environmental_kitchen = w.create_node(stream_name="environmental_kitchen", channel=M, plate_ids=["H1"])
        w.create_factor(
            tool=dict(name="component_filter", parameters=dict(key="uid", values={"S1_K"}, complement=False)),
            sources=[n_environmental_data], sink=n_environmental_kitchen)

        # Define the kitchen motion data
        n_motion_data = w.create_node(stream_name="motion_data", channel=M, plate_ids=["H1"])
        w.create_factor(
            tool=dict(name="component", parameters=dict(key="motion")),
            sources=[n_environmental_kitchen], sink=n_motion_data)
    
        # Take the mean of the motion stream over a sliding window
        n_average_motion = w.create_node(stream_name="average_motion", channel=M, plate_ids=["H1"])
        w.create_factor(
            tool=dict(name="sliding_apply", parameters=dict(func=online_average)),
            sources=[n_sliding_window, n_motion_data], sink=n_average_motion)
        
        w.execute(interval)

        for stream_name, stream in n_average_motion.streams.iteritems():
            print stream_name, stream
            for kv in stream.window(interval):
                print "", kv
            print
        
        md = list(n_average_motion.streams[(('house', '1'),)].window(interval).values())[:10]

        # print(md)
        assert md == [0.0, 0.0, 0.0, 0.25, 0.25, 0.4, 0.25, 0.25, 0.0, 0.0]

    def test_overlapping_plates(self):
        # TODO: Create test that involves multiple overlapping plates
        assert False


if __name__ == '__main__':
    unittest.main()
