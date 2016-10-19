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

import logging
from datetime import datetime, timedelta

from hyperstream import HyperStream, StreamId
from hyperstream.utils import UTC
from hyperstream.itertools2 import online_average
from sphere_helpers import PredefinedTools

from hyperstream import TimeInterval, RelativeTimeInterval

from hyperstream.utils.time_utils import MIN_DATE

if __name__ == '__main__':
    hyperstream = HyperStream()
    tools = PredefinedTools(hyperstream.channel_manager)

    # Various channels
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    T = hyperstream.channel_manager.tools
    D = hyperstream.channel_manager.mongo
    
    # A couple of parameters
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
    interval = TimeInterval(t1, t1 + timedelta(minutes=60))
    relative_interval = RelativeTimeInterval(-30, 0)
    
    minute = timedelta(minutes=1)
    second = timedelta(seconds=1)
    
    w = hyperstream.create_workflow(
        workflow_id="nt_test",
        name="test",
        owner="nt",
        description="test")
    
    # Create clock stream
    n_clock = w.create_node(
        stream_name='every_10_seconds',
        channel=M,
        plate_ids=None)
    f_clock = w.create_factor(tool=tools.clock_10s, sources=None, sink=n_clock)
    
    # Create the env data stream
    n_environmental_data = w.create_node(
        stream_name='environmental',
        channel=M,
        plate_ids=['H1'])
    f_environmental_data = w.create_factor(
        tool={
            "name": 'sphere',
            "parameters": {
                'modality': 'environmental',
                'filters': {
                    'uid': {
                        '$regex': r'S\d+_K'
                    }
                }
            }},
        sources=None,
        sink=n_environmental_data)
    
    # Apply a relative window to this stream
    n_relative_env_data = w.create_node(
        stream_name='relative_environmental_data',
        channel=M,
        plate_ids=['H1'])
    f_relative_env_data = w.create_factor(
        tool={
            "name": 'relative_window',
            "parameters": {
                'relative_start': -30,
                'relative_end': 0,
                'values_only': False
            }},
        sources=[
            n_environmental_data
        ],
        sink=n_relative_env_data,
        alignment_node=n_clock)
    
    # Do aggregation over the the reative windows data
    n_relative_mean = w.create_node(
        stream_name='relative_mean_env_data',
        channel=M,
        plate_ids=['H1'])
    f_relative_mean = w.create_factor(
        tool=tools.relative_apply_mean, 
        sources=[n_relative_env_data], 
        sink=n_relative_mean, 
        alignment_node=n_clock)
    
    w.execute(interval)
    
    rows = []
    for kk, node in n_relative_mean.streams.iteritems():
        for time, values in node.window(interval).iteritems():
            rows.append(values)
            rows[-1]['datetime'] = time
    
    import pandas as pd
    pd.options.display.width = 200
    df = pd.DataFrame(rows)
    df.set_index('datetime', inplace=True)
    
    print df
    
    exit()
    
    sid_every_second = StreamId('every30s')
    environmental = StreamId('environmental_niall', meta_data={'house': '1'})
    
    ti = TimeInterval(t1, t1 + timedelta(minutes=1))
    rel = RelativeTimeInterval(-30 * second, 0)
    
    # Create the synch tool
    M.create_stream(stream_id=sid_every_second)
    t_clock = hyperstream.channel_manager.get_tool(
        'clock', dict(first=MIN_DATE, stride=1 * second))
    
    # Get data from database
    # D.create_stream(environmental)
    t_environmental = hyperstream.channel_manager.get_tool(
        'sphere', dict(modality='environmental', filters={'uid': {'$regex': r'S\d+_K'}}))
    
    # Apply relative window
    window_id = StreamId('sid_window_environmental_niall')
    # D.create_stream(window_id )
    windowed_environmental = D[window_id]
    t_relative_environmental_data = hyperstream.channel_manager.get_tool(
        'relative_window', dict(relative_start=-30, relative_end=0, values_only=True))
    
    # #
    # m_averaged = StreamId('averaged_motion')
    # M.create_stream(m_averaged)
    # t_average = channels.get_tool('aggregate', dict(func=online_average))
    
    # Execute the tools
    t_clock.execute(
        alignment_stream=None,
        sources=None,
        sink=M[sid_every_second],
        interval=ti)
    
    t_environmental.execute(
        alignment_stream=None,
        sources=None,
        sink=D[environmental],
        interval=ti)
    
    t_relative_environmental_data.execute(
        alignment_stream=M[sid_every_second],
        sources=[
            D[environmental]
        ],
        sink=windowed_environmental,
        interval=ti
    )
    
    for time, values in windowed_environmental.window(ti):
        print time
        for value in values:
            print '\t', value
        break
    
    exit()
    
    # Define the workflow
    w = hyperstream.create_workflow(
        workflow_id="nt_test",
        name="test",
        owner="nt",
        description="test")
    
    # Define the sliding window
    n_sliding_window = w.create_node(stream_name="sliding_window", channel=M, plate_ids=None)
    f_sliding_window = w.create_factor(tool={"name": "sliding_window", "parameters": dict(
        first=MIN_DATE,
        lower=timedelta(seconds=-30),
        upper=timedelta(seconds=0),
        increment=timedelta(seconds=10)
    )}, sources=None, sink=n_sliding_window)
    
    # Define the environmental data
    n_environmental_data = w.create_node(stream_name="environmental", channel=M, plate_ids=["H1"])
    f_environmental_data = w.create_factor(tool=tools.environmental, sources=None, sink=n_environmental_data)
    
    # Define the motion data
    n_motion_data = w.create_node(stream_name="motion_data", channel=M, plate_ids=["H1"])
    f_motion_data = w.create_factor(tool={"name": "component", "parameters": dict(key="motion-S1_K")},
                                    sources=[n_environmental_data], sink=n_motion_data)
    
    # Take the mean of the motion stream over a sliding window
    n_average_motion = w.create_node(stream_name="average_motion", channel=M, plate_ids=["H1"])
    f_average_motion = w.create_factor(tool={"name": "sliding_apply", "parameters": dict(func=online_average)},
                                       sources=[n_sliding_window, n_motion_data], sink=n_average_motion)
    
    # Execute the factors
    w.execute(interval)
    
    for stream_name, stream in n_average_motion.streams.iteritems():
        print stream_name, stream
        for kv in stream.window(interval):
            print "", kv
        print
