"""
The MIT License (MIT)
Copyright (c) 2014-2016 University of Bristol

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

import logging
from datetime import datetime, timedelta

from hyperstream import OnlineEngine, HyperStreamConfig, StreamId
from hyperstream.modifiers import List, Average, Count, Component, First, IData, Data, Time, Head
from hyperstream.utils import UTC
from sphere_connector_package.sphere_connector import SphereLogger


if __name__ == '__main__':
    # TODO: hyperstream needs it's own logger (can be a clone of this one)
    sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.DEBUG)

    hyperstream_config = HyperStreamConfig()
    
    online_engine = OnlineEngine(hyperstream_config)
    
    # Various constants
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
    t2 = t1 + timedelta(minutes=5)
    second = timedelta(seconds=1)
    minute = timedelta(minutes=1)
    hour = timedelta(hours=1)

    # Various channels
    M = online_engine.channels.memory
    S = online_engine.channels.sphere
    T = online_engine.channels.tools
    
    # TODO: We could make the __getitem__ accept str and do the following, but for now it only accepts StringId
    environmental = StreamId(name='environmental', meta_data={'house': "1"})
    clock = StreamId('clock')
    aggregate = StreamId('aggregate')
    every30s = StreamId('every30s')
    motion_kitchen_windowed = StreamId('motion_kitchen_windowed')
    m_kitchen_30_s_window = StreamId('m_kitchen_30_s_window')
    average = StreamId('aver')
    count = StreamId('count')
    sum_ = StreamId('sum')
    
    # Simple querying
    el = S[environmental].window((t1, t1 + minute)).modify(Component('electricity-04063') + List()).items()
    edl = S[environmental].window((t1, t1 + minute)).modify(Component('electricity-04063') + Data() + List()).items()

    q1 = "\n".join("=".join(map(str, ee)) for ee in el)

    print(q1)
    print(edl)

    # Windowed querying
    # M[every30s] = T[clock].define(stride=30 * second).modify(First() + IData())
    # M[every30s] = T[clock].define(stride=30 * second).modify(Head(1) + Data())
    # M[every30s] = T[clock].define(stride=30 * second).modify(Time())
    # M[every30s] = T[clock].define(stride=30 * second)
    M.create_stream(stream_id=every30s, tool=T[clock].define(stride=30 * second))

    # m_kitchen_30_s_window = \
    #     S[environmental].define(modality='environmental').window(-30, 0).modify(Component('motion-S1_K'))
    # m_kitchen_30_s_window = S[environmental].define(modality='environmental').modify(Component('motion-S1_K'))
    S.create_stream(stream_id=m_kitchen_30_s_window).define(modality='environmental').modify(Component('motion-S1_K'))

    # M[average] = T[aggregate].define(
    #     input_streams=[m_kitchen_30_s_window],
    #     timer=M[every30s],
    #     func=Data() + Average()
    # )
    #
    # M[count] = T[aggregate].define(
    #     input_streams=[m_kitchen_30_s_window],
    #     timer=M[every30s],
    #     func=Data() + Count()
    # )

    averager = T[aggregate].define(
        input_streams=[S[m_kitchen_30_s_window]],
        timer=M[every30s],
        func=Data() + Average()
    )

    counter = T[aggregate].define(
        input_streams=[S[m_kitchen_30_s_window]],
        timer=M[every30s],
        func=Data() + Count()
    )

    M.create_stream(stream_id=average, tool=averager)
    M.create_stream(stream_id=count, tool=counter)

    stream_ref = M[average].window((t1, t2)).modify(Data() + List())
    aa = stream_ref.items()
    print(aa)
    assert (aa == [0.0, 0.25, 0.25, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])

    cc = M[count].window((t1, t1 + 5 * minute)).modify(Data() + List()).items()
    print(cc)
    assert (cc == [3, 4, 4, 3, 3, 3, 3, 3, 3, 3])
