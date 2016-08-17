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
import pytz

from hyperstream.config import HyperStreamConfig
from hyperstream.online_engine import OnlineEngine
from hyperstream.stream import StreamId
from hyperstream import modifiers
from sphere_connector_package.sphere_connector import SphereConnector, SphereLogger


if __name__ == '__main__':
    # TODO: would be nice to be able to refer to this sphere_connector object from the sphere_ tools
    connected = False
    sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.ERROR)
    sphere_connector = SphereConnector(include_mongo=True, include_redcap=False, sphere_logger=sphere_logger)
    
    hyperstream_config = HyperStreamConfig()
    
    online_engine = OnlineEngine(hyperstream_config)

    # Various constants
    utc = pytz.UTC
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, utc)
    t2 = t1 + timedelta(minutes=1)
    second = timedelta(seconds=1)
    minute = timedelta(minutes=1)

    e = StreamId(name='environmental')

    # Various channels
    M = online_engine.channels.memory_channel
    S = online_engine.channels.sphere_channel
    T = online_engine.channels.tool_channel

    # TODO: We could make the __getitem__ accept str and do the following, but for now it only accepts StringId
    clock = StreamId('clock')
    merge = StreamId('merge')
    every30s = StreamId('every30s')
    motion_kitchen_windowed = StreamId('motion_kitchen_windowed')
    m_kitchen_30_s_window = StreamId('m_kitchen_30_s_window')
    aver = StreamId('aver')
    count = StreamId('count')
    sum_ = StreamId('sum')
    
    # Simple query
    from pprint import pprint
    
    pprint(S[e, t1, t1 + minute, modifiers.Component('motion-S1_K') + modifiers.List()]())
    
    # Window'd querying
    M[every30s] = T[clock](stride=30 * second)
    m_kitchen_30_s_window = S[e, -30 * second, timedelta(0), modifiers.Component('motion-S1_K')]

    M[count] = T[merge](
        timer=M[every30s],
        data=m_kitchen_30_s_window,
        func=modifiers.Data() + modifiers.Count()
    )
    
    M[aver] = T[merge](
        timer=M[every30s],
        data=m_kitchen_30_s_window,
        func=modifiers.Data() + modifiers.Average()
    )

    pprint(M[count, t1, t1 + 5 * minute,  modifiers.Data() + modifiers.List()]())
    pprint(M[aver, t1, t1 + 5 * minute,  modifiers.Data() + modifiers.List()]())
    
