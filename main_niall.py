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

from hyperstream.config import HyperStreamConfig
from hyperstream.online_engine import OnlineEngine
from sphere_connector_package.sphere_connector import SphereConnector

if __name__ == '__main__':
    # TODO: would be nice to be able to refer to this sphere_connector object from the sphere_ tools
    connected = False
    sphere_connector = SphereConnector(log_filename='hyperstream_online', include_mongo=True, include_redcap=False)
    
    hyperstream_config = HyperStreamConfig()
    
    online_engine = OnlineEngine(hyperstream_config)
    
    from datetime import datetime, timedelta
    import pytz
    
    from hyperstream import modifiers
    
    # Various constants
    utc = pytz.UTC
    e = 'environmental'
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, utc)
    t2 = datetime(2016, 4, 29, 13, 0, 0, 0, utc)
    second = timedelta(seconds=1)
    minute = timedelta(minutes=1)
    
    # Various channels
    M = online_engine.channels.memory_channel
    S = online_engine.channels.sphere_channel
    T = online_engine.channels.tool_channel
    
    # # Simple querying
    # el = S[e, t1, t1 + minute, modifiers.Component('electricity-04063') + modifiers.List()]()
    # edl = S[e, t1, t1 + minute, modifiers.Component('electricity-04063') + modifiers.Data() + modifiers.List()]()
    #
    # print el
    # print edl
    # print
    
    # Window'd querying
    M['every30s'] = T['clock'](stride=30 * second)
    M['aver'] = T['merge'](
        timer=M['every30s'],
        data=S[e, -30 * second, timedelta(0), modifiers.Component('motion-S1_K')],
        func=modifiers.Data() + modifiers.Average()
    )
    aa = M['aver', t1, t1 + 5 * minute, modifiers.Data() + modifiers.List()]()
    
    M['count'] = T['merge'](
        timer=M['every30s'],
        data=S[e, -30 * second, timedelta(0),
               modifiers.Component('motion-S1_K')],
        func=modifiers.Data() + modifiers.Count()
    )
    cc = M['count', t1, t1 + 5 * minute, modifiers.Data() + modifiers.List()]()
        
    print aa
    print cc
