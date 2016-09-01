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

from hyperstream import ChannelManager, HyperStreamConfig, StreamId, Workflow, PlateManager, WorkflowManager, Client
from hyperstream.time_interval import RelativeTimeInterval
from hyperstream.modifiers import Component
from hyperstream.utils import UTC
from hyperstream.itertools2 import online_average, count as online_count
from sphere_connector_package.sphere_connector import SphereLogger

if __name__ == '__main__':
    # TODO: hyperstream needs it's own logger (can be a clone of this one)
    sphere_logger = SphereLogger(path='/tmp', filename='sphere_connector', loglevel=logging.DEBUG)
    
    hyperstream_config = HyperStreamConfig()
    client = Client(hyperstream_config.mongo)
    
    # Define some managers
    channels = ChannelManager(hyperstream_config.tool_path)
    plates = PlateManager(hyperstream_config.meta_data).plates
    workflows = WorkflowManager(channels=channels, plates=plates)
    
    # Various constants
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
    t2 = t1 + timedelta(seconds=10)
    interval = (t1, t2)
    
    # Various channels
    M = channels.memory
    S = channels.sphere
    T = channels.tools
    D = channels.mongo
    
    # Basic definition of a workflow
    w = Workflow(
        channels=channels,
        plates=plates,
        workflow_id="nt_multimodal_test",
        name="Test of multimodal performance",
        owner="nt",
        description="Another test of creating workflows")
    
    w.create_node(node_id="env", stream_name="environmental", plate_ids=["H1"])
    
    for stream in w.nodes['env'].window(interval).streams:
        for kv in stream.iteritems():
            print kv
