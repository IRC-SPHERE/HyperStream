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

from hyperstream import HyperStream, StreamId, TimeInterval
from hyperstream.utils import UTC

from sphere_helpers import PredefinedTools


motion_sensors = {
    "bedroom 1": "motion-S1_B1",
    "study": "motion-S1_B0",
    "bedroom 2": "motion-S1_B2",
    "bathroom": "motion-S1_BR",
    "hallway": "motion-S1_H",
    "kitchen": "motion-S1_K",
    "lounge": "motion-S1_L",
    "stairs": "motion-S1_S",
    "toilet": "motion-S1_WC"
}


if __name__ == '__main__':
    hyperstream = HyperStream()
    tools = PredefinedTools(hyperstream)

    # Various constants
    t1 = datetime(2016, 4, 28, 20, 0, 0, 0, UTC)
    # TODO: Shortened time interval for now
    # t2 = datetime(2016, 4, 29, 12, 0, 0, 0, UTC)
    second = timedelta(seconds=1)
    minute = timedelta(minutes=1)
    hour = timedelta(hours=1)
    t2 = t1 + 1 * minute

    # Various channels
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    T = hyperstream.channel_manager.tools
    D = hyperstream.channel_manager.mongo

    # TODO: We could make the __getitem__ accept str and do the following, but for now it only accepts StringId
    environmental = StreamId(name='environmental', meta_data={'house': '1'})
    clock = StreamId('clock')
    aggregate = StreamId('aggregate')
    every30s = StreamId('every30s')
    motion_kitchen_windowed = StreamId('motion_kitchen_windowed')
    env_kitchen_30_s_window = StreamId('env_kitchen_30_s_window')
    m_kitchen_30_s_window = StreamId('m_kitchen_30_s_window')
    average = StreamId('average')  # , meta_data={'house': '1'})
    count = StreamId('count')
    # sum_ = StreamId('sum')
    sphere = StreamId('sphere')
    component = StreamId('component')

    # Create a simple one step workflow for querying
    w = hyperstream.create_workflow(
        workflow_id="localisation",
        name="Test of localisation",
        owner="TD",
        description="Would like to test localisation using PIR and RSSI, so we need to first get the appropriate data"
                    "out of the SPHERE stream, with the appropriate meta-data attached, and then use a model")

    # 5 minutes of data to start with
    time_interval = TimeInterval(t1, t2)

    # Location plate
    # Here we used the splitter tool over the RSS data to generate the plate
    n_rss_flat = w.create_node(stream_name="rss", channel=S, plate_ids=["H1"])
    w.create_factor(tool=tools.wearable_rss, sources=None, sink=n_rss_flat).execute(time_interval)

    n_rss_flat.print_head(None, w.plates["H1"].values[0], time_interval)

    n_rss_aid = w.create_node(stream_name="rss_aid", channel=M, plate_ids=["H1.L"])
    w.create_multi_output_factor(
        tool=tools.split_aid, source=n_rss_flat, splitting_node=None, sink=n_rss_aid).execute(time_interval)

    # w.execute(time_interval)
    n_rss_aid.print_head(None, w.plates["H1.L"], time_interval)

    n_rss_aid_uid = w.create_node(stream_name="rss_aid_uid", channel=M, plate_ids=["H1.L.W"])
    w.create_multi_output_factor(
        tool=tools.split_uid, source=n_rss_aid, splitting_node=None, sink=n_rss_aid_uid).execute(time_interval)

    n_rss = w.create_node(stream_name="rss", channel=M, plate_ids=["H1.L.W"])
    w.create_factor(tool=tools.wearable_rss_values, sources=[n_rss_aid_uid], sink=n_rss).execute(time_interval)

    n_rss.print_head(None, w.plates["H1.L.W"], time_interval)

    exit(0)

    # PIR sensor plate

    # Stream to get motion sensor data
    n_pir = w.create_node(stream_name="environmental_db", channel=D, plate_ids=["H1"])
    f_pir = w.create_factor(tool=tools.environmental_motion, sources=None, sink=n_pir)

    # Stream to get RSS data
    n_rss_aid = w.create_node(stream_name="wearable", channel=S, plate_ids=["H1"])
    f_rss = w.create_factor(tool=tools.wearable_rss, sources=None, sink=n_rss_aid)

    # Execute the workflow
    w.execute(time_interval)

    print(n_pir.streams[('house', '1'), ].window(time_interval).values()[0:5])
    print(n_rss_aid.streams[('house', '1'),].window(time_interval).values()[0:5])

