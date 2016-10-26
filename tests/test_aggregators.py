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
"""
Unit tests for aggregating
"""
from __future__ import print_function
import sys
import unittest
import logging

from hyperstream import TimeInterval, TimeIntervals, RelativeTimeInterval
from hyperstream.itertools2 import online_average
from hyperstream.utils import MIN_DATE, utcnow
from helpers import *


def basic_workflow(workflow_id):
    w = hyperstream.create_workflow(workflow_id=workflow_id, name="Test", owner="Tests", description="")
    N = w.nodes

    nodes = (
        ("rss_raw", M, ["H1"]),  # Raw RSS data
        ("rss_aid", M, ["H1.L"]),  # RSS by access point id
        ("rss_aid_uid", M, ["H1.L.W"]),  # RSS by access point id and device id
        ("rss", M, ["H1.L.W"]),  # RSS values only (by access point id and device id)
        ("rss_dev_avg", M, ["H1.L"]),  # Averaged RSS values by device, per location
        ("rss_loc_avg", M, ["H1.W"]),  # Averaged RSS values by location, per device
    )

    # Create all of the nodes
    for stream_name, channel, plate_ids in nodes:
        w.create_node(stream_name, channel, plate_ids)

    w.create_factor(
        tool=tools.wearable_rss,
        sources=None,
        sink=N["rss_raw"])

    w.create_multi_output_factor(
        tool=tools.split_aid,
        source=N["rss_raw"],
        sink=N["rss_aid"])

    w.create_multi_output_factor(
        tool=tools.split_uid,
        source=N["rss_aid"],
        sink=N["rss_aid_uid"])

    w.create_factor(
        tool=tools.wearable_rss_values,
        sources=[N["rss_aid_uid"]],
        sink=N["rss"])

    return w


def print_head(w, node_id, parent_plate_values, plate_values, interval, n=10, print_func=print):
    print_func("Node: {}".format(node_id))
    w.nodes[node_id].print_head(parent_plate_values, plate_values, interval, n, print_func)


# Some plate values for display purposes
h1 = (('house', '1'),)
wA = (('wearable', 'A'),)
locs = tuple(("location", loc) for loc in ["kitchen", "hallway", "lounge"])


# noinspection PyMethodMayBeStatic
class HyperStreamAggregatorTests(unittest.TestCase):
    def test_basic_aggregator(self):
        w = basic_workflow(sys._getframe().f_code.co_name)

        aggregator = channels.get_tool(
            name="aggregate",
            parameters=dict(func=online_average, aggregation_plate="H1.L.W")
        )

        N = w.nodes
        w.create_factor(
            tool=aggregator,
            sources=[N["rss"]],
            sink=N["rss_dev_avg"]
        )

        time_interval = scripted_experiments[0]
        w.execute(time_interval)

        # print_head(w, "rss_raw", None, h1, time_interval, 10, print)
        # print_head(w, "rss_aid", h1, locs, time_interval, 10, print)
        # print_head(w, "rss_aid_uid", h1 + wA, locs, time_interval, 10, print)
        print_head(w, "rss", h1 + wA, locs, time_interval, 10, print)
        print_head(w, "rss_dev_avg", h1 + wA, locs, time_interval, 10, print)

        assert False

    def test_subarray(self):
        assert False
    
    def test_index_of(self):
        assert False


if __name__ == '__main__':
    unittest.main()
