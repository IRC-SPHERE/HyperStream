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

from hyperstream import HyperStream, TimeInterval, NodeIDAbsentError
from helpers import *


def basic_workflow(hs, workflow_id):
    w = hs.create_workflow(
        workflow_id=workflow_id, name="Test", owner="Tests",
        description="Nose test generated workflow"
    )

    ticker = w.create_node("ticker", hs.channel_manager.memory, None)
    w.create_factor(hs.tools.clock(), sources=[], sink=ticker)
    return w


def workflow_with_plates(hs, workflow_id):
    w = hs.create_workflow(
        workflow_id=workflow_id, name="Test with plates", owner="Tests",
        description="Nose test generated workflow with plates"
    )

    ticker_repeated = w.create_node(stream_name="ticker_repeated", channel=hs.channel_manager.memory, plate_ids=["T"])

    for plate_value in hs.plate_manager.plates["T"]:
        ticker_repeated[plate_value] = hs.factors.clock(w, sources=[])

    return w


def workflow_with_nested_plates(hs, workflow_id):
    w = hs.create_workflow(
        workflow_id=workflow_id, name="Test with plates", owner="Tests",
        description="Nose test generated workflow with plates"
    )

    ticker_repeated = w.create_node(stream_name="ticker_repeated", channel=hs.channel_manager.memory, plate_ids=["T"])

    for plate_value in hs.plate_manager.plates["T"]:
        ticker_repeated[plate_value] = hs.factors.clock(w, sources=[])

    return w


def print_head(w, node_id, parent_plate_values, plate_values, interval, n=10, print_func=print):
    print_func("Node: {}".format(node_id))
    w.nodes[node_id].print_head(parent_plate_values, plate_values, interval, n, print_func)


# noinspection PyMethodMayBeStatic
class HyperStreamWorkflowTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(HyperStreamWorkflowTests, self).__init__(*args, **kwargs)
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        try:
            delete_meta_data(hs)
            delete_plates(hs)
        except NodeIDAbsentError:
            pass

    def test_save_workflow(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        workflow_id = sys._getframe().f_code.co_name

        # hyperstream.logger.setLevel(logging.WARN)

        # First delete the workflow if it's there
        hs.workflow_manager.delete_workflow(workflow_id)

        w1 = basic_workflow(hs, workflow_id)

        time_interval = TimeInterval(t1, t2)
        w1.execute(time_interval)

        hs.workflow_manager.commit_workflow(workflow_id)

        # Now remove it from the workflow manager
        del hs.workflow_manager.workflows[workflow_id]

        # And then reload it
        w2 = hs.workflow_manager.load_workflow(workflow_id)

        # print_head(w, "rss", h1 + wA, locs, time_interval, 10, print)
        # print_head(w, "rss_dev_avg", h1, locs, time_interval, 10, print)

    def test_new_api(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        workflow_id = sys._getframe().f_code.co_name

        insert_meta_data(hs)
        create_plates(hs)

        w = workflow_with_plates(hs, workflow_id)
        time_interval = TimeInterval(t1, t2)
        w.execute(time_interval)

        delete_plates(hs)
        delete_meta_data(hs)

    def test_new_api2(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        workflow_id = sys._getframe().f_code.co_name

        insert_meta_data(hs)
        create_plates(hs)

        w = workflow_with_nested_plates(hs, workflow_id)
        time_interval = TimeInterval(t1, t2)
        w.execute(time_interval)

        delete_plates(hs)
        delete_meta_data(hs)

if __name__ == '__main__':
    unittest.main()


