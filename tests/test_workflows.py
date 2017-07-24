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

from hyperstream import TimeInterval, IncompatiblePlatesError
from .helpers import *


def get_workflow_parameters(sub_type=""):
    return dict(
        name="Test{}".format(sub_type),
        description="Nose test generated workflow{}".format(sub_type),
        owner="Tests"
    )


def identity(x):
    return x


def basic_workflow(hs, workflow_id):
    with hs.create_workflow(workflow_id=workflow_id, **get_workflow_parameters("")) as w:
        ticker = w.create_node("ticker", hs.channel_manager.memory, None)
        w.create_factor(hs.tools.clock(), sources=[], sink=ticker)
        return w


def print_head(w, node_id, parent_plate_values, plate_values, interval, n=10, print_func=print):
    print_func("Node: {}".format(node_id))
    w.nodes[node_id].print_head(parent_plate_values, plate_values, interval, n, print_func)


# noinspection PyMethodMayBeStatic
class HyperStreamWorkflowTests(unittest.TestCase):
    # @with_setup(setup, teardown)
    def test_save_workflow(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        workflow_id = sys._getframe().f_code.co_name

        # First delete the workflow if it's there
        hs.workflow_manager.delete_workflow(workflow_id)

        w = basic_workflow(hs, workflow_id)

        time_interval = TimeInterval(t1, t2)
        w.execute(time_interval)

        hs.workflow_manager.commit_workflow(workflow_id)

        # Now remove it from the workflow manager
        del hs.workflow_manager.workflows[workflow_id]

        # And then reload it
        hs.workflow_manager.load_workflow(workflow_id)

    def test_new_api(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        workflow_id = sys._getframe().f_code.co_name

        self.assertRaises(ValueError, hs.factors.stream_broadcaster, identity)

        with hs.create_workflow(workflow_id=workflow_id, **get_workflow_parameters(" with plates")) as w:
            ticker_repeated = w.create_node(stream_name="ticker_repeated", channel=hs.channel_manager.memory,
                                            plate_ids=["T"])

            for plate_value in hs.plate_manager.plates["T"]:
                ticker_repeated[plate_value] = hs.factors.clock(sources=[])

            time_interval = TimeInterval(t1, t1 + minute)
            w.execute(time_interval)

    def test_new_api_nested_plates(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        workflow_id = sys._getframe().f_code.co_name

        with hs.create_workflow(workflow_id=workflow_id, **get_workflow_parameters(" with nested plates")) as w:
            ticker_repeated = w.create_node(
                stream_name="ticker_repeated", channel=hs.channel_manager.memory, plate_ids=["T"])
            nested_repeated = w.create_node(
                stream_name="nested_repeated", channel=hs.channel_manager.memory, plate_ids=["T.U"])

            for outer in hs.plate_manager.plates["T"]:
                ticker_repeated[outer] = hs.factors.clock(sources=[])
                for inner in hs.plate_manager.plates["T.U"][outer]:
                    # Some cases that should fail
                    cloner = hs.factors.stream_broadcaster(ticker_repeated[outer], func=identity)
                    self.assertRaises(IncompatiblePlatesError, ticker_repeated.__setitem__, inner, cloner)
                    self.assertRaises(IncompatiblePlatesError, ticker_repeated.__setitem__, (inner, outer), cloner)

                    nested_repeated[outer][inner] = hs.factors.stream_broadcaster(ticker_repeated[outer], func=identity)

            time_interval = TimeInterval(t1, t1 + minute)
            w.execute(time_interval)

    def test_new_api_overlapping_plates(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        dg = hs.plugins.data_generators
        ex = hs.plugins.example
        M = hs.channel_manager.memory

        workflow_id = sys._getframe().f_code.co_name

        with hs.create_workflow(workflow_id=workflow_id, **get_workflow_parameters(" overlapping plates")) as w:
            ticker = w.create_node("ticker", channel=M, plate_ids=[])

            random_repeated1 = w.create_node(stream_name="rr1", channel=M, plate_ids=["T"])
            random_repeated2 = w.create_node(stream_name="rr2", channel=M, plate_ids=["V"])
            random_product = w.create_node(stream_name="rp", channel=M, plate_ids=["T", "V"])
            random_product_clone = w.create_node(stream_name="rpc", channel=M, plate_ids=["T", "V"])

            ticker[None] = hs.factors.clock(sources=[])

            for t in hs.plate_manager.plates["T"]:
                random_repeated1[t] = dg.factors.random(sources=[], alignment_node=ticker, seed=1)
            for v in hs.plate_manager.plates["V"]:
                random_repeated2[v] = dg.factors.random(sources=[], alignment_node=ticker, seed=2)

            for t in hs.plate_manager.plates["T"]:
                for v in hs.plate_manager.plates["V"]:
                    prod = hs.factors.product([random_repeated1[t], random_repeated2[v]])
                    self.assertRaises(IncompatiblePlatesError, random_product.__setitem__, t, prod)
                    self.assertRaises(IncompatiblePlatesError, random_product.__setitem__, v, prod)

                    random_product[t, v] = hs.factors.product(sources=[random_repeated1[t], random_repeated2[v]])
                    random_product_clone[t, v] = ex.factors.dummy(sources=[random_product[t, v]])

            time_interval = TimeInterval(t1, t1 + minute)
            w.execute(time_interval)
            pv = (('test', '0'), ('test_overlap', '0'))

            h1 = random_repeated1.streams[(pv[0],)].window(time_interval).values()
            h2 = random_repeated2.streams[(pv[1],)].window(time_interval).values()
            hp = random_product.streams[pv].window(time_interval).values()
            self.assertListEqual(map(lambda (a, b): a * b, zip(h1, h2)), hp)


if __name__ == '__main__':
    unittest.main()
