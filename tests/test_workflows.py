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
import simplejson as json

from hyperstream import TimeInterval, IncompatiblePlatesError
from .helpers import *

W_DICT = {
    'plates': {
        'root': [
            {'type': 'factor', 'id': 'Clock(first=datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=UTC, stride=1.0)'}
        ],
        u'T': [
            {'type': 'node', 'id': 'rp'},
            {'type': 'node', 'id': 'rr1'},
            {'type': 'factor', 'id': 'Random(seed=1)'},
            {'type': 'factor', 'id': 'Product()'}
        ],
        u'V': [
            {'type': 'node', 'id': 'rp'},
            {'type': 'node', 'id': 'rr2'},
            {'type': 'factor', 'id': 'Random(seed=2)'},
            {'type': 'factor', 'id': 'Product()'}
        ]
    },
    'nodes': [{'id': 'rp'}, {'id': 'ticker'}, {'id': 'rr2'}, {'id': 'rr1'}],
    'factors': [
        {'sources': [], 'id': 'Clock(first=datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=UTC, stride=1.0)',
         'sink': 'ticker'},
        {'sources': [], 'id': 'Random(seed=1)', 'sink': 'rr1'},
        {'sources': [], 'id': 'Random(seed=2)', 'sink': 'rr2'},
        {'sources': ['rr1', 'rr2'], 'id': 'Product()', 'sink': 'rp'}
    ]
}

W_VIZ = {
    'nodes': [
        {'type': 'rv', 'id': 'rp'},
        {'type': 'rv', 'id': 'ticker'},
        {'type': 'rv', 'id': 'rr2'},
        {'type': 'rv', 'id': 'rr1'},
        {'type': 'fac', 'id': 'Clock(first=datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=UTC, stride=1.0)'},
        {'type': 'fac', 'id': 'Random(seed=1)'},
        {'type': 'fac', 'id': 'Random(seed=2)'},
        {'type': 'fac', 'id': 'Product()'}
    ],
    'links': [
        {'source': 'Clock(first=datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=UTC, stride=1.0)', 'target': 'ticker'},
        {'source': 'Random(seed=1)', 'target': 'rr1'}, {'source': 'Random(seed=2)', 'target': 'rr2'},
        {'source': 'rr1', 'target': 'Product()'},
        {'source': 'rr2', 'target': 'Product()'},
        {'source': 'Product()', 'target': 'rp'}
    ]
}



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

        T = hs.plate_manager.plates['T']

        with hs.create_workflow(workflow_id=workflow_id, **get_workflow_parameters(" with plates")) as w:
            ticker_repeated = w.create_node(stream_name="ticker_repeated", channel=hs.channel_manager.memory,
                                            plates=[T])

            for t in T:
                ticker_repeated[t] = hs.factors.clock(sources=[])

            time_interval = TimeInterval(t1, t1 + minute)
            w.execute(time_interval)

    def test_new_api_nested_plates(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        workflow_id = sys._getframe().f_code.co_name

        T = hs.plate_manager.plates['T']
        U = hs.plate_manager.plates['T.U']

        with hs.create_workflow(workflow_id=workflow_id, **get_workflow_parameters(" with nested plates")) as w:
            ticker_repeated = w.create_node(
                stream_name="ticker_repeated", channel=hs.channel_manager.memory, plates=[T])
            nested_repeated = w.create_node(
                stream_name="nested_repeated", channel=hs.channel_manager.memory, plates=[U])

            for t in T:
                ticker_repeated[t] = hs.factors.clock(sources=[])
                for u in U[t]:
                    # Some cases that should fail
                    cloner = hs.factors.stream_broadcaster(ticker_repeated[t], func=identity)
                    self.assertRaises(IncompatiblePlatesError, ticker_repeated.__setitem__, u, cloner)
                    self.assertRaises(IncompatiblePlatesError, ticker_repeated.__setitem__, (u, t), cloner)

                    nested_repeated[t][u] = hs.factors.stream_broadcaster(ticker_repeated[t], func=identity)

            time_interval = TimeInterval(t1, t1 + minute)
            w.execute(time_interval)

    def test_new_api_overlapping_plates(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        dg = hs.plugins.data_generators
        ex = hs.plugins.example
        M = hs.channel_manager.memory

        T = hs.plate_manager.plates['T']
        V = hs.plate_manager.plates['V']

        workflow_id = sys._getframe().f_code.co_name

        with hs.create_workflow(workflow_id=workflow_id, **get_workflow_parameters(" overlapping plates")) as w:
            ticker = w.create_node("ticker", channel=M, plates=[])

            random_repeated1 = w.create_node(stream_name="rr1", channel=M, plates=[T])
            random_repeated2 = w.create_node(stream_name="rr2", channel=M, plates=[V])
            random_product = w.create_node(stream_name="rp", channel=M, plates=[T, V])
            random_product_clone = w.create_node(stream_name="rpc", channel=M, plates=[T, V])

            ticker[None] = hs.factors.clock(sources=[])

            for t in T:
                random_repeated1[t] = dg.factors.random(sources=[], alignment_node=ticker, seed=1)
            for v in V:
                random_repeated2[v] = dg.factors.random(sources=[], alignment_node=ticker, seed=2)

            for t in T:
                for v in V:
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
            self.assertListEqual(list(map(lambda z: z[0] * z[1], zip(h1, h2))), hp)

    def test_json_output(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        dg = hs.plugins.data_generators
        M = hs.channel_manager.memory

        T = hs.plate_manager.plates['T']
        V = hs.plate_manager.plates['V']

        workflow_id = sys._getframe().f_code.co_name

        with hs.create_workflow(workflow_id=workflow_id, **get_workflow_parameters(" json output")) as w:
            ticker = w.create_node("ticker", channel=M, plates=[])

            random_repeated1 = w.create_node(stream_name="rr1", channel=M, plates=[T])
            random_repeated2 = w.create_node(stream_name="rr2", channel=M, plates=[V])
            random_product = w.create_node(stream_name="rp", channel=M, plates=[T, V])

            ticker[None] = hs.factors.clock(sources=[])  # The ticker is outside the plate
            for t in T:
                random_repeated1[t] = dg.factors.random(sources=[], alignment_node=ticker, seed=1)
            for v in V:
                random_repeated2[v] = dg.factors.random(sources=[], alignment_node=ticker, seed=2)
            for (t, v) in zip(T, V):
                random_product[t, v] = hs.factors.product(sources=[random_repeated1[t], random_repeated2[v]])

            d = w.to_dict()
            j = w.to_json()

            print(d)
            print(j)

            assert_dict_equal(d, W_DICT)
            assert_dict_equal(json.loads(j), W_DICT)

            j = w.to_json(w.factorgraph_viz)
            print(j)

            assert_dict_equal(json.loads(j), W_VIZ)

if __name__ == '__main__':
    unittest.main()
