# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

import unittest

from hyperstream import HyperStream

hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)


def insert_meta_data():
    for data in map(str, range(4)):
        identifier = 'test_{}'.format(data)
        hs.plate_manager.meta_data_manager.insert(
            tag='test', identifier=identifier, parent='root', data=data)


def delete_meta_data():
    for data in map(str, range(4)):
        identifier = 'test_{}'.format(data)
        hs.plate_manager.meta_data_manager.delete(identifier)


def get_meta_data():
    return sorted(x.identifier for x in hs.plate_manager.meta_data_manager.global_plate_definitions.all_nodes())


def create_plate():
    hs.plate_manager.create_plate(
        plate_id="T",
        description="test",
        meta_data_id="test",
        values=[],
        complement=True,
        parent_plate=None)


def delete_plate():
    hs.plate_manager.delete_plate("T")


class TestMetaData(unittest.TestCase):
    def test_meta_data(self):
        insert_meta_data()
        self.assertListEqual(get_meta_data(), ['root', 'test_0', 'test_1', 'test_2', 'test_3'])
        delete_meta_data()
        self.assertListEqual(get_meta_data(), ['root'])

    def test_plate_creation(self):
        insert_meta_data()
        create_plate()
        expected = [(('test', '0'),), (('test', '1'),), (('test', '2'),), (('test', '3'),)]
        self.assertListEqual(hs.plate_manager.plates["T"].values, expected)
        delete_plate()
        delete_meta_data()
