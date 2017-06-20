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

from helpers import *
from hyperstream import HyperStream, NodeIDAbsentError


class TestMetaData(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestMetaData, self).__init__(*args, **kwargs)
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        try:
            delete_meta_data(hs)
            delete_plates(hs)
        except NodeIDAbsentError:
            pass

    def test_meta_data(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        insert_meta_data(hs)
        self.assertListEqual(get_meta_data(hs), ['root', 'test_0', 'test_1', 'test_2', 'test_3'])
        delete_meta_data(hs)
        self.assertListEqual(get_meta_data(hs), ['root'])

    def test_plate_creation(self):
        hs = HyperStream(file_logger=False, console_logger=False, mqtt_logger=None)
        insert_meta_data(hs)
        create_plates(hs)
        expected = [(('test', '0'),), (('test', '1'),), (('test', '2'),), (('test', '3'),)]
        self.assertListEqual(hs.plate_manager.plates["T"].values, expected)
        delete_plates(hs)
        delete_meta_data(hs)


if __name__ == '__main__':
    unittest.main()
