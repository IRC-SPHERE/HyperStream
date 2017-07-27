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

from .helpers import *


class TestMetaData(unittest.TestCase):
    def test_meta_data(self):
        with HyperStream(file_logger=False, console_logger=False, mqtt_logger=None) as hs:
            tag = 'test_meta_data'
            values = list(map(str, range(4)))
            insert_meta_data(hs, tag, values)
            self.assertListEqual(get_meta_data(hs, tag), ["{}_{}".format(tag, i) for i in values])
            delete_meta_data(hs, tag, values)
            self.assertListEqual(get_meta_data(hs, tag), [])

    def test_plate_creation(self):
        with HyperStream(file_logger=False, console_logger=False, mqtt_logger=None) as hs:
            tag = 'test_plate_creation'
            values = list(map(str, range(4)))
            insert_meta_data(hs, tag, values)
            create_plate(hs, "T1", tag)
            expected = [((tag, i),) for i in values]
            self.assertListEqual(sorted(hs.plate_manager.plates["T1"].values), expected)
            delete_plate(hs, "T1")  # note this now deletes meta data as well


if __name__ == '__main__':
    unittest.main()
