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
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

import unittest
import logging

from hyperstream import Client
from hyperstream import HyperStreamConfig


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.config = HyperStreamConfig()


    def test___init__(self):
        client = Client(self.config.mongo, auto_connect=False)
        self.assertIs(type(client), Client)

    def test_connect(self):
        client = Client(self.config.mongo, auto_connect=False)
        client.connect(self.config.mongo)
        self.assertIs(type(client), Client)

    def test_get_config_value(self):
        client = Client(self.config.mongo)
        key = ''.join(key for key in client.server_config.keys())
        expected = 'default_value'
        self.assertEqual(client.get_config_value(key, expected), expected)

        key = 'db'
        expected = 'hyperstream'
        self.assertEqual(client.get_config_value(key), expected)

if __name__ == "__main__":
    unittest.main()
