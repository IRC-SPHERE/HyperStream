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

from hyperstream import HyperStream
from hyperstream.workflow.workflow import Workflow


class HyperStreamTests(unittest.TestCase):
    def test___init__(self):
        hyperstream = HyperStream(loglevel=logging.INFO, file_logger=False,
                                  console_logger=False)
        self.assertIs(type(hyperstream), HyperStream)

    def test___repr__(self):
        hyperstream = HyperStream(loglevel=logging.INFO, file_logger=False,
                                  console_logger=False)
        self.assertIs(type(hyperstream.__repr__()), str)

    def test___str__(self):
        hyperstream = HyperStream(loglevel=logging.INFO, file_logger=False,
                                  console_logger=False)
        self.assertIs(type(hyperstream.__str__()), str)

    def test_create_workflow(self):
        hyperstream = HyperStream(loglevel=logging.INFO, file_logger=False,
                                  console_logger=False)
        workflow_id = 1
        name = 'test_workflow'
        owner = 'unittest'
        description = 'test of workflow'
        w = hyperstream.create_workflow(workflow_id, name, owner, description)
        self.assertIs(type(w), Workflow)
        self.assertEqual(w.workflow_id, workflow_id)
        self.assertEqual(w.name, name)
        self.assertEqual(w.owner, owner)
        self.assertEqual(w.description, description)


if __name__ == "__main__":
    unittest.main()
