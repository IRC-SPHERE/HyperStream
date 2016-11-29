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

import unittest

import os
from datetime import datetime
import pytz

from sphere_connector_package.sphere_connector import SphereConnector, DataWindow


# noinspection PyMethodMayBeStatic
class SphereConnectorTests(unittest.TestCase):
    def test_filtering(self):
        sphere_connector = SphereConnector(
            config_filename='config_strauss.json',
            include_mongo=True,
            include_redcap=False,
            sphere_logger=None)
        
        t1 = datetime(2016, 4, 28, 20, 0, 0, 0, pytz.UTC)
        t2 = datetime(2016, 4, 28, 20, 10, 0, 0, pytz.UTC)
        
        window = DataWindow(sphere_connector, t1, t2)
        docs = window.modalities['environmental'].get_data(
            elements=None,
            # filters={'e.n', {'$regex': r'.*?S1_K$'}}
            # filters={'uid': {'$in': ['04063']}}
            # filters={'uid': {'$regex': '/04063/'}}
        )
        
        
if __name__ == '__main__':
    unittest.main()
