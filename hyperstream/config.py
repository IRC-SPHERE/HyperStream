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
HyperStream configuration module.
"""

import logging
import json
import os

from utils import Printable
from plugin_manager import Plugin
from time_interval import RelativeTimeInterval


class OnlineEngineConfig(Printable):
    def __init__(self, interval, sleep=5, iterations=100, alarm=None):
        self.interval = RelativeTimeInterval(**interval)
        self.sleep = sleep
        self.iterations = iterations
        self.alarm = sleep * iterations if not alarm else alarm


class HyperStreamConfig(Printable):
    """
    Wrapper around the hyperstream configuration files (hyperstream_config.json and meta_data.json)
    """
    def __init__(self):
        """
        Initialise the configuration - currently uses fixed file names (hyperstream_config.json and meta_data.json)
        """
        self.mongo = None

        try:
            with open('hyperstream_config.json', 'r') as f:
                logging.getLogger("hyperstream")
                logging.info('Reading ' + os.path.abspath(f.name))
                config = json.load(f)
                self.mongo = config['mongo']
                self.output_path = config['output_path'] if 'output_path' in config else 'output'
                self.plugins = [Plugin(**p) for p in config['plugins']]
                self.online_engine = OnlineEngineConfig(**config["online_engine"])
        except (OSError, IOError, TypeError) as e:
            # raise
            logging.error("Configuration error: " + str(e))
