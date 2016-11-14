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
Online Engine module. This will be used in the online execution mode.
"""
import logging
from time import sleep
import signal
from datetime import datetime, timedelta

from .time_interval import TimeInterval
from .utils import UTC


class OnlineEngine(object):
    """
    OnlineEngine class.
    """
    def __init__(self, hyperstream):
        """
        Initialise the engine.

        :param hyperstream: The hyperstream object
        """
        self.hyperstream = hyperstream

    def execute(self):
        """
        Execute the engine - currently simple executes all workflows.
        """
        # Set some default times for execution (debugging)
        start_time = datetime(year=2016, month=10, day=19, hour=12, minute=28, tzinfo=UTC)
        duration = timedelta(seconds=5)
        end_time = start_time + duration
        time_interval = TimeInterval(start_time, end_time)
        workflow_id = "lda_localisation_model_predict"

        for _ in range(100):
            signal.alarm(305)  # if this takes more than 5 minutes, kill myself
            logging.info("Online engine starting up.")

            self.hyperstream.workflow_manager.set_requested_intervals(workflow_id, [time_interval])

            self.hyperstream.workflow_manager.execute_all()
            sleep(5)

            time_interval += duration

