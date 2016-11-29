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
import fasteners


from .time_interval import TimeInterval, TimeIntervals, RelativeTimeInterval
from .utils import UTC, utcnow


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

    @fasteners.interprocess_locked('/tmp/hyperstream.lock')
    def execute(self, debug=False):
        """
        Execute the engine - currently simple executes all workflows.
        """

        if debug:
            # Set some default times for execution (debugging)
            start_time = datetime(year=2016, month=10, day=19, hour=12, minute=28, tzinfo=UTC)
            duration = timedelta(seconds=5)
            end_time = start_time + duration

            relative_interval = RelativeTimeInterval(0, 0)
            time_interval = TimeInterval(start_time, end_time)
            # workflow_id = "lda_localisation_model_predict"
        else:
            duration = 0  # not needed
            relative_interval = self.hyperstream.config.online_engine.interval
            time_interval = relative_interval.absolute(utcnow())

        for _ in range(self.hyperstream.config.online_engine.iterations):
            if not debug:
                # if this takes more than x minutes, kill myself
                signal.alarm(self.hyperstream.config.online_engine.alarm)

            logging.info("Online engine starting up.")

            # self.hyperstream.workflow_manager.set_requested_intervals(workflow_id, TimeIntervals([time_interval]))
            self.hyperstream.workflow_manager.set_all_requested_intervals(TimeIntervals([time_interval]))
            self.hyperstream.workflow_manager.execute_all()

            logging.info("Online engine shutting down.")
            logging.info("")

            sleep(self.hyperstream.config.online_engine.sleep)

            if debug:
                time_interval += duration
            else:
                time_interval = TimeInterval(time_interval.end, utcnow() + timedelta(seconds=relative_interval.end))

