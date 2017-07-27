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

from hyperstream import TimeInterval
from .helpers import *


def print_sessions(hs):
    print("Sessions:")
    print("------")
    if hs.sessions:
        for s in hs.sessions:
            print(s)
    else:
        print("[]")
    print("")
    print("------")
    print("Current session")
    print("------")
    print(hs.current_session)
    print("------")
    print("")
    print("")


class SessionTests(unittest.TestCase):
    def test_sessions(self):
        hs = HyperStream(loglevel=logging.CRITICAL)
        print_sessions(hs)
        # hs.clear_sessions(inactive_only=False, clear_history=True)

        # TODO: this needs to clear stream definitions as well
        hs.clear_sessions(clear_history=True)
        print("after clearing")
        print_sessions(hs)
        assert (len(hs.sessions) == 0)
        del hs

        with HyperStream(loglevel=logging.CRITICAL) as hs:
            print("enter ...")
            print_sessions(hs)
            assert (len(hs.sessions) == 1)
            assert hs.current_session.active

            M = hs.channel_manager.memory
            dg = hs.plugins.data_generators

            ticker = M.get_or_create_stream("ticker")
            random = M.get_or_create_stream("random")

            ti = TimeInterval(t1, t1 + minute)

            hs.tools.clock().execute(sources=[], sink=ticker, interval=ti)
            dg.tools.random().execute(sources=[], sink=random, interval=ti, alignment_stream=ticker)

            history = hs.current_session.history
            for item in history:
                print(item)

            assert (history[0].value['tool'] == 'clock')
            assert (history[1].value['tool'] == 'random')
            assert (history[0].value['document_count'] == 60)
            assert (history[1].value['document_count'] == 60)

        print("exit ...")

        hs = HyperStream(loglevel=logging.CRITICAL)

        assert hs.current_session is None
        print_sessions(hs)
        assert (len(hs.sessions) == 1)
        assert hs.sessions[0].end is not None
        assert not hs.sessions[0].active
