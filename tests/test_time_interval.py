"""
The MIT License (MIT)
Copyright (c) 2014-2017 University of Bristol

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.
"""
import unittest
from hyperstream import TimeInterval, TimeIntervals, RelativeTimeInterval
from helpers import *


class HyperStreamTimeIntervalTests(unittest.TestCase):
    def test_constructors(self):
        # TODO: is something supposed to go here?
        pass
    
    def test_time_interval(self):
        i1 = TimeIntervals([
            TimeInterval(now, now + hour),
            TimeInterval(now + 2 * hour, now + 3 * hour),
        ])
        
        i2 = TimeIntervals([
            TimeInterval(now + 30 * minute, now + 30 * minute + 2 * hour),
        ])
        
        # print(i1)
        assert (i1 == TimeIntervals(intervals=[TimeInterval(start=datetime(2016, 1, 1, 0, 0),
                                                            end=datetime(2016, 1, 1, 1, 0)),
                                               TimeInterval(start=datetime(2016, 1, 1, 2, 0),
                                                            end=datetime(2016, 1, 1, 3, 0))]))
        
        # print(i2)
        # print()
        s = i1 + i2
        assert (s == TimeIntervals(intervals=[TimeInterval(start=datetime(2016, 1, 1, 0, 0),
                                                           end=datetime(2016, 1, 1, 3, 0))]))
        
        d = i1 - i2
        
        assert (d == TimeIntervals(intervals=[TimeInterval(start=datetime(2016, 1, 1, 0, 0),
                                                           end=datetime(2016, 1, 1, 0, 30)),
                                              TimeInterval(start=datetime(2016, 1, 1, 2, 30),
                                                           end=datetime(2016, 1, 1, 3, 0))]))
        # print(s)
        # print(d)
        # print()

    def test_relative_time_interval(self):
        # TODO ... write some tests here
        r1 = RelativeTimeInterval(-30 * second, zero)
        pass

if __name__ == '__main__':
    unittest.main()
