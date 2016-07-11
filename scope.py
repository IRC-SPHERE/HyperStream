"""
The MIT License (MIT)
Copyright (c) 2014-2015 University of Bristol

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

# import logging
from sphere_connector_package.sphere_connector.utils import Printable
from datetime import timedelta, datetime
from dateutil.parser import parse


def parse_time(start, end):
    now = datetime.now()
    if isinstance(start, int):
        offset = timedelta(seconds=start)
        start_time = now - offset
    elif start is None:
        start_time = datetime(1970, 1, 1)
    else:
        start_time = parse(start)

    if isinstance(end, int):
        # TODO: add check for future (negative values) and ensure that start < end
        offset = timedelta(seconds=end)
        end_time = now - offset
    else:
        end_time = parse(end)

    return [start_time, end_time]


class Scope(Printable):
    def __init__(self, _id, time, filters):
        self.scope_id = _id
        [self.start, self.end] = parse_time(time['start'], time['end'])
        self.filters = filters

    def __repr__(self):
        return str(self)
