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

from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool

import logging


class AlignedMerge(Tool):
    """
    Merges streams that should have aligned timestamps.
    This version takes account of missing data in streams, but is much less efficient.
    """
    def __init__(self, names=None):
        super(AlignedMerge, self).__init__(names=names)
        self.names = names if names else []

    # Note: cannot check stream count because the number depends on the length of self.names
    def _execute(self, sources, alignment_stream, interval):
        if self.names and len(self.names) != len(sources):
            raise TypeError("Tool AlignedMerge expected {} streams as input, got {} instead".format(
                len(self.names), len(sources)))
        streams = [list(source.window(interval, force_calculation=True)) for source in sources]

        # Get all available timestamps. see https://goo.gl/uyA7pt
        timestamps = sorted(set().union(*([i.timestamp for i in s] for s in streams)))

        for timestamp in timestamps:
            try:
                tt, values = zip(*[s[0] for s in streams])
            except IndexError:
                logging.warn("{}: Stream empty (lengths {})".format(self.name, ", ".join(map(str, map(len, streams)))))
                return

            # If one of the streams doesn't have this timestamp, it's a misalignment
            matches = map(lambda t: t == timestamp, tt)
            if all(matches):
                if self.names is None:
                    yield StreamInstance(timestamp, values)
                else:
                    # noinspection PyTypeChecker
                    yield StreamInstance(timestamp, dict([(name, values[i]) for i, name in enumerate(self.names)]))

            for i, match in enumerate(matches):
                if match:
                    # remove from the lists
                    streams[i].pop(0)
