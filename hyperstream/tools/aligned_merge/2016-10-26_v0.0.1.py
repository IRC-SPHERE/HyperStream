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


class AlignedMerge(Tool):
    def __init__(self, names=None):
        super(AlignedMerge, self).__init__(names=names)
        self.names = names if names else []

    # Note: cannot check stream count because the number depends on the length of self.names
    def _execute(self, sources, alignment_stream, interval):
        if self.names and len(self.names) != len(sources):
            raise TypeError("Tool AlignedMerge expected {} streams as input, got {} instead".format(
                len(self.names), len(sources)))
        streams = [iter(source.window(interval, force_calculation=True)) for source in sources]

        # Take data from the execute
        while True:
            try:
                docs = [next(stream) for stream in streams]
                times = [tt for (tt, dd) in docs]
                for tt in times[1:]:
                    if tt != times[0]:
                        raise ValueError("Tool AlignedMerge expects aligned streams, "
                                         "but received conflicting timestamps {} and {}".format(times[0], tt))
                values = [dd for (tt, dd) in docs]
                if self.names is None:
                    yield StreamInstance(times[0], values)
                else:
                    # noinspection PyTypeChecker
                    yield StreamInstance(times[0], dict([(name, values[i]) for i, name in enumerate(self.names)]))
            except StopIteration:
                break
