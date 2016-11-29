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

from hyperstream.tool import Tool, check_input_stream_count
from collections import defaultdict
from hyperstream.stream import StreamInstance


class RelativeApply2(Tool):
    """
    Modification of RelativeApply that applies the functions to the keys as well as the values
    """

    def __init__(self, func):
        super(RelativeApply2, self).__init__(func=func)
        self.func = func

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        for tt, rows in sources[0].window(interval, force_calculation=True):
            values = defaultdict(list)

            for row in rows:
                # try:
                # for kk, vv in iter(row.value):
                for kk, vv in row.iteritems():
                    if isinstance(vv, (int, float)):
                        values[kk].append(vv)
                        # except AttributeError:
                        #     # This is not iterable, try to apply directly here
                        #     if isinstance(row, (int, float)):
                        #         values[None].append(row)
                    else:
                        # raise NotImplementedError
                        # TODO: Need to store other things like the uid for output
                        pass

            # if len(values) == 1 and None in values:
            #     yield StreamInstance(tt, self.func(iter(values[None])))
            # else:
            for kk, vv in values.iteritems():
                try:
                    result = self.func(kk, vv)
                    if result is not None:
                        v = {kk: result}
                        yield StreamInstance(tt, v)
                except KeyError:
                    pass
