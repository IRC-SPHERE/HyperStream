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

from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count


class RssiToVector(Tool):
    """
    Simple tool that picks out a component of the data dict if it is a range of values. If compliment is true,
    the component is picked out if it is not in the list. e.g. to delete None values, use:
    ComponentFilter(key, values=[None], complement=true)
    """
    def __init__(self, aids, default_value):
        super(RssiToVector, self).__init__(aids=aids, default_value=default_value)
        self.aids = aids
        self.default_value = default_value

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        for time, data in sources[0].window(interval, force_calculation=True):
            res = self.aids[:]
            for i in range(len(res)):
                res[i] = self.default_value
            if "gw" in data.keys():
                for subdoc in data["gw"]:
                    for i in range(len(self.aids)):
                        if subdoc["aid"]==self.aids[i]:
                            res[i] = subdoc["rssi"]
            yield StreamInstance(time, res)
