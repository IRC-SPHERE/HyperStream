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

from hyperstream.tool import MultiOutputTool
from hyperstream.stream import StreamInstance, StreamMetaInstance

from copy import deepcopy


class StreamBroadcaster(MultiOutputTool):
    """
    Broadcasts the streams onto the output plate using the function provided applied to the document values
    """

    def __init__(self, func):
        """
        Stream broadcaster

        :param func: The function to be applied to the value in each document
        """
        super(StreamBroadcaster, self).__init__(func=func)

    def _execute(self, source, splitting_stream, interval, meta_data_id, output_plate_values):
        if splitting_stream is not None:
            raise ValueError("Splitting stream not supported for this tool")

        for doc in source.window(interval, force_calculation=True):
            for vv in output_plate_values:
                data = self.func(deepcopy(doc.value))
                yield StreamMetaInstance(StreamInstance(doc.timestamp, data), vv)
