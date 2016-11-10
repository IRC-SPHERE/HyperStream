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
from hyperstream.stream import StreamMetaInstance
from hyperstream.time_interval import TimeInterval
from hyperstream.utils import MIN_DATE, MAX_DATE


class StreamBroadcaster(MultiOutputTool):
    def __init__(self, mapping):
        super(StreamBroadcaster, self).__init__(mapping=mapping)
        self.mapping = mapping
    
    def _execute(self, source, interval, output_plate):
        # TODO: This factor is currently used to pull out the parameters of a localisation model, and as such does \
        #   use the time interval, but only pulls the last instance in the stream. Will need to change this in \
        #   future instances
        param_doc = source.window(TimeInterval(MIN_DATE, MAX_DATE), force_calculation=True).last()
        
        for kk, vv in self.mapping.iteritems():
            yield StreamMetaInstance(
                (param_doc.timestamp, param_doc.value), (output_plate.meta_data_id, str(vv)))
