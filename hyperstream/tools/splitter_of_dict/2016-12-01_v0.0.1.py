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
import logging
from copy import deepcopy


class SplitterOfDict(MultiOutputTool):
    def __init__(self, mapping):
        super(SplitterOfDict, self).__init__(mapping=mapping)
        self.mapping = mapping

    def _execute(self, source, splitting_stream, interval, output_plate):
        if splitting_stream is not None:
            raise NotImplementedError("Splitting stream not supported for this tool")

        for timestamp, value in source.window(interval, force_calculation=True):
            for key in value.keys():
                if key not in self.mapping:
                    logging.warn("Unknown value {} for meta data in SplitterOfDict".format(key))
                    continue
                plate_value = self.mapping[key]
                yield StreamMetaInstance((timestamp, value[key]), (output_plate.meta_data_id, plate_value))
