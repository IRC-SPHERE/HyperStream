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


class Splitter(MultiOutputTool):
    def __init__(self, mapping):
        super(Splitter, self).__init__(mapping=mapping)
        self.mapping = mapping

    def _execute(self, source, sinks, interval, output_plate):
        for timestamp, value in source.window(interval).items():
            if self.mapping[0] not in value:
                logging.debug("Mapping {} not in instance".format(self.mapping[0]))
                continue
            meta_data = value.pop(self.mapping[0])
            if meta_data not in self.mapping[1]:
                logging.warn("Unknown value {} for meta data {}".format(meta_data, self.mapping[0]))
                continue
            plate_value = self.mapping[1][meta_data]
            yield StreamMetaInstance((timestamp, value), (output_plate.meta_data_id, plate_value))
            # TODO: Put this into the correct stream. Perhaps we should just yield the value along with which plate it
            # should go onto, and let the MultiOutputTool object put it onto the correct plate

