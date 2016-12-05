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


class SplitterWithUnlist(MultiOutputTool):
    def __init__(self, element, subelement, mapping):
        super(SplitterWithUnlist, self).__init__(element=element, subelement=subelement, mapping=mapping)

    def _execute(self, source, splitting_stream, interval, output_plate):
        if splitting_stream is not None:
            raise NotImplementedError("Splitting stream not supported for this tool")

        for timestamp, value in source.window(interval, force_calculation=True):
            value = deepcopy(value)
            if self.element not in value:
                logging.debug("Mapping element {} not in instance".format(self.element))
                continue
            value_list = value.pop(self.element)
            for subvalue in value_list:
                subvalue = deepcopy(subvalue)
                if self.subelement not in subvalue:
                    logging.debug("Mapping subelement {} not in subinstance".format(self.subelement))
                    continue
                meta_data = str(subvalue.pop(self.subelement))
                if meta_data not in self.mapping:
                    logging.warn("Unknown value {} for meta data {}".format(meta_data, self.subelement))
                    continue
                plate_value = self.mapping[meta_data]
                subvalue.update(value)
                yield StreamMetaInstance((timestamp, subvalue), (output_plate.meta_data_id, plate_value))
