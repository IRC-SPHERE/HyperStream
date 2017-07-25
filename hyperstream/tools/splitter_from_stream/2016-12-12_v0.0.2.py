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

from hyperstream import TimeInterval, MIN_DATE
from hyperstream.tool import MultiOutputTool
from hyperstream.stream import StreamMetaInstance, AssetStream
import logging
from copy import deepcopy


class SplitterFromStream(MultiOutputTool):
    """
    This version of the splitter assumes that the mapping exists as the last element in a (asset) stream
    From version 0.0.2 onwards it supports element=None (the default value), in which case each document is assumed to
    be a dict with keys corresponding to plate values, the respective values will then be written into corresponding
    streams
    """
    def __init__(self, element=None):
        super(SplitterFromStream, self).__init__(element=element)

    def _execute(self, source, splitting_stream, interval, meta_data_id, output_plate_values):
        if splitting_stream is None:
            raise ValueError("Splitting stream required for this tool")

        if isinstance(splitting_stream, AssetStream):
            time_interval = TimeInterval(MIN_DATE, interval.end)
            splitter = splitting_stream.window(time_interval, force_calculation=True).last()
        else:
            splitter = splitting_stream.window(interval, force_calculation=True).last()

        if not splitter:
            logging.debug("No assets found for source {} and splitter {}"
                          .format(source.stream_id, splitting_stream.stream_id))
            return

        mapping = splitter.value

        for timestamp, value in source.window(interval, force_calculation=True):
            if self.element is None:
                for plate_value, sub_value in value.items():
                    if plate_value in mapping.keys():
                        yield StreamMetaInstance((timestamp, sub_value), (meta_data_id, plate_value))
                    else:
                        logging.error("Unexpected splitting value {}".format(plate_value))
            else:
                if self.element not in value:
                    logging.debug("Mapping element {} not in instance".format(self.element))
                    continue
                value = deepcopy(value)
                meta_data = str(value.pop(self.element))
                if meta_data not in mapping:
                    logging.warn("Unknown value {} for meta data {}".format(meta_data, self.element))
                    continue
                plate_value = mapping[meta_data]
                yield StreamMetaInstance((timestamp, value), (meta_data_id, plate_value))
