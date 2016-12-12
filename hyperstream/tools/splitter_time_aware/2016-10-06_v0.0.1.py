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
from hyperstream.time_interval import TimeInterval, TimeIntervals
from hyperstream import IncompatiblePlatesError

import logging


class SplitterTimeAware(MultiOutputTool):
    def __init__(self, time_intervals, meta_data_id):
        super(SplitterTimeAware, self).__init__(time_intervals=time_intervals, meta_data_id=meta_data_id)
        self.time_intervals = time_intervals
        self.meta_data_id = meta_data_id

    def _execute(self, source, splitting_stream, interval, output_plate):
        if splitting_stream is not None:
            raise NotImplementedError("This tool expects the splitting to be provided as a parameter")

        # time intervals could be a TimeIntervals object, a list of TimeInterval objects,
        # or a list of tuples of plate ids and TimeInterval objects
        if output_plate.meta_data_id != self.meta_data_id:
            raise IncompatiblePlatesError("Output plate does not match the specified meta data id")

        mapping = {}
        if isinstance(self.time_intervals, (tuple, list, TimeIntervals)):
            for i, el in enumerate(self.time_intervals):
                if isinstance(el, TimeInterval):
                    pv = str(i + 1)
                    ti = el
                elif isinstance(el, (tuple, list)):
                    if len(el) != 2 or not isinstance(el[0], str) or not isinstance(el[1], TimeInterval):
                        raise ValueError("{}: Unknown data format".format(self.name))
                    pv, ti = el
                else:
                    raise ValueError("{}: Unknown data format".format(self.name))

                if ti not in interval:
                    # This means that the user has requested a time interval of
                    # calculation that doesn't cover the range of the intervals provided as parameters
                    logging.warn("{}: Requested interval doesn't cover the range of the intervals provided"
                                 .format(self.name))
                    continue

                if pv in mapping:
                    raise ValueError("Repeated time intervals for single plate value not currently supported")

                mapping[pv] = ti
        else:
            raise TypeError("Expected [tuple, list, TimeIntervals], got{}".format(type(self.time_intervals)))

        for pv, ti in mapping.items():
            found_data = False
            for instance in source.window(ti, force_calculation=True):
                found_data = True
                yield StreamMetaInstance(instance, (self.meta_data_id, pv))
            if not found_data:
                logging.debug("{}: no data for source {} with plate value {} and time interval {}"
                              .format(self.name, source, pv, ti))
