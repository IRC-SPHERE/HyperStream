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

from . import BaseTool
from ..time_interval import TimeInterval

import logging


class PlateCreationTool(BaseTool):
    """
    Special type of tool that creates a new plate.
    There is no sink in this case, as it does not yet exist.
    Note that no alignment stream is required here.
    Also note that we don't subclass Tool due to different calling signatures
    """
    def _execute(self, source, interval):
        """
        Tool implementations should override this function to actually perform computations

        :param source: The source stream
        :param interval: The time interval
        :type source: Stream
        :type interval: TimeInterval
        :return: None
        """
        raise NotImplementedError

    def execute(self, source, interval, input_plate_value):
        """
        Execute the tool over the given time interval.

        :param source: The source stream
        :param interval: The time interval
        :param input_plate_value: The value of the plate where data comes from (can be None)
        :type source: Stream
        :type interval: TimeInterval
        :type input_plate_value: tuple[tuple[str, str]] | None
        :return: None
        """
        if not isinstance(interval, TimeInterval):
            raise TypeError('Expected TimeInterval, got {}'.format(type(interval)))
        # logging.info(self.message(interval))

        output_plate_values = set()

        for item in self._execute(source=source, interval=interval):
            # Join the output meta data with the parent plate meta data
            # meta_data = input_plate_value + (item.meta_data,)
            # sink.writer(item.stream_instance)
            output_plate_values.add(item.meta_data, )

        if not output_plate_values:
            logging.debug("{} did not produce any data for time interval {} on stream {}".format(
                self.name, interval, source))

        return output_plate_values
