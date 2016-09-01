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

# TODO: the sphere channel should come out of hyperstream

import os
from collections import Iterable
import logging

from sphere_connector_package.sphere_connector import SphereConnector, DataWindow

from memory_channel import MemoryChannel
from ..stream import StreamInstance
from ..time_interval import TimeIntervals, TimeInterval
from ..utils import MIN_DATE, MAX_DATE

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sphere_connector = SphereConnector(
    config_filename=os.path.join(path, 'config_strauss.json'),
    include_mongo=True,
    include_redcap=False,
    sphere_logger=None)


class SphereDataWindow(DataWindow):
    """
    Helper class to use the global sphere_connector object
    """
    def __init__(self, time_interval):
        if isinstance(time_interval, TimeInterval):
            start, end = time_interval.to_tuple()
        elif isinstance(time_interval, Iterable):
            start, end = time_interval
        else:
            raise TypeError
        super(SphereDataWindow, self).__init__(sphere_connector, start, end)


class SphereChannel(MemoryChannel):
    """
    SPHERE MongoDB storing the raw sensor data
    """

    def __init__(self, channel_id, up_to_timestamp=None):
        super(SphereChannel, self).__init__(channel_id=channel_id)

        if up_to_timestamp is None:
            # TODO: maybe should be utcnow, but then we'd have to keep updating it?
            up_to_timestamp = MAX_DATE  # utcnow()

        if up_to_timestamp > MIN_DATE:
            self.update_streams(up_to_timestamp)

        # from collections import defaultdict
        # self.visited = defaultdict(lambda: False)

    def update_streams(self, up_to_timestamp):
        """
        Call this function to report to the system that the SPHERE MongoDB is fully populated until up_to_timestamp
        """
        for stream_id in self.streams:
            self.streams[stream_id].calculated_intervals = TimeIntervals([(MIN_DATE, up_to_timestamp)])
        self.up_to_timestamp = up_to_timestamp

    def execute_tool(self, stream_ref, interval):
        try:
            stream_ref.tool.execute(None, interval, stream_ref.writer)
        except AttributeError:
            raise

    def _get_data_not_used(self, stream_ref, **kwargs):
        """
        Another version of _get_data, which directly uses SphereDataWindow, rather than executing the tool
        :param stream_ref: Stream reference
        :param kwargs: Keyword arguments
        :return: The data generator
        """
        # TODO: Perhaps this is sufficient, rather than requiring all of the SPHERE specific tools
        window = SphereDataWindow(stream_ref.time_interval)
        if "modality" not in kwargs:
            raise KeyError("modality not in tool_parameters")
        if kwargs["modality"] not in window.modalities:
            raise ValueError("unknown modality {}".format(kwargs["modality"]))
        elements = kwargs["elements"] if "elements" in kwargs else None
        return map(reformat, window.modalities[kwargs["modality"]].get_data(elements=elements))

    def _get_data(self, stream_ref):
        """
        Gets the data. Assumes that it is already sorted by timestamp
        :param stream_ref: The stream reference
        :return: The data generator
        """
        logging.debug((id(self), len(self.data)))
        # return (d for d in self.data[stream_ref]
        #         if d.timestamp in stream_ref.time_interval)
        for d in self.data[stream_ref.stream_id]:
            if d.timestamp in stream_ref.time_interval:
                yield d

    def get_stream_writer(self, stream_ref):
        def writer(document_collection):
            data = map(reformat, document_collection)
            self.data[stream_ref.stream_id].extend(data)
            logging.debug((id(self), len(self.data)))
        return writer


def reformat(doc):
    dt = doc['datetime']
    del doc['datetime']
    return StreamInstance(dt, doc)
