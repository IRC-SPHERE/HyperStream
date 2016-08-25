"""
The MIT License (MIT)
Copyright (c) 2014-2017 University of Bristol

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.
"""
from memory_channel import MemoryChannel
from ..modifiers import Identity
from ..time_interval import TimeIntervals
from ..utils import MIN_DATE, MAX_DATE, timeit
from sphere_connector_package.sphere_connector import SphereConnector, DataWindow


class SphereChannel(MemoryChannel):
    sphere_connector = SphereConnector(config_filename='config_strauss.json', include_mongo=True,
                                       include_redcap=False, sphere_logger=None)
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

    def get_results(self, stream_ref, **kwargs):
        abs_interval = self.get_absolute_start_end(stream_ref)

        # Testing for infinite recursion
        # if self.visited[stream_ref]:
        #     raise Exception("OUCH!")
        # self.visited[stream_ref] = True

        # TODO: use the need_to_calc_times like the tool channel does

        window = DataWindow(start=abs_interval.start, end=abs_interval.end, sphere_connector=self.sphere_connector)

        # METHOD 1: Directly connect to SPHERE - no need for SPHERE tool
        @timeit
        def method1():
            if "modality" not in kwargs:
                raise KeyError("modality not in tool_parameters")
            if kwargs["modality"] not in window.modalities:
                raise ValueError("unknown modality {}".format(kwargs["modality"]))
            elements = kwargs["elements"] if "elements" in kwargs else None
            return map(reformat, window.modalities[kwargs["modality"]].get_data(elements=elements))

        # METHOD 2: Use the tools
        @timeit
        def method2():
            # TODO: tool.tool is ugly
            stream_ref.tool.tool.execute(None, abs_interval, stream_ref.writer)
            # stream_ref.tool.execute(abs_interval, stream_ref.writer)
            stream_ref.calculated_intervals += TimeIntervals([abs_interval])

            return ((timestamp, data) for (timestamp, data) in self.data[stream_ref.stream_id]
                    if timestamp in abs_interval)
            # result = []
            # for (timestamp, data) in self.data[stream_ref.stream_id]:
            #     if abs_interval.start < timestamp <= abs_interval.end:
            #         result.append((timestamp, data))
            # return result

        # result1 = method1()
        result2 = method2()

        # assert(all(x == y for x, y in zip(result1, result2)))

        # assume that the data are already sorted by time
        return stream_ref.modifiers.execute(result2)

    def get_default_ref(self):
        return {'start': MIN_DATE, 'end': self.up_to_timestamp, 'modifiers': Identity()}

    def get_stream_writer(self, stream_id):
        def writer(document_collection):
            self.data[stream_id].extend(map(reformat, document_collection))
        return writer


def reformat(doc):
    dt = doc['datetime']
    del doc['datetime']
    return dt, doc
