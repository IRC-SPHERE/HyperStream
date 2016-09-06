# The MIT License (MIT)
#  Copyright (c) 2014-2017 University of Bristol
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#  # The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
#  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
#  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
#  OR OTHER DEALINGS IN THE SOFTWARE.
from base_channel import BaseChannel
from ..models import StreamInstanceModel
from ..stream import StreamInstance
from ..time_interval import TimeIntervals
from ..utils import MIN_DATE, utcnow


class DatabaseChannel(BaseChannel):
    def __init__(self, channel_id):
        super(DatabaseChannel, self).__init__(channel_id=channel_id, can_calc=True, can_create=False)
        self.update_streams(utcnow())

    def update_streams(self, up_to_timestamp):
        intervals = TimeIntervals([(MIN_DATE, up_to_timestamp)])
        # for stream_id in self.streams.keys():
        #     self.streams[stream_id].calculated_intervals = intervals
        for stream in self.streams:
            stream.calculated_intervals = intervals
        self.up_to_timestamp = up_to_timestamp

    def _get_data(self, stream_ref):
        # TODO: Get the data from the database. Should the responsibility be here or in Stream.Items()?
        return sorted((StreamInstance(timestamp, data) for (timestamp, data) in stream_ref.items()
                       if timestamp in stream_ref.time_interval), key=lambda x: x.timestamp)

    def get_results(self, stream_ref):
        # TODO: This is identical to MemoryChannel - can they share a base instantiation (BaseChannel)??
        if not stream_ref.required_intervals.is_empty:
            for interval in stream_ref.required_intervals:
                self.execute_tool(stream_ref, interval)
                stream_ref.calculated_intervals += TimeIntervals([interval])

            if stream_ref.required_times.is_not_empty:
                raise RuntimeError('Tool execution did not cover the specified interval.')

        return self._get_data(stream_ref)

        # abs_end, abs_start = self.get_absolute_start_end(stream_ref)
        #
        # done_calc_times = stream_ref.calculated_intervals
        # need_to_calc_times = TimeIntervals([(abs_start, abs_end)]) - done_calc_times
        #
        # self.do_calculations(stream_ref, abs_start, abs_end, need_to_calc_times)
        #
        # result = []
        # for (timestamp, data) in stream_ref.items():
        #     if abs_start < timestamp <= abs_end:
        #         result.append((timestamp, data))
        #
        # result.sort(key=lambda x: x[0])
        #
        # # make a generator out from result and then apply the modifiers
        # result = stream_ref.modifiers(iter(result))  # (x for x in result))
        # return result
    
    # def do_calculations(self, stream_ref, abs_start, abs_end, need_to_calc_times):
    #     if need_to_calc_times.is_empty:
    #         return
    #
    #     tool = stream_ref.tool
    #
    #     for (start2, end2) in need_to_calc_times.value:
    #         kwargs2 = self.get_params(stream_ref.kwargs, start2, end2)
    #
    #         # Here we're actually executing the tool
    #         tool(stream_ref, start2, end2, stream_ref.writer, **kwargs2)
    #
    #         stream_ref.calculated_intervals += TimeIntervals([(start2, end2)])
    #
    #     done_calc_times = stream_ref.calculated_intervals
    #     need_to_calc_times = TimeIntervals([(abs_start, abs_end)]) - done_calc_times
    #     logging.debug(done_calc_times)
    #     logging.debug(need_to_calc_times)
    #
    #     if need_to_calc_times.is_not_empty:
    #         raise ValueError('Tool execution did not cover the specified interval.')

    def create_stream(self, stream_id, tool_stream=None):
        # TODO: Functionality here
        raise NotImplementedError("Database streams currently need to be defined in the database")

    def get_stream_writer(self, stream_ref):
        def writer(document_collection):
            # TODO: Does this check whether a stream_id/datetime pair already exists in the DB? (unique pairs?)
            for t, doc in document_collection:
                instance = StreamInstanceModel(
                    stream_id=stream_ref.stream_id,
                    stream_type=stream_ref.stream_type,
                    datetime=t,
                    metadata=stream_ref.tool.metadata,
                    version=stream_ref.tool.version,
                    value=doc
                )
                instance.save()
        return writer
