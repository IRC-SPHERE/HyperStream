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
from base_channel import BaseChannel
from ..stream import StreamReference
from ..modifiers import Identity
from datetime import timedelta
from ..time_interval import TimeIntervals, TimeInterval
from ..utils import MIN_DATE, MAX_DATE
import logging
from collections import defaultdict


class MemoryChannel(BaseChannel):
    def __init__(self, channel_id):
        super(MemoryChannel, self).__init__(channel_id=channel_id, can_calc=True, can_create=True)
        self.max_stream_id = 0
        self.data = defaultdict(list)

    def create_stream(self, stream_id, tool):
        """
        Must be overridden by deriving classes, must create the stream according to the tool and return its unique
        identifier stream_id
        """
        # TODO: check time_interval and get_results_func in the below
        self.streams[stream_id] = StreamReference(
            channel=self,
            stream_id=stream_id,
            time_interval=TimeInterval(MIN_DATE, MAX_DATE),
            calculated_intervals=TimeIntervals(),
            modifiers=Identity(),
            get_results_func=tool.execute,
            tool=tool,
            input_streams=None
        )

    def update_streams(self, up_to_timestamp):
        raise NotImplementedError

    def get_results(self, stream_ref, **kwargs):
        abs_interval = self.get_absolute_start_end(stream_ref)

        need_to_calc_times = TimeIntervals([abs_interval]) - stream_ref.calculated_intervals

        if not need_to_calc_times.is_empty:
            for interval in need_to_calc_times.intervals:
                try:
                    stream_ref.tool.execute(stream_ref.input_streams, interval=interval, writer=stream_ref.writer)
                except AttributeError as e:
                    # TODO: for debugging
                    raise e
                stream_ref.calculated_intervals += TimeIntervals([interval])

            need_to_calc_times = TimeIntervals([abs_interval]) - stream_ref.calculated_intervals
            # logging.debug(stream_ref.calculated_intervals)
            # logging.debug(need_to_calc_times)

            if need_to_calc_times.is_not_empty:
                raise ValueError('Tool execution did not cover the specified interval.')

        result = []
        for (timestamp, data) in self.data[stream_ref.stream_id]:
            if timestamp in abs_interval:
                result.append((timestamp, data))

        result.sort(key=lambda x: x[0])
        result = stream_ref.modifiers.execute(iter(result))
        
        return result
    
    def get_stream_writer(self, stream_id):
        def writer(document_collection):
            self.data[stream_id].extend(document_collection)
        return writer
    
    def get_default_ref(self):
        return {'start': None, 'end': None, 'modifiers': Identity()}


class ReadOnlyMemoryChannel(BaseChannel):
    """
    An abstract channel with a read-only set of memory-based streams.
    By default it is constructed empty with the last update at MIN_DATE.
    New streams and documents within streams are created with the update(up_to_timestamp) method,
    which ensures that the channel is up to date until up_to_timestamp.
    No documents nor streams are ever deleted.
    Any deriving class must override update_streams(up_to_timestamp) which must update self.streams to be calculated
    until up_to_timestamp exactly.
    The data structure self.streams is a dict of streams indexed by stream_id, each stream is a list of tuples
    (timestamp,data), in no specific order.
    Names and identifiers are the same in this channel.
    """
    
    def __init__(self, channel_id, up_to_timestamp=MIN_DATE):
        # TODO: should the up_to_timestamp parameter be up to datetime.max?
        super(ReadOnlyMemoryChannel, self).__init__(channel_id=channel_id, can_calc=False, can_create=False)
        self.up_to_timestamp = MIN_DATE
        if up_to_timestamp > MIN_DATE:
            self.update_streams(up_to_timestamp)
            self.update_state(up_to_timestamp)

    def create_stream(self, stream_id, tool):
        raise ValueError("Read-only channel")
    
    def get_stream_writer(self, stream_id):
        raise ValueError("Read-only channel")
    
    # def str_stream(self, stream_id):
    #     return 'externally defined, memory-based, read-only stream'

    def update_streams(self, up_to_timestamp):
        """
        Deriving classes must override this function
        """
        raise NotImplementedError

    def update_state(self, up_to_timestamp):
        """
        Call this function to ensure that the channel is up to date at the time of timestamp.
        I.e., all the streams that have been created before or at that timestamp are calculated exactly until
        up_to_timestamp.
        """
        for stream_id in self.streams:
            self.streams[stream_id].calculated_intervals = TimeIntervals([(MIN_DATE, up_to_timestamp)])
        self.up_to_timestamp = up_to_timestamp
    
    def get_results(self, stream_ref, **kwargs):
        start = stream_ref.time_interval.start
        end = stream_ref.time_interval.end
        if isinstance(start, timedelta) or isinstance(end, timedelta):
            raise ValueError('Cannot calculate a relative stream_ref')
        if end > self.up_to_timestamp:
            raise ValueError(
                'The stream is not available after ' + str(self.up_to_timestamp) + ' and cannot be calculated')
        result = []
        for (tool_info, data) in self.streams[stream_ref.stream_id]:
            if start < tool_info.timestamp <= end:
                result.append((tool_info.timestamp, data))
        result.sort(key=lambda x: x[0])
        result = stream_ref.modifiers.execute(
            (x for x in result))  # make a generator out from result and then apply the modifiers
        return result
