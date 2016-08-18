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
from datetime import timedelta, datetime
from ..time_interval import TimeIntervals, TimeInterval
from ..utils import MIN_DATE
import logging


class MemoryChannel(BaseChannel):
    def __init__(self, channel_id):
        super(MemoryChannel, self).__init__(channel_id=channel_id, can_calc=True, can_create=True)
        self.max_stream_id = 0
    
    def create_stream(self, stream_id, stream_def):
        """
        Must be overridden by deriving classes, must create the stream according to stream_def and return its unique
        identifier stream_id
        """
        # TODO: No particular reason for integer IDs?
        # self.max_stream_id += 1
        # stream_id = self.max_stream_id
        
        # TODO: Why is this just a list?
        self.streams[stream_id] = []
        # return stream_id
    
    def get_params(self, x, start, end):
        if isinstance(x, (list, tuple)):
            res = []
            for x_i in x:
                res.append(self.get_params(x_i, start, end))
            if isinstance(x, list):
                return res
            else:
                return tuple(res)
        elif isinstance(x, dict):
            res = {}
            for x_i in x:
                res[x_i] = self.get_params(x[x_i], start, end)
            return res
        elif isinstance(x, StreamReference):
            return x(time_interval=TimeInterval(start=start, end=end))
        else:
            return x
    
    def get_results(self, stream_ref, args, kwargs):
        stream_id = stream_ref.stream_id
        abs_end, abs_start = self.get_absolute_start_end(kwargs, stream_ref)
        try:
            done_calc_times = self.state.calculated_intervals[stream_id]
        except KeyError as e:
            # For debugging
            raise e
        
        need_to_calc_times = TimeIntervals([(abs_start, abs_end)]) - done_calc_times
        
        self.do_calculations(stream_id, abs_start, abs_end, need_to_calc_times)
        
        result = []
        for (timestamp, data) in self.streams[stream_ref.stream_id]:
            if abs_start < timestamp <= abs_end:
                result.append((timestamp, data))
                
        result.sort(key=lambda x: x[0])
        result = stream_ref.modifier.execute(
            (x for x in result))
        
        return result
    
    def do_calculations(self, stream_id, abs_start, abs_end, need_to_calc_times):
        if need_to_calc_times.is_empty:
            return
            
        stream_def = self.state.stream_id_to_definition_mapping[stream_id]
        writer = self.get_stream_writer(stream_id)
        try:
            tool = stream_def.tool
        except AttributeError as e:
            # For debugging
            raise e
        
        for interval in need_to_calc_times.intervals:
            args = self.get_params(stream_def.args, interval.start, interval.end)
            kwargs = self.get_params(stream_def.kwargs, interval.start, interval.end)
            
            tool.execute(
                stream_def,
                interval.start,
                interval.end,
                writer,
                *args,
                **kwargs
            )
            
            self.state.calculated_intervals[stream_id] += TimeIntervals([interval])
        
        done_calc_times = self.state.calculated_intervals[stream_id]
        need_to_calc_times = TimeIntervals([(abs_start, abs_end)]) - done_calc_times
        logging.debug(done_calc_times)
        logging.debug(need_to_calc_times)
        
        if need_to_calc_times.is_not_empty:
            raise ValueError('Tool execution did not cover the specified interval.')
    
    def get_stream_writer(self, stream_id):
        def writer(document_collection):
            # TODO: What is actually happening here?
            self.streams[stream_id].extend(document_collection)
        
        return writer
    
    def get_default_ref(self):
        return {'start': None, 'end': None, 'modifier': Identity()}


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
            self.update(up_to_timestamp)
    
    def create_stream(self, stream_id, stream_def):
        raise ValueError("Read-only channel")
    
    def get_stream_writer(self, stream_id):
        raise ValueError("Read-only channel")
    
    def repr_stream(self, stream_id):
        return 'externally defined, memory-based, read-only stream'
    
    def update_streams(self, up_to_timestamp):
        """
        Deriving classes must override this function
        """
        raise NotImplementedError
    
    def update(self, up_to_timestamp):
        """
        Call this function to ensure that the channel is up to date at the time of timestamp.
        I.e., all the streams that have been created before or at that timestamp are calculated exactly until
        up_to_timestamp.
        """
        self.update_streams(up_to_timestamp)
        self.update_state(up_to_timestamp)
    
    def update_state(self, up_to_timestamp):
        for stream_id in self.streams:
            intervals = TimeIntervals([(MIN_DATE, up_to_timestamp)])
            self.state.calculated_intervals[stream_id] = intervals
        self.up_to_timestamp = up_to_timestamp
    
    def get_results(self, stream_ref, args, kwargs):
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
        result = stream_ref.modifier.execute(
            (x for x in result))  # make a generator out from result and then apply the modifier
        return result
