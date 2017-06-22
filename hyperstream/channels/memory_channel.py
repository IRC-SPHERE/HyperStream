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

from .base_channel import BaseChannel
from ..stream import Stream, StreamInstance, StreamInstanceCollection
from ..time_interval import TimeIntervals
from ..utils import MIN_DATE, StreamNotFoundError, StreamAlreadyExistsError


class MemoryChannel(BaseChannel):
    """
    Channel whose data lives in memory
    """
    def __init__(self, channel_id):
        """
        Initialise the channel
        :param channel_id: The channel id
        """
        super(MemoryChannel, self).__init__(channel_id=channel_id, can_calc=True, can_create=True)
        self.max_stream_id = 0
        self.data = dict()

    @property
    def non_empty_streams(self):
        return dict(filter(lambda x: len(x[1]) > 0, self.data.items()))

    def create_stream(self, stream_id, sandbox=None):
        """
        Must be overridden by deriving classes, must create the stream according to the tool and return its unique
        identifier stream_id
        """
        if stream_id in self.streams:
            raise StreamAlreadyExistsError("Stream with id '{}' already exists".format(stream_id))

        if sandbox is not None:
            raise ValueError("Cannot use sandboxes with memory streams")

        stream = Stream(channel=self, stream_id=stream_id, calculated_intervals=None, sandbox=None)
        
        self.streams[stream_id] = stream
        self.data[stream_id] = StreamInstanceCollection()
        return stream

    def clear_stream(self, stream_id):
        """
        Clears all the data in a given stream
        :param stream_id: The stream id
        :return: None
        """
        if stream_id not in self.streams:
            raise StreamNotFoundError(stream_id)
        self.data[stream_id] = StreamInstanceCollection()

    def clear_all(self):
        """
        Clears all streams in the channel - use with caution!
        :return: None
        """
        for stream_id in self.streams:
            self.data[stream_id] = StreamInstanceCollection()

    def delete_stream(self, stream_id):
        if stream_id not in self.streams:
            raise StreamNotFoundError(stream_id)
        del self.streams[stream_id]
        del self.data[stream_id]

    def update_streams(self, up_to_timestamp):
        raise NotImplementedError
    
    def check_calculation_times(self):
        pass

    def get_results(self, stream, time_interval):
        """
        Calculates/receives the documents in the stream interval determined by the stream
        :param stream: The stream reference
        :param time_interval: The time interval
        :return: The sorted data items
        """
        return [StreamInstance(t, self.data[stream.stream_id][t])
                for t in sorted(self.data[stream.stream_id]) if t in time_interval]

    def get_stream_writer(self, stream):
        def writer(document_collection):
            if stream.stream_id not in self.data:
                raise RuntimeError("Data slot does not exist for {}, perhaps create_stream was not used?"
                                   .format(stream))
            if isinstance(document_collection, StreamInstance):
                self.data[stream.stream_id].append(document_collection)
            elif isinstance(document_collection, list):
                self.data[stream.stream_id].extend(document_collection)
            else:
                raise TypeError('Expected: [StreamInstance, list<StreamInstance>], got {}. '
                                .format(type(document_collection)))
        
        return writer


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
        super(ReadOnlyMemoryChannel, self).__init__(channel_id=channel_id, can_calc=False, can_create=False)
        self.up_to_timestamp = MIN_DATE
        if up_to_timestamp > MIN_DATE:
            self.update_streams(up_to_timestamp)
            self.update_state(up_to_timestamp)

    def create_stream(self, stream_id, sandbox=None):
        raise RuntimeError("Read-only channel")
    
    def get_stream_writer(self, stream):
        raise RuntimeError("Read-only channel")

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
    
    def get_results(self, stream, time_interval):
        raise NotImplementedError

