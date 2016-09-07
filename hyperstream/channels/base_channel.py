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

from ..stream import StreamInstance, StreamDict
from ..time_interval import TimeIntervals
from ..utils import Printable, MIN_DATE, MAX_DATE


class BaseChannel(Printable):
    def __init__(self, channel_id, can_calc=False, can_create=False, calc_agent=None):
        self.channel_id = channel_id
        self.streams = StreamDict()
        self.can_calc = can_calc
        self.can_create = can_create
        self.calc_agent = calc_agent
        self.up_to_timestamp = MAX_DATE

    def update_streams(self, up_to_timestamp):
        """
        Deriving classes must override this function
        """
        raise NotImplementedError

    def _get_data(self, stream):
        """
        Default way of getting data from streams. Can be overridden for special stream types
        :param stream: The stream reference
        :return: The data generator
        """
        return sorted((StreamInstance(timestamp, data) for (timestamp, data) in stream.items()
                       if timestamp in stream.time_interval), key=lambda x: x.timestamp)

    def execute_tool(self, stream, interval):
        """
        Executes the stream's tool over the given time interval
        :param stream: the stream reference
        :param interval: the time interval
        :return: None
        """
        if interval.end > self.up_to_timestamp:
            raise ValueError(
                'The stream is not available after ' + str(self.up_to_timestamp) + ' and cannot be calculated')

        required_intervals = TimeIntervals([interval]) - stream.calculated_intervals
        if not required_intervals.is_empty:
            for interval in required_intervals:
                stream.tool.execute(stream.input_streams, interval, stream.writer)
                stream.calculated_intervals += TimeIntervals([interval])

            if stream.required_intervals.is_not_empty:
                raise RuntimeError('Tool execution did not cover the specified time interval.')

        # stream.tool.execute(stream.input_streams, interval, stream.writer)

    def get_results(self, stream):
        """
        Must be overridden by deriving classes.
        1. Calculates/receives the documents in the stream for the time interval given
        2. Returns success or failure and the results (for some channels the values of kwargs can override the
        return process, e.g. introduce callbacks)
        """
        raise NotImplementedError

    def get_or_create_stream(self, stream_id):  # , tool_stream=None):
        """
        Helper function to get a stream or create one if it's not already defined
        :param stream_id: The stream id
        :param tool_stream: The tool stream
        :return: The stream object
        """
        if stream_id in self.streams:
            print("found {}".format(stream_id))
            return self.streams[stream_id]
        else:
            # Try to create the stream
            print("creating {}".format(stream_id))
            return self.create_stream(stream_id=stream_id)  # , tool_stream=tool_stream)

    def create_stream(self, stream_id):  # , tool_stream=None):
        """
        Must be overridden by deriving classes, must create the stream according to the tool and return its unique
        identifier stream_id
        """
        raise NotImplementedError

    def get_stream_writer(self, stream):
        """
        Must be overridden by deriving classes, must return a function(document_collection) which writes all the
        given documents of the form (timestamp,data) from document_collection to the stream
           Example:
           if stream_id==1:
         def f(document_collection):
           for (timestamp,data) in document_collection:
             database[timestamp] = data
             return(f)
           else:
             raise Exception('No stream with id '+str(stream_id))
        """
        raise NotImplementedError

    def __str__(self):
        s = self.__class__.__name__ + ' with ID: ' + str(self.channel_id)
        s += ' and containing {} streams:'.format(len(self.streams))
        # for stream_id in self.streams:
        #     calculated_ranges = repr(self.streams[stream_id].calculated_intervals) \
        #         if stream_id in self.calculated_intervals else "Error - stream not found"
        for stream in self.streams:
            calculated_ranges = repr(stream.calculated_intervals)
            s += '\nSTREAM ID: ' + str(stream.stream_id)
            s += "\n  CALCULATED RANGES: " + calculated_ranges
            s += "\n  STREAM DEFINITION: "
            s += str(stream)
        return s
    
    def __getitem__(self, item):
        return self.streams[item]

    def __setitem__(self, key, value):
        self.streams[key] = value

    def __contains__(self, item):
        return item in self.streams
        # TODO: More elegant way of doing this
        # try:
        #     var = self[item]
        #     return True
        # except KeyError:
        #     return False
