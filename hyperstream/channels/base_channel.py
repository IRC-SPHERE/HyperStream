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
from ..stream import StreamInstance, StreamDict
from ..modifiers import Identity
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

    def _get_data(self, stream_ref):
        """
        Default way of getting data from streams. Can be overridden for special stream types
        :param stream_ref: The stream reference
        :return: The data generator
        """
        return sorted((StreamInstance(timestamp, data) for (timestamp, data) in stream_ref.items()
                       if timestamp in stream_ref.absolute_interval), key=lambda x: x.timestamp)

    def execute_tool(self, stream_ref, interval):
        """
        Executes the stream's tool over the given time interval
        :param stream_ref: the stream reference
        :param interval: the time interval
        :return: None
        """
        stream_ref.tool.execute(stream_ref.input_streams, interval, stream_ref.writer)

    def get_results(self, stream_ref):  # TODO: force_calc=False):
        """
        Must be overridden by deriving classes.
        1. Calculates/receives the documents in the stream interval determined by the stream_ref
        2. Applies the modifiers within stream_ref
        3. Applies channel custom modifiers as determined by kwargs
        4. Returns success or failure and the results (for some channels the values of kwargs can override the
        return process, e.g. introduce callbacks)
        """
        raise NotImplementedError
    
    def create_stream(self, stream_id, tool=None):
        """
        Must be overridden by deriving classes, must create the stream according to the tool and return its unique
        identifier stream_id
        """
        raise NotImplementedError

    def get_stream_writer(self, stream_id):
        """
        Must be overridden by deriving classes, must return a function(document_collection) which writes all the
        given documents of the form (timestamp,data) from document_collection to the stream stream_id
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
    
    def get_default_ref(self):
        """
        Could be overridden by deriving classes, should return the default values for start,end,modifiers when
        referring to a stream in this channel
        """
        # TODO: Should end be timedelta(0) ?
        return {'start': MIN_DATE, 'end': MAX_DATE, 'modifiers': Identity()}
    
    def __str__(self):
        s = self.__class__.__name__ + ' with ID: ' + str(self.channel_id)
        s += ' and containing {} streams:'.format(len(self.streams))
        for stream_id in self.streams:
            calculated_ranges = repr(self.streams[stream_id].calculated_intervals) \
                if stream_id in self.calculated_intervals else "Error - stream not found"

            s += '\nSTREAM ID: ' + str(stream_id)
            s += "\n  CALCULATED RANGES: " + calculated_ranges
            s += "\n  STREAM DEFINITION: "
            s += str(self.streams[stream_id])
        return s
    
    def __getitem__(self, item):
        return self.streams[item]

    def __setitem__(self, key, value):
        # TODO: is there a better way of setting the channel?
        # from . import ToolChannel
        # if isinstance(value.channel, ToolChannel) and not isinstance(self, ToolChannel):
        #     value.channel = self

        self.streams[key] = value

    def __contains__(self, item):
        # TODO: More elegant way of doing this
        try:
            var = self[item]
            return True
        except KeyError:
            return False
