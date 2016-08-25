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
from ..stream import StreamReference, StreamDict, StreamId
from ..tool import Tool
# from ..channel import ChannelState
from ..modifiers import Identity, Modifier
from ..time_interval import TimeIntervals, TimeInterval, RelativeTimeInterval
from datetime import datetime, timedelta, date
from ..utils import Printable, MIN_DATE, MAX_DATE
from ..errors import StreamDataNotAvailableError


class BaseChannel(Printable):
    def __init__(self, channel_id, can_calc=False, can_create=False, calc_agent=None):
        self.channel_id = channel_id
        self.streams = StreamDict()
        self.can_calc = can_calc
        self.can_create = can_create
        # self.state = ChannelState()
        self.calc_agent = calc_agent
        self.up_to_timestamp = MAX_DATE
        self.calculated_intervals = {}

    def get_absolute_start_end(self, stream_ref):
        start = stream_ref.time_interval.start if stream_ref.time_interval else timedelta(0)
        abs_start = start
        if isinstance(start, timedelta):
            if isinstance(stream_ref.time_interval, RelativeTimeInterval):
                raise StreamDataNotAvailableError('The stream reference to a stream has a relative start time, '
                                                  'need an absolute start time')
            abs_start = stream_ref.time_interval.start + start

        end = stream_ref.time_interval.end if stream_ref.time_interval else timedelta(0)
        abs_end = end
        if isinstance(end, timedelta):
            if isinstance(stream_ref.time_interval, RelativeTimeInterval):
                raise StreamDataNotAvailableError('The stream reference to a stream has a relative end time, '
                                                  'need an absolute end time')
            abs_end = stream_ref.time_interval.end + end

        if abs_end > self.up_to_timestamp:
            raise StreamDataNotAvailableError(
                'The stream is not available after ' + str(self.up_to_timestamp) + ' and cannot be obtained' +
                ' (' + str(abs_end) + ' is provided)')
        return TimeInterval(start=abs_start, end=abs_end)

    def update_streams(self, up_to_timestamp):
        """
        Deriving classes must override this function
        """
        raise NotImplementedError

    def get_results(self, stream_ref, **kwargs):  # TODO: force_calc=False):
        """
        Must be overridden by deriving classes.
        1. Calculates/receives the documents in the stream interval determined by the stream_ref
        2. Applies the modifiers within stream_ref
        3. Applies channel custom modifiers as determined by kwargs
        4. Returns success or failure and the results (for some channels the values of kwargs can override the
        return process, e.g. introduce callbacks)
        """
        raise NotImplementedError
    
    def create_stream(self, stream_id, tool):
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
    
    # def str_stream(self, stream_id):
    #     """
    #     Can be over-ridden to provide details about the stream
    #     """
    #     return str(self.streams[stream_id])
    
    # def parse_setkey(self, key):
    #     # TODO: add docstrings to this function. It is not clear why the cases below are structures as they are.
    #
    #     # ( stream_id_part [,stream_id_part]* )
    #     if isinstance(key, (list, tuple)):
    #         if len(key) == 0:
    #             raise ValueError('Empty stream identifier')
    #
    #         if len(key) == 1:
    #             return self.parse_setkey(key[0])
    #
    #         # Create dict holding the indices of each type
    #         type_dict = dict((type(k), i) for i, k in enumerate(key))
    #
    #         stream_id = key[type_dict[StreamId]]
    #
    #         classes = set(map(type, key))
    #         if any([issubclass(cls, Modifier) for cls in classes]):
    #             raise TypeError('StreamReference identifier cannot include a Modifier')
    #
    #         if (timedelta in classes) or (date in classes):
    #             raise TypeError('StreamReference identifier cannot include datetime or timedelta')
    #
    #         if StreamId not in type_dict:
    #             raise TypeError('StreamId not found')
    #
    #         stream_id.meta_data['extra'] = [key[type_dict[t]] for t in type_dict if t is not StreamId]
    #
    #         # return '.'.join(map(str, key))
    #
    #     elif isinstance(key, StreamId):
    #         return key
    #
    #     raise KeyError(repr(key))
    #     # TODO: could do following?
    #     # return StreamID(name=str(key), meta_data={})
    
    # def parse_getkey(self, key):
    #     # TODO: add docstrings to this function. It is not clear why the cases below are structures as they are.
    #
    #     allowed_types = (timedelta, date, datetime)
    #
    #     # ( stream_id_part [,stream_id_part]* [,start | ,start,end] [,modifiers] )
    #     refdict = self.get_default_ref()
    #
    #     if isinstance(key, (tuple, list)):
    #         if (len(key) >= 2) and isinstance(key[-1], Modifier):
    #             refdict['modifiers'] = key[-1]
    #             key = key[:-1]
    #
    #         if (len(key) >= 3) and isinstance(key[-2], allowed_types) and isinstance(key[-1], allowed_types):
    #             refdict['start'] = key[-2]
    #             refdict['end'] = key[-1]
    #             key = key[:-2]
    #
    #         elif (len(key) >= 2) and isinstance(key[-1], allowed_types):
    #             refdict['start'] = key[-1]
    #             key = key[:-1]
    #
    #         refdict['stream_id'] = self.parse_setkey(key)
    #
    #     else:
    #         refdict['stream_id'] = self.parse_setkey(key)
    #
    #     return refdict
    
    # def __getitem__(self, key, modifiers=None):
    #     # return self.streams[self.parse_getkey(key)]
    #     key = self.parse_getkey(key)
    #
    #     # TODO: Callee should use TimeInterval
    #     time_interval = None
    #
    #     if 'start' in key and 'end' in key:
    #         start = key['start']
    #         end = key['end']
    #
    #         if isinstance(start, timedelta) and isinstance(end, timedelta):
    #             time_interval = RelativeTimeInterval(start, end)
    #         elif isinstance(start, (date, datetime)) and isinstance(end, (date, datetime)):
    #             time_interval = TimeInterval(start=start, end=end)
    #         else:
    #             raise ValueError("Invalid combination of start and end types")
    #
    #     return StreamReference(
    #         channel=self,
    #         stream_id=key['stream_id'],
    #         get_results_func=self.get_results,
    #         modifiers=modifiers,
    #         time_interval=time_interval,
    #         calculated_intervals=TimeIntervals()
    #     )

    def __getitem__(self, item):
        return self.streams[item]

    # def __setitem__(self, key, tool):
    #     stream_id = self.parse_setkey(key)
    #
    #     if not isinstance(tool, Tool):
    #         raise TypeError(str(type(tool)))
    #
    #     # TODO: Possibly make the callee responsible for creating the stream
    #     self.create_stream(stream_id, tool)
    #
    #     self.streams[stream_id].calculated_intervals = TimeIntervals()

    def __setitem__(self, key, value):
        # if isinstance(value, Tool):
        # TODO: is there a better way of setting the channel?
        from . import ToolChannel
        if isinstance(value.channel, ToolChannel) and not isinstance(self, ToolChannel):
            value.channel = self

        # if value.tool:
        #     self.create_stream(key, value)
        # else:
        self.streams[key] = value

    def __contains__(self, item):
        # TODO: More elegant way of doing this
        try:
            var = self[item]
            return True
        except KeyError:
            return False
