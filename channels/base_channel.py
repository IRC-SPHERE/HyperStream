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
from ..channel_state import ChannelState
from ..modifiers import Identity, Modifier
from ..time_interval import TimeIntervals, TimeInterval
from datetime import datetime, timedelta, date
from ..utils import Printable
import pytz


class BaseChannel(Printable):
    def __init__(self, channel_id, can_calc=False, can_create=False, calc_agent=None):
        self.streams = StreamDict()
        self.can_calc = can_calc
        self.can_create = can_create
        self.state = ChannelState(channel_id)
        self.calc_agent = calc_agent
        self.up_to_timestamp = datetime.max.replace(tzinfo=pytz.utc)
    
    def get_absolute_start_end(self, kwargs, stream_ref):
        start = stream_ref.time_interval.start if stream_ref.time_interval else timedelta(0)
        abs_start = start
        if isinstance(start, timedelta):
            try:
                abs_start = kwargs['time_interval'].start + start
            except KeyError:
                raise Exception('The stream reference to a stream has a relative start time, '
                                'need an absolute start time')
        end = stream_ref.time_interval.end if stream_ref.time_interval else timedelta(0)
        abs_end = end
        if isinstance(end, timedelta):
            try:
                abs_end = kwargs['time_interval'].end + end
            except KeyError:
                raise Exception(
                    'The stream reference to a stream has a relative end time, need an absolute end time')
        if abs_end > self.up_to_timestamp:
            raise Exception(
                'The stream is not available after ' + str(self.up_to_timestamp) + ' and cannot be obtained' +
                ' (' + str(abs_end) + ' is provided)')
        return abs_end, abs_start
    
    def get_results(self, stream_ref, args, kwargs):  # TODO: force_calc=False):
        """
        Must be overridden by deriving classes.
        1. Calculates/receives the documents in the stream interval determined by the stream_ref
        2. Applies the modifiers within stream_ref
        3. Applies channel custom modifiers as determined by args and kwargs
        4. Returns success or failure and the results (for some channels the values of args and kwargs can override the
        return process, e.g. introduce callbacks)
        """
        raise NotImplementedError
    
    def create_stream(self, stream_id, stream_def):
        """
        Must be overridden by deriving classes, must create the stream according to stream_def and return its unique
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
        Could be overridden by deriving classes, should return the default values for start,end,modifier when
        referring to a stream in this channel
        """
        # TODO: Should end be timedelta(0) ?
        return {'start': datetime.min.replace(tzinfo=pytz.utc), 'end': datetime.max.replace(tzinfo=pytz.utc),
                'modifier': Identity()}
    
    # @property
    def __repr__(self):
        s = self.__class__.__name__ + ' with ID: ' + str(self.state.channel_id)
        s += ' and containing {} streams:'.format(len(self.streams))
        for stream_id in self.streams:
            calculated_ranges = repr(self.state.calculated_intervals[stream_id]) \
                if stream_id in self.state.calculated_intervals else "Error - stream not found"

            s += '\nSTREAM ID: ' + str(stream_id)
            s += "\n  CALCULATED RANGES: " + calculated_ranges
            s += "\n  STREAM DEFINITION: "
            s += self.repr_stream(stream_id)
        return s
    
    def repr_stream(self, stream_id):
        """
        Can be over-ridden to provide details about the stream
        """
        return repr(self.streams[stream_id])
    
    def parse_setkey(self, key):
        # TODO: add docstrings to this function. It is not clear why the cases below are structures as they are.
        
        # ( stream_id_part [,stream_id_part]* )
        if isinstance(key, (list, tuple)):
            if len(key) == 0:
                raise ValueError('Empty stream identifier')

            if len(key) == 1:
                return self.parse_setkey(key[0])

            # Create dict holding the indices of each type
            type_dict = dict((type(k), i) for i, k in enumerate(key))

            stream_id = key[type_dict[StreamId]]

            classes = set(map(type, key))
            if any([issubclass(cls, Modifier) for cls in classes]):
                raise TypeError('StreamReference identifier cannot include a Modifier')
            
            if (timedelta in classes) or (date in classes):
                raise TypeError('StreamReference identifier cannot include datetime or timedelta')

            if StreamId not in type_dict:
                raise TypeError('StreamId not found')

            stream_id.meta_data['extra'] = [key[type_dict[t]] for t in type_dict if t is not StreamId]

            # return '.'.join(map(str, key))

        elif isinstance(key, StreamId):
            return key

        raise TypeError(str(key))
        # TODO: could do following?
        # return StreamID(name=str(key), meta_data={})
    
    def parse_getkey(self, key):
        # TODO: add docstrings to this function. It is not clear why the cases below are structures as they are.
        
        allowed_types = (timedelta, date, datetime)
        
        # ( stream_id_part [,stream_id_part]* [,start | ,start,end] [,modifier] )
        refdict = self.get_default_ref()
        if isinstance(key, (tuple, list)):
            if (len(key) >= 2) and isinstance(key[-1], Modifier):
                refdict['modifier'] = key[-1]
                key = key[:-1]
            
            if (len(key) >= 3) and isinstance(key[-2], allowed_types) and isinstance(key[-1], allowed_types):
                refdict['start'] = key[-2]
                refdict['end'] = key[-1]
                key = key[:-2]
            
            elif (len(key) >= 2) and isinstance(key[-1], allowed_types):
                refdict['start'] = key[-1]
                key = key[:-1]
            
            refdict['stream_id'] = self.parse_setkey(key)
        
        else:
            refdict['stream_id'] = self.parse_setkey(key)
        
        return refdict
    
    def __getitem__(self, key):
        # return self.streams[self.parse_getkey(key)]
        key = self.parse_getkey(key)
        
        key['channel_id'] = self.state.channel_id
        key['get_results_func'] = self.get_results

        # TODO: Callee should use TimeInterval
        if key['start'] and key['end']:
            key['time_interval'] = TimeInterval(start=key['start'], end=key['end'])
        else:
            key['time_interval'] = None
        del(key['start'])
        del(key['end'])

        return StreamReference(**key)
    
    def __setitem__(self, key, value):
        stream_id = self.parse_setkey(key)

        if value not in self.state.stream_definition_to_id_mapping:
            self.create_stream(stream_id, value)
            
            self.state.calculated_intervals[stream_id] = TimeIntervals()
            self.state.stream_definition_to_id_mapping[value] = stream_id
            self.state.stream_id_to_definition_mapping[stream_id] = value
