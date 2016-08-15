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
from ..stream import StreamReference
from ..modifiers import Identity, Modifier
from ..time_interval import TimeIntervals
from datetime import datetime, timedelta, date
from ..utils import Printable
import pytz


class BaseChannel(Printable):
    def __init__(self, can_calc=False, can_create=False, state=None, calc_agent=None):
        self.can_calc = can_calc
        self.can_create = can_create
        self.state = state
        self.calc_agent = calc_agent
        self.up_to_timestamp = datetime.min.replace(tzinfo=pytz.utc)

    def get_absolute_start_end(self, kwargs, stream_ref):
        start = stream_ref.start
        abs_start = start
        if isinstance(start, timedelta):
            try:
                abs_start = kwargs['start'] + start
            except KeyError:
                raise Exception('The stream reference to a stream has a relative start time, '
                                'need an absolute start time')
        end = stream_ref.end
        abs_end = end
        if isinstance(end, timedelta):
            try:
                abs_end = kwargs['end'] + end
            except KeyError:
                raise Exception(
                    'The stream reference to a stream has a relative end time, need an absolute end time')
        if abs_end > self.up_to_timestamp:
            raise Exception(
                'The stream is not available after ' + str(self.up_to_timestamp) + ' and cannot be obtained')
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

    def create_stream(self, stream_def):
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
        return {'start': datetime.min.replace(tzinfo=pytz.utc), 'end': timedelta(0), 'modifier': Identity()}

    # @property
    def __repr__(self):
        s = self.__class__.__name__ + ' with ID: ' + str(self.state.channel_id)
        s = s + ' and containing ' + str(len(self.state.stream_id_to_intervals_mapping)) + " streams:"
        for stream_id in self.state.stream_id_to_intervals_mapping:
            s += '\nSTREAM ID: ' + str(stream_id)
            # s += "\n  NAMES: "
            # s += ', '.join(name for name in self.state.name_to_id_mapping if self.state.name_to_id_mapping[name] == stream_id)
            s += "\n  CALCULATED RANGES: " + repr(self.state.stream_id_to_intervals_mapping[stream_id])
            s += "\n  STREAM DEFINITION: "
            s += self.repr_stream(stream_id)
        return s

    def repr_stream(self, stream_id):
        """
        Must be over-ridden to provide details about the stream
        """
        raise NotImplementedError

    def parse_setkey(self, key):
        # ( stream_id_part [,stream_id_part]* )
        if not key:
            raise ValueError('Empty stream identifier')

        if isinstance(key, (list, tuple)):
            s = ""
            for k in key:
                if isinstance(k, Modifier):
                    raise ValueError('StreamReference identifier cannot include a Modifier')
                if isinstance(k, (date, datetime, timedelta)):
                    raise ValueError('StreamReference identifier cannot include datetime or timedelta')
                s += '.' + str(k)
            return s
        else:
            return str(key)

    def parse_getkey(self, key):
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
        key = self.parse_getkey(key)
        key['channel_id'] = self.state.channel_id
        # key['stream_id'] = self.state.name_to_id_mapping[key['stream_id']]
        key['get_results_func'] = self.get_results
        return StreamReference(**key)

    def __setitem__(self, key, value):
        key = self.parse_setkey(key)
        if value in self.state.stream_definition_to_id_mapping:
            stream_id = self.state.stream_definition_to_id_mapping[value]
        else:
            stream_id = self.create_stream(value)
            self.state.stream_id_to_intervals_mapping[stream_id] = TimeIntervals()
            self.state.stream_definition_to_id_mapping[value] = stream_id
            self.state.stream_id_to_definition_mapping[stream_id] = value
        # self.state.name_to_id_mapping[key] = stream_id
        return
