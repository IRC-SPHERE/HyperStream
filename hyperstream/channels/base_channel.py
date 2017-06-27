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

from ..stream import StreamDict
from ..time_interval import TimeIntervals
from ..utils import Printable, MAX_DATE, StreamNotFoundError, MultipleStreamsFoundError

import logging


class BaseChannel(Printable):
    """
    Abstract base class for channels
    """
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
                stream.tool.execute(stream.input_streams, stream, interval)
                stream.calculated_intervals += interval

            if not stream.required_intervals.is_empty:
                raise RuntimeError('Tool execution did not cover the specified time interval.')

    def get_results(self, stream, time_interval):
        """
        Must be overridden by deriving classes.
        1. Calculates/receives the documents in the stream for the time interval given
        2. Returns success or failure and the results (for some channels the values of kwargs can override the
        return process, e.g. introduce callbacks)
        """
        raise NotImplementedError

    def get_or_create_stream(self, stream_id, try_create=True):
        """
        Helper function to get a stream or create one if it's not already defined
        :param stream_id: The stream id
        :param try_create: Whether to try to create the stream if not found
        :return: The stream object
        """
        if stream_id in self.streams:
            logging.debug("found {}".format(stream_id))
            return self.streams[stream_id]
        elif try_create:
            # Try to create the stream
            logging.debug("creating {}".format(stream_id))
            return self.create_stream(stream_id=stream_id)

    def create_stream(self, stream_id, sandbox=None):
        """
        Must be overridden by deriving classes, must create the stream according to the tool and return its unique
        identifier stream_id
        """
        raise NotImplementedError

    def find_streams(self, **kwargs):
        """
        Finds streams with the given meta data values. Useful for debugging purposes.
        :param kwargs: The meta data as keyword arguments
        :return: The streams found
        """
        found = {}

        if 'name' in kwargs:
            name = kwargs.pop('name')
        else:
            name = None

        for stream_id, stream in self.streams.items():
            if name is not None and stream_id.name != name:
                continue

            d = dict(stream_id.meta_data)
            if all(k in d and d[k] == str(v) for k, v in kwargs.items()):
                found[stream_id] = stream
        return found

    def find_stream(self, **kwargs):
        """
        Finds a single stream with the given meta data values. Useful for debugging purposes.
        :param kwargs: The meta data as keyword arguments
        :return: The stream found
        """
        found = list(self.find_streams(**kwargs).values())
        if not found:
            raise StreamNotFoundError(kwargs)
        if len(found) > 1:
            raise MultipleStreamsFoundError(kwargs)
        return found[0]

    def purge_node(self, node_id, sandbox=None):
        """
        Purges a node (collection of streams)
        :param node_id: The node identifier
        :param sandbox: The sandbox
        :return: None
        """
        for stream_id in self.streams:
            stream = self.streams[stream_id]
            if not stream.parent_node:
                # This can happen if streams have been defined outside of nodes - generally nothing to worry about
                logging.debug("cannot purge the stream with id {} because it has no parent node".format(stream_id))
                continue
            if stream.parent_node.node_id == node_id:
                self.purge_stream(stream_id, sandbox=sandbox)

    def purge_stream(self, stream_id, sandbox=None):
        """
        Must be overridden by deriving classes, purges the stream and removes the calculated intervals
        """
        raise NotImplementedError

    def get_stream_writer(self, stream):
        """
        Must be overridden by deriving classes, must return a function(document_collection) which writes all the
        given documents of the form (timestamp,data) from document_collection to the stream
        Example::

        .. code-block:: python

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
