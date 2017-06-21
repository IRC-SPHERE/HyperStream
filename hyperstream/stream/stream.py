# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.

from ..time_interval import TimeInterval, TimeIntervals, RelativeTimeInterval, parse_time_tuple
from ..utils import Hashable, utcnow
from . import StreamView, StreamId
from ..models import TimeIntervalModel, StreamDefinitionModel

from collections import Iterable
from mongoengine.context_managers import switch_db
from mongoengine.errors import NotUniqueError, DoesNotExist
import logging


class Stream(Hashable):
    """
    Stream reference class
    """
    def __init__(self, channel, stream_id, calculated_intervals, sandbox):
        """
        :param channel: The channel to which this stream belongs
        :param stream_id: The unique identifier for this string
        :param calculated_intervals: The time intervals in which this has been calculated
        :param sandbox: The sandbox in which this stream lives
        :type channel: BaseChannel
        :type stream_id: StreamId
        :type calculated_intervals: TimeIntervals, None
        :type sandbox: str, unicode, None
        """
        self.channel = channel
        if not isinstance(stream_id, StreamId):
            raise TypeError(type(stream_id))
        self.stream_id = stream_id
        # self.get_results_func = get_results_func
        self._calculated_intervals = None
        if calculated_intervals:
            if not isinstance(calculated_intervals, TimeIntervals):
                raise TypeError(type(calculated_intervals))
            self._calculated_intervals = calculated_intervals
        else:
            self._calculated_intervals = TimeIntervals()
        self.tool_reference = None  # needed to traverse the graph outside of workflows
        self.sandbox = sandbox
        self._node = None  # Back reference to node

        # Here we define the output type. When modifiers are applied, this changes
        # self.output_format = 'doc_gen'

    def set_tool_reference(self, tool_reference):  # needed to traverse the graph outside of workflows
        self.tool_reference = tool_reference

    def __str__(self):
        return "{}(stream_id={}, channel_id={})".format(
            self.__class__.__name__,
            self.stream_id,
            self.channel.channel_id)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return str(self) == str(other)

    @property
    def parent_node(self):
        return self._node

    @parent_node.setter
    def parent_node(self, node):
        self._node = node

    @property
    def calculated_intervals(self):
        """
        Get the calculated intervals
        This will be read from the stream_status collection if it's in the database channel
        :return: The calculated intervals
        """
        return self._calculated_intervals

    @calculated_intervals.setter
    def calculated_intervals(self, value):
        """
        Set the calculated intervals
        This will be written to the stream_status collection if it's in the database channel
        :param value: The calculated intervals
        :type value: TimeIntervals, TimeInterval, list[TimeInterval]
        """
        if not value:
            self._calculated_intervals = TimeIntervals()
            return

        if isinstance(value, TimeInterval):
            value = TimeIntervals([value])
        elif isinstance(value, TimeIntervals):
            pass
        elif isinstance(value, list):
            value = TimeIntervals(value)
        else:
            raise TypeError("Expected list/TimeInterval/TimeIntervals, got {}".format(type(value)))

        for interval in value:
            if interval.end > utcnow():
                raise ValueError("Calculated intervals should not be in the future")

        self._calculated_intervals = value

    @property
    def writer(self):
        return self.channel.get_stream_writer(self)

    def window(self, time_interval=None, force_calculation=False):
        """
        Gets a view on this stream for the time interval given
        :param time_interval: either a TimeInterval object or (start, end) tuple of type str or datetime
        :param force_calculation: Whether we should force calculation for this stream view if data does not exist
        :type time_interval: None | Iterable | TimeInterval
        :type force_calculation: bool
        :return: a stream view object
        """
        if not time_interval:
            if self.calculated_intervals:
                time_interval = self.calculated_intervals[-1]
            else:
                raise ValueError("No calculations have been performed and no time interval was provided")
        elif isinstance(time_interval, TimeInterval):
            time_interval = TimeInterval(time_interval.start, time_interval.end)
        elif isinstance(time_interval, Iterable):
            time_interval = parse_time_tuple(*time_interval)
            if isinstance(time_interval, RelativeTimeInterval):
                raise NotImplementedError
        elif isinstance(time_interval, RelativeTimeInterval):
            raise NotImplementedError
        else:
            raise TypeError("Expected TimeInterval or (start, end) tuple of type str or datetime, got {}"
                            .format(type(time_interval)))
        return StreamView(stream=self, time_interval=time_interval, force_calculation=force_calculation)


class DatabaseStream(Stream):
    """
    Simple subclass that overrides the calculated intervals property
    """
    def __init__(self, channel, stream_id, calculated_intervals, last_accessed, last_updated, sandbox,
                 mongo_model=None):
        super(DatabaseStream, self).__init__(
            channel=channel,
            stream_id=stream_id,
            calculated_intervals=None,  # TODO: probably no point in having the actual calculated intervals here
            sandbox=sandbox)

        if mongo_model:
            self.mongo_model = mongo_model
            self._calculated_intervals = self.mongo_model.get_calculated_intervals()
        else:
            # First try to load it from the database
            try:
                self.load()
            except DoesNotExist:
                self.mongo_model = StreamDefinitionModel(
                    stream_id=self.stream_id.as_dict(),
                    channel_id=self.channel.channel_id,
                    calculated_intervals=calculated_intervals,
                    last_accessed=last_accessed,
                    last_updated=last_updated,
                    sandbox=self.sandbox)
                self.save()

    def load(self):
        """
        Load the stream definition from the database
        :return: None
        """
        with switch_db(StreamDefinitionModel, 'hyperstream'):
            self.mongo_model = StreamDefinitionModel.objects.get(__raw__=self.stream_id.as_raw())
            self._calculated_intervals = self.mongo_model.get_calculated_intervals()

    def save(self):
        """
        Saves the stream definition to the database. This assumes that the definition doesn't already exist, and will
        raise an exception if it does.
        :type upsert: bool
        :return: None
        """
        with switch_db(StreamDefinitionModel, 'hyperstream'):
            self.mongo_model.save()

    @property
    def calculated_intervals(self):
        """
        Gets the calculated intervals from the database
        :return: The calculated intervals
        """
        if self._calculated_intervals is None:
            logging.debug("get calculated intervals")
            self.load()
            return self.mongo_model.get_calculated_intervals()
        return self._calculated_intervals

    @calculated_intervals.setter
    def calculated_intervals(self, intervals):
        """
        Updates the calculated intervals in the database. Performs an upsert
        :param intervals: The calculated intervals
        :return: None
        """
        logging.debug("set calculated intervals")
        self.mongo_model.set_calculated_intervals(intervals)
        self.save()
        self._calculated_intervals = TimeIntervals(intervals)

    @property
    def last_accessed(self):
        """
        Gets the last accessed time from the database
        :return: The last accessed time
        """
        self.load()
        return self.mongo_model.last_accessed

    @last_accessed.setter
    def last_accessed(self, dt):
        """
        Updates the last accessed time in the database. Performs an upsert
        :param dt: The last accessed time
        :return: None
        """
        self.mongo_model.last_accessed = dt

    @property
    def last_updated(self):
        """
        Gets the last updated time from the database
        :return: The last updated time
        """
        self.load()
        return self.mongo_model.last_updated

    @last_updated.setter
    def last_updated(self, dt):
        """
        Updates the last updated time in the database. Performs an upsert
        :param dt: The last updated time
        :return: None
        """
        self.mongo_model.last_updated = dt


class AssetStream(DatabaseStream):
    """
    Simple subclass that overrides the calculated intervals property
    """
    @property
    def calculated_intervals(self):
        return super(AssetStream, self).calculated_intervals

    @calculated_intervals.setter
    def calculated_intervals(self, intervals):
        """
        Updates the calculated intervals in the database. Performs an upsert
        :param intervals: The calculated intervals
        :return: None
        """
        if len(intervals) > 1:
            raise ValueError("Only single calculated interval valid for AssetStream")
        super(AssetStream, self.__class__).calculated_intervals.fset(self, intervals)
