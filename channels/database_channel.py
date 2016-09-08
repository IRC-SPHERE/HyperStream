# The MIT License (MIT)
#  Copyright (c) 2014-2017 University of Bristol
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#  # The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
#  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
#  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
#  OR OTHER DEALINGS IN THE SOFTWARE.
"""
Database channel module.
"""
from base_channel import BaseChannel
from ..models import StreamInstanceModel, StreamIdField
from ..time_interval import TimeIntervals
from ..utils import MIN_DATE, utcnow
from ..stream import StreamInstance

import logging
from mongoengine import NotUniqueError


class DatabaseChannel(BaseChannel):
    """
    Database Channel. Data stored and retrieved in mongodb using mongoengine.
    """
    def __init__(self, channel_id):
        """
        Initialise this channel
        :param channel_id: The channel identifier
        """
        super(DatabaseChannel, self).__init__(channel_id=channel_id, can_calc=True, can_create=False)
        # self.update_streams(utcnow())

    def update_streams(self, up_to_timestamp):
        """
        Update the streams
        :param up_to_timestamp:
        :return:
        """
        raise NotImplementedError
        # intervals = TimeIntervals([(MIN_DATE, up_to_timestamp)])
        # for stream in self.streams:
        #     stream.calculated_intervals = intervals
        # self.up_to_timestamp = up_to_timestamp

    def get_results(self, stream):
        """
        Get the results for a given stream
        :param stream: The stream object
        :return: A generator over stream instances
        """
        # TODO: Need to check if the timestamp is in range too!
        raise Exception("Need to check if the timestamp is in range")
        for instance in StreamInstanceModel.objects(stream_id=stream.stream_id.as_dict()):
            yield StreamInstance(timestamp=instance.datetime, value=instance.value)

    def create_stream(self, stream_id):
        """
        Create the stream
        :param stream_id: The stream identifier
        :return: None
        :raises: NotImplementedError
        """
        raise NotImplementedError("Database streams currently need to be defined in the database")

    def get_stream_writer(self, stream):
        """
        Gets the database channel writer
        The mongoengine model checks whether a stream_id/datetime pair already exists in the DB (unique pairs)
        :param stream: The stream
        :return: The stream writer function
        """

        def writer(document_collection):
            # TODO: Presumably this should be overridden by users' personal channels - allows for non-mongo outputs.
            for t, doc in document_collection:
                instance = StreamInstanceModel(
                    stream_id=stream.stream_id.as_dict(),
                    datetime=t,
                    value=doc)
                try:
                    instance.save()
                except NotUniqueError as e:
                    # TODO: Fix this ... computed intervals not being stored!
                    pass
                    # raise e
                    # logging.warn(e)
        return writer
