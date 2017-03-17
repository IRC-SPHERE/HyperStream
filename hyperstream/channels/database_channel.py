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
from mongoengine import NotUniqueError, InvalidDocumentError
from mongoengine.context_managers import switch_db
from pymongo.errors import InvalidDocument
import logging

from base_channel import BaseChannel
from hyperstream.utils.errors import StreamAlreadyExistsError, StreamNotFoundError
from ..time_interval import TimeIntervals
from ..models import StreamInstanceModel
from ..stream import StreamInstance, DatabaseStream
from ..utils import utcnow


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

    def get_results(self, stream, time_interval):
        """
        Get the results for a given stream
        :param time_interval: The time interval
        :param stream: The stream object
        :return: A generator over stream instances
        """
        query = stream.stream_id.as_raw()
        query['datetime'] = {'$gt': time_interval.start, '$lte': time_interval.end}
        with switch_db(StreamInstanceModel, 'hyperstream'):
            for instance in StreamInstanceModel.objects(__raw__=query):
                yield StreamInstance(timestamp=instance.datetime, value=instance.value)

    def create_stream(self, stream_id, sandbox=None):
        """
        Create the stream
        :param stream_id: The stream identifier
        :param sandbox: The sandbox for this stream
        :return: None
        :raises: NotImplementedError
        """
        if sandbox is not None:
            raise NotImplementedError

        if stream_id in self.streams:
            raise StreamAlreadyExistsError("Stream with id '{}' already exists".format(stream_id))

        stream = DatabaseStream(channel=self, stream_id=stream_id, calculated_intervals=None,
                                last_accessed=utcnow(), last_updated=utcnow(), sandbox=sandbox)
        self.streams[stream_id] = stream
        return stream

    def purge_stream(self, stream_id, sandbox=None):
        """
        Purge the stream
        :param stream_id: The stream identifier
        :param sandbox: The sandbox for this stream
        :return: None
        :raises: NotImplementedError
        """
        # TODO: Add time interval to this

        if sandbox is not None:
            raise NotImplementedError

        if stream_id not in self.streams:
            raise StreamNotFoundError("Stream with id '{}' not found".format(stream_id))

        stream = self.streams[stream_id]
        query = stream_id.as_raw()
        with switch_db(StreamInstanceModel, 'hyperstream'):
            StreamInstanceModel.objects(__raw__=query).delete()

        # Also update the stream status
        stream.calculated_intervals = TimeIntervals([])
        logging.info("Purged stream {}".format(stream_id))

    def get_stream_writer(self, stream):
        """
        Gets the database channel writer
        The mongoengine model checks whether a stream_id/datetime pair already exists in the DB (unique pairs)
        Should be overridden by users' personal channels - allows for non-mongo outputs.
        :param stream: The stream
        :return: The stream writer function
        """

        def writer(document_collection):
            with switch_db(StreamInstanceModel, 'hyperstream'):
                if isinstance(document_collection, StreamInstance):
                    document_collection = [document_collection]

                for t, doc in document_collection:
                    instance = StreamInstanceModel(
                        stream_id=stream.stream_id.as_dict(),
                        datetime=t,
                        value=doc)
                    try:
                        instance.save()
                    except NotUniqueError as e:
                        # Implies that this has already been written to the database
                        # Raise an error if the value differs from that in the database
                        logging.warn("Found duplicate document: {}".format(e.message))
                        existing = StreamInstanceModel.objects(stream_id=stream.stream_id.as_dict(), datetime=t)[0]
                        if existing.value != doc:
                            raise e
                    except (InvalidDocumentError, InvalidDocument) as e:
                        # Something wrong with the document - log the error
                        logging.error(e)
        return writer
