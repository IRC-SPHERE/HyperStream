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

import os
from collections import Iterable
import logging
from datetime import timedelta

from sphere_connector_package.sphere_connector import SphereConnector, DataWindow, Experiment, ExperimentConfig

from hyperstream.channels.memory_channel import MemoryChannel
from hyperstream import TimeIntervals, TimeInterval, StreamInstance
from hyperstream.utils import MIN_DATE, MAX_DATE

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

sphere_connector = None


def get_sphere_connector():
    sphere_connector = SphereConnector(
        config_filename=os.path.join(path, 'config.json'),
        include_mongo=True,
        include_redcap=False,
        sphere_logger=None)
    return sphere_connector


class SphereDataWindow(DataWindow):
    """
    Helper class to use the global sphere_connector object
    """
    def __init__(self, time_interval):
        if isinstance(time_interval, TimeInterval):
            start, end = time_interval.to_tuple()
        elif isinstance(time_interval, Iterable):
            start, end = time_interval
        else:
            raise TypeError
        sphere_connector = get_sphere_connector()
        super(SphereDataWindow, self).__init__(sphere_connector, start, end)


class SphereExperiment(Experiment):
    """
    Helper class to use the global sphere_connector object
    """
    def __init__(self, time_interval, annotators):
        if isinstance(time_interval, TimeInterval):
            start, end = time_interval.to_tuple()
        elif isinstance(time_interval, Iterable):
            start, end = time_interval
        else:
            raise TypeError
        sphere_connector = get_sphere_connector()
        annotations = dict((annotator_id, {'filename': None}) for annotator_id in annotators)
        experiment_config = ExperimentConfig(experiment_start=start, experiment_end=end, annotations=annotations)
        super(SphereExperiment, self).__init__(sphere_connector, experiment_config, auto_initialise=False)


class SphereChannel(MemoryChannel):
    """
    SPHERE MongoDB storing the raw sensor data
    """

    def __init__(self, channel_id, up_to_timestamp=None):
        super(SphereChannel, self).__init__(channel_id=channel_id)
        
        if up_to_timestamp is None:
            # TODO: maybe should be utcnow, but then we'd have to keep updating it?
            up_to_timestamp = MAX_DATE  # utcnow()
        
        if up_to_timestamp > MIN_DATE:
            self.update_streams(up_to_timestamp)
    
    def update_streams(self, up_to_timestamp):
        """
        Call this function to report to the system that the SPHERE MongoDB is fully populated until up_to_timestamp
        """
        for stream_id in self.streams:
            self.streams[stream_id].calculated_intervals = TimeIntervals([(MIN_DATE, up_to_timestamp)])
        self.up_to_timestamp = up_to_timestamp

    def get_stream_writer(self, stream):
        def writer(document_collection):
            if stream.stream_id not in self.data:
                raise RuntimeError("Data slot does not exist for {}, perhaps create_stream was not used?"
                                   .format(stream))
            if isinstance(document_collection, StreamInstance):
                try:
                    self.data[stream.stream_id].append(document_collection)
                except KeyError as e:
                    # Deal with the duplicate error by adding microseconds to the time until we succeed
                    logging.debug(e.message)
                    doc = StreamInstance(
                        timestamp=document_collection.timestamp + timedelta(microseconds=1),
                        value=document_collection.value)
                    return writer(doc)
            elif isinstance(document_collection, list):
                for d in document_collection:
                    writer(d)
            else:
                raise TypeError('Expected: [StreamInstance, list<StreamInstance>], got {}. '
                                .format(type(document_collection)))

        return writer
