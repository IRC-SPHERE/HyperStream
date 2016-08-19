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
from memory_channel import ReadOnlyMemoryChannel
from ..stream import StreamId, StreamReference
from ..modifiers import Identity
from ..time_interval import TimeIntervals, TimeInterval
from ..utils import utcnow, MIN_DATE, MAX_DATE
from sphere_connector_package.sphere_connector import SphereConnector, DataWindow


class SphereChannel(ReadOnlyMemoryChannel):
    """
    SPHERE MongoDB storing the raw sensor data
    """
    def __init__(self, channel_id, up_to_timestamp=None):
        super(SphereChannel, self).__init__(channel_id=channel_id)
        # TODO: Populate this from the Mongo database instead
        self.modalities = ('video', 'environmental')
        for modality in self.modalities:
            if up_to_timestamp is None:
                up_to_timestamp = utcnow()
            intervals = TimeIntervals([(MIN_DATE, up_to_timestamp)])

            # TODO populate meta data here??? When do the streams get split?
            stream_id = StreamId(name=modality, meta_data={})

            self.state.calculated_intervals[stream_id] = intervals

            # TODO: Not sure if the following is correct or necessary!
            self.streams[stream_id] = StreamReference(
                channel_id=channel_id,
                stream_id=stream_id,
                time_interval=TimeInterval(start=MIN_DATE, end=MAX_DATE),
                modifier=Identity(),
                get_results_func=self.get_results
            )

        if up_to_timestamp > MIN_DATE:
            self.update(up_to_timestamp)

        self.sphere_connector = SphereConnector(config_filename='config_strauss.json', include_mongo=True,
                                                include_redcap=False)

    def update_streams(self, up_to_timestamp):
        """
        Call this function to report to the system that the SPHERE MongoDB is fully populated until up_to_timestamp
        """
        for stream_id in self.modalities:
            intervals = TimeIntervals([(MIN_DATE, up_to_timestamp)])
            self.state.calculated_intervals[stream_id] = intervals
        self.up_to_timestamp = up_to_timestamp

    def update_status(self):
        # TODO: seems like this is done in update_streams?
        pass

    def get_results(self, stream_ref, args, kwargs):
        stream_id = stream_ref.stream_id
        abs_end, abs_start = self.get_absolute_start_end(kwargs, stream_ref)
        window = DataWindow(start=abs_start, end=abs_end, sphere_connector=self.sphere_connector)
        
        if stream_id.name not in self.modalities:
            raise KeyError('The stream ID must be in the set {' + ','.join(self.modalities) +'}')
        
        if stream_id.name == 'video':
            data = window.video.get_data(elements={"2Dbb"})
        elif stream_id.name == 'environmental':
            data = window.environmental.get_data()

        def reformat(doc):
            timestamp = doc['datetime']
            del doc['datetime']
            return timestamp, doc

        # assume that the data are already sorted by time
        result = stream_ref.modifier.execute(
            (reformat(doc) for doc in data))  # make a generator out from result and then apply the modifier
        return result

    def get_default_ref(self):
        return {'start': MIN_DATE, 'end': self.up_to_timestamp, 'modifier': Identity()}

    # def __getitem__(self, item):
    #     import ipdb
    #     ipdb.set_trace()
    #     return super(SphereChannel, self).__getitem__(item)
