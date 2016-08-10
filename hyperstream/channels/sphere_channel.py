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
from stream_channel import StreamChannel
from ..channel_state import ChannelState
from ..modifiers import Identity
from ..time_interval import TimeIntervals
from datetime import datetime, timedelta
from sphere_connector_package.sphere_connector import SphereConnector, DataWindow


class SphereChannel(StreamChannel):
    """
    SPHERE MongoDB storing the raw sensor data
    """

    def get_stream_writer(self, stream_id):
        raise Exception('SphereChannel is read-only, cannot write new streams')

    def create_stream(self, stream_def):
        raise Exception('SphereChannel is read-only, cannot create new streams')

    def __init__(self, base_id, up_to_timestamp=datetime.min):
        state = ChannelState(base_id)
        super(SphereChannel, self).__init__(can_calc=False, can_create=False, state=state)
        self.modalities = ('video', 'environmental')
        for stream_id in self.modalities:
            self.state.set_name2id(stream_id, stream_id)
            self.state.set_id2calc(stream_id, TimeIntervals([(datetime.min, up_to_timestamp)]))
        self.up_to_timestamp = datetime.min
        if up_to_timestamp > datetime.min:
            self.update(up_to_timestamp)

        self.sphere_connector = SphereConnector(config_filename='config_strauss.json', include_mongo=True,
                                                include_redcap=False)

    def repr_stream(self, stream_id):
        return 'read-only SPHERE MongoDB stream'

    def __setitem__(self, key, value):
        raise Exception('SphereChannel is read-only, cannot create new streams')

    def update(self, up_to_timestamp):
        """
        Call this function to report to the system that the SPHERE MongoDB is fully populated until up_to_timestamp
        """
        for stream_id in self.modalities:
            self.state.set_id2calc(stream_id, TimeIntervals([(datetime.min, up_to_timestamp)]))
        self.up_to_timestamp = up_to_timestamp

    def get_results(self, stream_ref, args, kwargs):
        stream_id = stream_ref.stream_id
        start = stream_ref.start
        abs_start = start
        if isinstance(start, timedelta):
            try:
                abs_start = kwargs['start'] + start
            except KeyError:
                raise Exception('The stream reference to a SphereChannel stream has a relative start time, '
                                'need an absolute start time')
        end = stream_ref.end
        abs_end = end
        if isinstance(end, timedelta):
            try:
                abs_end = kwargs['end'] + end
            except KeyError:
                raise Exception(
                    'The stream reference to a SphereChannel stream has a relative end time, need an absolute end time')
        if abs_end > self.up_to_timestamp:
            raise Exception(
                'The stream is not available after ' + str(self.up_to_timestamp) + ' and cannot be obtained')
        window = DataWindow(start=abs_start, end=abs_end, sphere_connector=self.sphere_connector)
        if stream_id not in self.modalities:
            raise Exception('Unknown stream_id: ' + str(stream_id))
        if stream_id == 'video':
            data = window.video.get_data(elements={"2Dbb"})
        elif stream_id == 'environmental':
            data = window.environmental.get_data()
        else:
            raise KeyError('The stream ID must be in the set {' + ','.join(self.modalities) +'}')

        def reformat(doc):
            timestamp = doc['datetime']
            del doc['datetime']
            return timestamp, doc

        # assume that the data are already sorted by time
        result = stream_ref.modifier(
            (reformat(doc) for doc in data))  # make a generator out from result and then apply the modifier
        return result

    def get_default_ref(self):
        return {'start': datetime.min, 'end': self.up_to_timestamp, 'modifier': Identity()}
