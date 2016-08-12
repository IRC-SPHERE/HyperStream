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
from base_channel import BaseChannel
from ..channel_state import ChannelState
from ..models import StreamInstanceModel, StreamDefinitionModel, StreamStatusModel
from ..stream import StreamReference
from datetime import datetime
from ..time_interval import TimeIntervals
import pytz
import logging


class DatabaseChannel(BaseChannel):
    def __init__(self, channel_id):
        state = ChannelState(channel_id)
        super(DatabaseChannel, self).__init__(can_calc=True, can_create=False, state=state)
        self.streams = {}
        self.update()

    def repr_stream(self, stream_id):
        s = repr(self.state.id2def[stream_id])
        return s

    def update(self):
        self.update_streams()
        self.update_state()

    def update_state(self):
        intervals = TimeIntervals([(datetime.min.replace(tzinfo=pytz.utc), datetime.utcnow().replace(tzinfo=pytz.utc))])
        for stream_id in self.streams.keys():
            self.state.name_to_id_mapping[stream_id] = stream_id
            self.state.stream_id_to_intervals_mapping[stream_id] = intervals

    def update_streams(self):
        """
        Call this function to load streams in from the database
        """
        # TODO What about start/end/modifier??? are they needed
        for s in StreamDefinitionModel.objects():
            self.streams[s.stream_id] = StreamReference(
                channel_id=self.state.channel_id,
                stream_id=s.stream_id,
                start=None,
                end=None,
                modifier=None,
                get_results_func=self.get_results
            )

    def get_params(self, x, start, end):
        # TODO: This was copied from MemoryChannel
        if isinstance(x, (list, tuple)):
            res = []
            for x_i in x:
                res.append(self.get_params(x_i, start, end))
            if isinstance(x, list):
                return res
            else:
                return tuple(res)
        elif isinstance(x, dict):
            res = {}
            for x_i in x:
                res[x_i] = self.get_params(x[x_i], start, end)
            return res
        elif isinstance(x, StreamReference):
            return x(start=start, end=end)
        else:
            return x

    def get_results(self, stream_ref, args, kwargs):
        stream_id = stream_ref.stream_id
        abs_end, abs_start = self.get_absolute_start_end(kwargs, stream_ref)

        # TODO: this should be read from the stream_status collection
        done_calc_times = self.state.stream_id_to_intervals_mapping[stream_id]
        need_to_calc_times = TimeIntervals([(abs_start, abs_end)]) - done_calc_times

        # TODO: why string comparison?
        if str(need_to_calc_times) != '':
            stream_def = self.state.stream_id_to_definition_mapping[stream_id]
            writer = self.get_stream_writer(stream_id)
            tool = stream_def.tool
            for (start2, end2) in need_to_calc_times.value:
                args2 = self.get_params(stream_def.args, start2, end2)
                kwargs2 = self.get_params(stream_def.kwargs, start2, end2)

                # Here we're actually executing the tool
                tool(stream_def, start2, end2, writer, *args2, **kwargs2)

                # TODO: write to stream_status collection
                self.state.stream_id_to_intervals_mapping[stream_id] += TimeIntervals([(start2, end2)])

            done_calc_times = self.state.stream_id_to_intervals_mapping[stream_id]
            need_to_calc_times = TimeIntervals([(abs_start, abs_end)]) - done_calc_times
            logging.debug(done_calc_times)
            logging.debug(need_to_calc_times)

            # TODO: why string comparison?
            assert str(need_to_calc_times) == ''

        result = []
        for (timestamp, data) in self.streams[stream_ref.stream_id]:
            if abs_start < timestamp <= abs_end:
                result.append((timestamp, data))

        result.sort(key=lambda x: x[0])

        # make a generator out from result and then apply the modifier
        result = stream_ref.modifier(iter(result))  # (x for x in result))
        return result

    def create_stream(self, stream_def):
        # TODO: Functionality here
        raise RuntimeError("Database streams currently need to be defined in the database")

    def get_stream_writer(self, stream_id):
        def writer(document_collection):
            for doc in document_collection:
                instance = StreamInstanceModel(
                    stream_id=stream_id,
                    stream_type="",
                    datetime=datetime.utcnow().replace(tzinfo=pytz.utc),
                    metadata={},
                    version="",
                    value=doc
                )
                instance.save()
        return writer
