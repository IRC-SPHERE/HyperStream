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
from stream_base import StreamBase


class MemoryBase(StreamBase):
    def __init__(self, base_id):
        state = BaseState(base_id)
        super(MemoryBase, self).__init__(can_calc=True, can_create=True, state=state)
        self.streams = {}
        self.max_stream_id = 0

    def repr_stream(self, stream_id):
        s = repr(self.state.id2def[stream_id])
        return (s)

    def create_stream(self, stream_def):
        '''Must be overridden by deriving classes, must create the stream according to stream_def and return its unique identifier stream_id'''
        self.max_stream_id = self.max_stream_id + 1
        stream_id = self.max_stream_id
        self.streams[stream_id] = []
        return (stream_id)

    def get_params(self, x, start, end):
        if x.__class__ in (list, tuple):
            res = []
            for x_i in x:
                res.append(self.get_params(x_i, start, end))
            if x.__class__ == list:
                return (res)
            else:
                return (tuple(res))
        elif x.__class__ == dict:
            res = {}
            for x_i in x:
                res[x_i] = self.get_params(x[x_i], start, end)
            return (res)
        elif x.__class__ == StreamRef:
            return (x(start=start, end=end))
        else:
            return (x)

    def get_results(self, stream_ref, args, kwargs):
        stream_id = stream_ref.stream_id
        start = stream_ref.start
        abs_start = start
        if type(start) == delta:
            try:
                abs_start = kwargs['start'] + start
            except KeyError:
                raise Exception(
                    'The stream reference to be calculated has a relative start time, need an absolute start time')
        end = stream_ref.end
        abs_end = end
        if type(end) == delta:
            try:
                abs_end = kwargs['end'] + end
            except KeyError:
                raise Exception(
                    'The stream reference to be calculated has a relative end time, need an absolute end time')
        done_calc_times = self.state.get_id2calc(stream_id)
        need_to_calc_times = TimeIntervals([(abs_start, abs_end)]) - done_calc_times
        if str(need_to_calc_times) != '':
            stream_def = self.state.get_id2def(stream_id)
            writer = self.get_stream_writer(stream_id)
            tool = stream_def.tool
            for (start2, end2) in need_to_calc_times.value:
                args2 = self.get_params(stream_def.args, start2, end2)
                kwargs2 = self.get_params(stream_def.kwargs, start2, end2)
                tool(stream_def, start2, end2, writer, *args2, **kwargs2)
                self.state.set_id2calc(stream_id, self.state.get_id2calc(stream_id) + TimeIntervals([(start2, end2)]))
            done_calc_times = self.state.get_id2calc(stream_id)
            need_to_calc_times = TimeIntervals([(abs_start, abs_end)]) - done_calc_times
            print(done_calc_times)
            print(need_to_calc_times)
            assert str(need_to_calc_times) == ''
        result = []
        for (timestamp, data) in self.streams[stream_ref.stream_id]:
            if abs_start < timestamp <= abs_end:
                result.append((timestamp, data))
        result.sort(key=lambda x: x[0])
        result = stream_ref.modifier(
            (x for x in result))  # make a generator out from result and then apply the modifier
        return (result)

    def get_stream_writer(self, stream_id):
        def writer(document_collection):
            self.streams[stream_id].extend(document_collection)

        return (writer)

    def get_default_ref(self):
        return ({'start': delta(0), 'end': delta(0), 'modifier': Identity()})
