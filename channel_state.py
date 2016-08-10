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


class ChannelState(object):
    # TODO needs to be stored permanently as well
    def __init__(self, channel_id):
        self.channel_id = channel_id
        self.name2id = {}
        self.def2id = {}
        self.id2def = {}
        self.id2calc = {}

    def get_name2id(self, name):
        return self.name2id[name]

    def set_name2id(self, name, stream_id):
        self.name2id[name] = stream_id

    def get_def2id(self, stream_def):
        return self.def2id[stream_def]

    def set_def2id(self, stream_def, stream_id):
        self.def2id[stream_def] = stream_id

    def get_id2def(self, stream_id):
        return self.id2def[stream_id]

    def set_id2def(self, stream_id, stream_def):
        self.id2def[stream_id] = stream_def

    def get_id2calc(self, stream_id):
        return self.id2calc[stream_id]

    def set_id2calc(self, stream_id, calc_interval):
        self.id2calc[stream_id] = calc_interval
