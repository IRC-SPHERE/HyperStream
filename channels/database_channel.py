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
from ..models import StreamInstanceModel


class DatabaseChannel(BaseChannel):
    def __init__(self, channel_id):
        state = ChannelState(channel_id)
        super(DatabaseChannel, self).__init__(can_calc=True, can_create=False, state=state)
        self.streams = {}
        # self.max_stream_id = 0

    def repr_stream(self, stream_id):
        pass

    def get_results(self, stream_ref, args, kwargs):
        pass

    def create_stream(self, stream_def):
        # TODO: Functionality here
        raise RuntimeError("Database streams currently need to be defined in the database")

    def get_stream_writer(self, stream_id):
        def writer(document_collection):
            for doc in document_collection:
                instance = StreamInstanceModel(
                    stream_id=stream_id,
                    stream_type=None,
                    datetime=datetime.utcnow().replace(),
                    metadata= DictField(required=False)
                    value=doc
                )
