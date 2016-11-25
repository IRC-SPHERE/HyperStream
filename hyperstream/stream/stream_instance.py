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

from datetime import datetime
from collections import namedtuple
import logging

from ..utils import utcnow


class StreamInstance(namedtuple("StreamInstance", "timestamp value")):
    """
    Simple helper class for storing data instances that's a bit neater than simple tuples
    """

    def __new__(cls, timestamp, value):
        if not isinstance(timestamp, datetime):
            raise ValueError("Timestamp must be datetime.datetime")

        if timestamp > utcnow():
            raise ValueError("Timestamp {} should not be in the future!".format(timestamp))

        return super(StreamInstance, cls).__new__(cls, timestamp, value)

    def as_list(self, flat=True):
        if flat:
            l = self.value.items()
            l.append(["timestamp", self.timestamp])
            return l
        else:
            return [self.timestamp, self.value]

    def as_dict(self, flat=True):
        if flat:
            d = self.value.copy()
            d['timestamp'] = self.timestamp
            return d
        else:
            return dict(timestamp=self.timestamp, value=self.value)


class StreamMetaInstance(namedtuple("StreamMetaInstance", "stream_instance meta_data")):
    """
    StreamInstance that also contains meta data
    """

    def __new__(cls, stream_instance, meta_data):
        if isinstance(stream_instance, (list, tuple)):
            stream_instance = StreamInstance(*stream_instance)
        if not isinstance(stream_instance, StreamInstance):
            raise ValueError("Not a stream instance object")
        return super(StreamMetaInstance, cls).__new__(cls, stream_instance, meta_data)
