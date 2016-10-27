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

from ..utils import Hashable


class StreamId(Hashable):
    """
    Helper class for stream identifiers. A stream identifier contains the stream name and any meta-data
    """

    def __init__(self, name, meta_data=None):
        self.name = name
        if meta_data:
            if isinstance(meta_data, dict):
                self.meta_data = tuple(sorted(meta_data.items()))
            elif isinstance(meta_data, tuple):
                self.meta_data = meta_data
            else:
                raise ValueError("Expected dict or tuple, got {}".format(type(meta_data)))
        else:
            self.meta_data = tuple()
            # self.meta_data = meta_data if meta_data else {}

    def __str__(self):
        if self.meta_data:
            # return self.name + ": [" + ", ".join("{}={}".format(k, v) for k, v in self.meta_data.items()) + "]"
            return self.name + ": [" + ", ".join("{}={}".format(k, v) for k, v in self.meta_data) + "]"
        else:
            return self.name

    def __repr__(self):
        return "{}(name={}, meta_data={})".format(
            self.__class__.__name__,
            repr(self.name),
            repr(self.meta_data)
        )

    def __eq__(self, other):
        return isinstance(other, StreamId) and \
               self.name == other.name and \
               sorted(self.meta_data) == sorted(other.meta_data)

    def as_dict(self):
        return dict(name=self.name, meta_data=dict(self.meta_data) if self.meta_data else {})

    def as_raw(self):
        """
        Return a representation of this object that can be used with mongoengine Document.objects(__raw__=x)
        Example:

        >>> stream_id = StreamId(name='test', meta_data=((u'house', u'1'), (u'resident', u'1')))
        >>> stream_id.as_raw()
        {u'stream_id.meta_data.house': u'1', u'stream_id.meta_data.resident': u'1', 'stream_id.name': 'test'}

        :return: The raw representation of this object.
        """
        d = dict(('stream_id.meta_data.' + k, v) for k, v in self.meta_data)
        d['stream_id.name'] = self.name
        return d
