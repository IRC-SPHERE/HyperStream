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

from hyperstream.stream import StreamMetaInstance  # StreamInstance
from hyperstream.tool import NodeCreationTool, check_input_stream_count
# from copy import deepcopy


class MetaInstanceFromTimestamp(NodeCreationTool):
    def __init__(self, func):
        """
        Meta instance output tool.
        """
        super(MetaInstanceFromTimestamp, self).__init__(func=func)
        self.func = func

    def message(self, interval):
        return '{} running from {} to {} with stride {}'.format(
            self.__class__.__name__, str(interval.start), str(interval.end), str(self.stride))

    @check_input_stream_count(0)
    def _execute(self, sources, alignment_stream, interval):
        if alignment_stream is not None:
            raise NotImplementedError

        for instance in sources[0].window(interval):
            yield StreamMetaInstance(instance, self.func(instance))

        # At some point, do a version that looks for the meta data inside the instances too, like so:
        # for timestamp, value in sources[0].window(interval):
            # v = deepcopy(value)
            # if self.key not in v:
            #     continue
            # meta = (self.key, v.pop(self.key))
            # yield StreamMetaInstance(StreamInstance(timestamp, v), meta)
