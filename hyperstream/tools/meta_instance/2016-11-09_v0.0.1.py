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
from hyperstream.tool import PlateCreationTool
# from copy import deepcopy


class MetaInstance(PlateCreationTool):
    def __init__(self, func):
        """
        Meta instance output tool.
        """
        super(MetaInstance, self).__init__(func=func)
        self.func = func

    def _execute(self, source, interval):
        for instance in source.window(interval, force_calculation=True):
            yield StreamMetaInstance(instance, self.func(instance))

        # At some point, do a version that looks for the meta data inside the instances too, like so:
        # for timestamp, value in sources[0].window(interval, force_calculation=True):
            # v = deepcopy(value)
            # if self.key not in v:
            #     continue
            # meta = (self.key, v.pop(self.key))
            # yield StreamMetaInstance(StreamInstance(timestamp, v), meta)
