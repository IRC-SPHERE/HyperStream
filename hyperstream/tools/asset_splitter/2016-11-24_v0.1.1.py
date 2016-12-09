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

from hyperstream.stream import StreamInstance, StreamMetaInstance
from hyperstream.tool import MultiOutputTool


class AssetSplitter(MultiOutputTool):
    def __init__(self, element=None):
        """
        Special tool to extract data from the asset channel
        :param element: The element to extract
        """
        super(AssetSplitter, self).__init__(element=element)

    def _execute(self, source, splitting_stream, interval, output_plate):
        for timestamp, data in source.window(interval, force_calculation=True):
            if self.element is None:
                data_element = data
            else:
                if self.element in data:
                    data_element = data[self.element]
                else:
                    continue
            for key, value in data_element.items():
                yield StreamMetaInstance(StreamInstance(timestamp=timestamp, value=value),
                                         (output_plate.meta_data_id, key))
