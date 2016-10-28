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

from hyperstream.tool import Tool, check_input_stream_count
from hyperstream.stream import StreamInstance

from plugins.sphere.channels.sphere_channel import SphereDataWindow


def reformat(doc):
    dt = doc['datetime']
    del doc['datetime']
    
    return StreamInstance(dt, doc)


class Sphere(Tool):
    def __init__(self, modality, elements=None, filters=None, rename_keys=False):
        super(Sphere, self).__init__(modality=modality, elements=elements, filters=filters, rename_keys=rename_keys)
        self.modality = modality
        self.elements = elements
        self.filters = filters
        self.rename_keys = rename_keys
    
    @check_input_stream_count(0)
    def _execute(self, sources, alignment_stream, interval):
        window = SphereDataWindow(interval)
        source = window.modalities[self.modality]
        yield map(reformat, source.get_data(self.elements, self.filters, self.rename_keys))
