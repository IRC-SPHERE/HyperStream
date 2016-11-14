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
from plugins.sphere.channels.sphere_channel import SphereDataWindow, SphereExperiment


def reformat(doc):
    dt = doc['datetime']
    del doc['datetime']
    if 'house_id' in doc:
        house_id = doc.pop('house_id')
    else:
        house_id = '1'
    return StreamMetaInstance(stream_instance=StreamInstance(dt, doc), meta_data=('house', house_id))


class Sphere(MultiOutputTool):
    def __init__(self, modality, elements=None, filters=None, rename_keys=False, annotators=None):
        super(Sphere, self).__init__(modality=modality, elements=elements, filters=filters, rename_keys=rename_keys,
                                     annotators=annotators)
        self.modality = modality
        self.elements = elements
        self.filters = filters
        self.rename_keys = rename_keys
        self.annotators = annotators
    
    def _execute(self, source, splitting_stream, interval, output_plate):
        if source is not None:
            raise ValueError("Sphere tool does not expect an input source")
        window = SphereExperiment(interval, self.annotators) if self.annotators else SphereDataWindow(interval)
        source = window.modalities[self.modality]
        return map(reformat, source.get_data(self.elements, self.filters, self.rename_keys))
