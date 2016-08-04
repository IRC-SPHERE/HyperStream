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
import os
import simplejson as json
from collections import OrderedDict
import logging
from ..utils import Printable
from ..kernel import Kernel
import imp
from ..interface import StandardInput, StandardOutput, RawInput


INPUT_TYPES = {
    "raw": RawInput,
    "standard": StandardInput
}

OUTPUT_TYPES = {
    "standard": StandardOutput
}


class KernelCollection(Printable):
    kernels = {}

    def __init__(self, kernel_path):
        with open("kernel_ids.json", 'r') as f:
            kernel_ids = json.load(f, object_pairs_hook=OrderedDict)
            for kernel_id in kernel_ids:
                d = kernel_ids[kernel_id]
                try:
                    src = imp.load_source(kernel_id, os.path.join(kernel_path, kernel_id, d["version"], "runner.py"))
                except (IOError, ImportError):
                    logging.error("Not found kernel with appropriate version: " + d["name"])
                    raise

                if d['input_type'] not in INPUT_TYPES:
                    raise NotImplementedError("Unknown input type: " + d['input_type'])
                input_interface = INPUT_TYPES[d['input_type']]().get_data

                if d['output_type'] not in OUTPUT_TYPES:
                    raise NotImplementedError("Unknown output type: " + d['output_type'])
                output_interface = OUTPUT_TYPES[d['output_type']]().put_data

                del d['input_type']
                del d['output_type']

                self.kernels[kernel_id] = Kernel(src.Runner(input_interface, output_interface), kernel_id, **d)
