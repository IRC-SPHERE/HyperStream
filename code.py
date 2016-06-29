"""
The MIT License (MIT)
Copyright (c) 2014-2015 University of Bristol

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
import logging
from sphere_connector_package.sphere_connector.utils import Printable


class CodeCollection(Printable):
    codes = {}

    def __init__(self, path):
        with open(os.path.join(path, "codeIds.json"), 'r') as f:
            code_ids = json.load(f)
            for cid in code_ids:
                # Load the code details
                d = code_ids[cid]
                with open(os.path.join(path, d["code"] + ".json")) as s:
                    stream_versions = json.load(s)
                    found = False
                    for v in stream_versions:
                        if v == d["version"]:
                            d.update(**stream_versions[v])
                            found = True
                    if not found:
                        logging.error("Not found stream with appropriate version: " + cid["code"])
                    else:
                        self.codes[cid] = Code(**d)


class Code(Printable):
    def __init__(self, code, version, name, description, stype, parameters, modality=None, releaseNotes=None):
        self.codeId = code
        self.version = version
        self.stype = stype
        self.parameters = parameters
        self.name = name
        self.modality = modality
        self.description = description
        self.releaseNotes = releaseNotes
        self.sources = []

    def execute(self):
        logging.info("Executing code: " + self.codeId)

    def __repr__(self):
        return str(self)
