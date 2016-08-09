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


class StreamRef(object):
    def __init__(self, base_id, stream_id, start, end, modifier, get_results_func):
        self.base_id = base_id
        self.stream_id = stream_id
        self.start = start
        self.end = end
        self.modifier = modifier
        self.get_results_func = get_results_func

    def __repr__(self):  ### TODO possibly need repr as well? or even repr instead of str?
        s = "StreamRef\n      BASE_ID  : " + repr(self.base_id)
        s = s + "\n      STREAM_ID: " + repr(self.stream_id)
        s = s + "\n      START    : " + repr(self.start)
        s = s + "\n      END      : " + repr(self.end)
        s = s + "\n      MODIFIER : " + repr(self.modifier)
        s = s + "\n    "
        return (s)

    def __eq__(self, other):
        return (str(self) == str(other))

    def __hash__(self):
        return (hash(str(self)))

    def __call__(self, *args, **kwargs):
        return (self.get_results_func(self, args, kwargs))
