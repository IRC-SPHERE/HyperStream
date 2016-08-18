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


class Modifier(object):
    def __init__(self):
        """
        Must be overridden, self.types must be a dict with key representing input type and value representing the
        respective output type, each either 'doc_gen' or 'data_gen' or 'doc' or 'data'
        """
        self.types = {}

    def __repr__(self):
        s = self.__class__.__name__ + '('
        p = []
        for param in self.__dict__:
            if param != 'types':
                p.append(param + '=' + repr(self.__dict__[param]))
        s += ','.join(p)
        s += ')'
        return s

    def execute(self, data):
        raise NotImplementedError

    def __add__(self, other):
        return ComposedModifier(self, other)

    def __eq__(self, other):
        # TODO: Could do with some unit tests for this
        return isinstance(other, Modifier) and self.__dict__ == other.__dict__


class ComposedModifier(Modifier):
    def __init__(self, modifier_1, modifier_2):
        super(ComposedModifier, self).__init__()
        self.modifier_1 = modifier_1
        self.modifier_2 = modifier_2
        for input_type in modifier_1.types:
            if modifier_2.types.has_key(modifier_1.types[input_type]):
                self.types[input_type] = modifier_2.types[modifier_1.types[input_type]]
        if len(self.types) == 0:
            raise ValueError('Type mismatch in composing modifiers ' + repr(modifier_1) + ' and ' + repr(modifier_2))

    def __repr__(self):
        return repr(self.modifier_1) + '+' + repr(self.modifier_2)

    def execute(self, data):
        return self.modifier_2.execute(self.modifier_1.execute(data))

    def __eq__(self, other):
        return isinstance(other, ComposedModifier) and \
               self.modifier_1 == other.modifier_1 and \
               self.modifier_2 == other.modifier_2
