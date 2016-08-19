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
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
import pprint
import pytz
from datetime import datetime
from treelib.tree import Tree, NodePropertyAbsentError, NodeIDAbsentError

try:
    from StringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO


class MetaDataTree(Tree):
    (ROOT, DEPTH, WIDTH, ZIGZAG) = list(range(4))

    def __str__(self):
        self.reader = ""

        def write(line):
            self.reader += line.decode('utf-8') + "\n"

        self._Tree__print_backend(func=write, idhidden=False)
        return self.reader

    def _Tree__print_backend(self, nid=None, level=ROOT, idhidden=True, queue_filter=None,
                             key=None, reverse=False, line_type='ascii-ex',
                             data_property=None, func=print, iflast=[], ):
        """
        Another implementation of printing tree using Stack
        Print tree structure in hierarchy style.
        For example:
            Root
            |___ C01
            |    |___ C11
            |         |___ C111
            |         |___ C112
            |___ C02
            |___ C03
            |    |___ C31
        A more elegant way to achieve this function using Stack
        structure, for constructing the Nodes Stack push and pop nodes
        with additional level info.
        UPDATE: the @key @reverse is present to sort node at each
        level.
        """
        line_types = \
            {'ascii': ('|', '|-- ', '+-- '),
             'ascii-ex': ('\u2502', '\u251c\u2500\u2500 ', '\u2514\u2500\u2500 '),
             'ascii-exr': ('\u2502', '\u251c\u2500\u2500 ', '\u2570\u2500\u2500 '),
             'ascii-em': ('\u2551', '\u2560\u2550\u2550 ', '\u255a\u2550\u2550 '),
             'ascii-emv': ('\u2551', '\u255f\u2500\u2500 ', '\u2559\u2500\u2500 '),
             'ascii-emh': ('\u2502', '\u255e\u2550\u2550 ', '\u2558\u2550\u2550 ')}
        DT_VLINE, DT_LINE_BOX, DT_LINE_COR = line_types[line_type]

        nid = self.root if (nid is None) else nid
        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)

        if data_property is not None and hasattr(self[nid].data, data_property):
            display_value = getattr(self[nid].data, data_property)
        elif data_property is None:
            display_value = self[nid].tag
        else:
            raise NodePropertyAbsentError("Node '%s' does not have data property '%s'" % (nid, data_property))

        label = ('{0}'.format(display_value)) \
            if idhidden \
            else ('{0}[{1}:{2}]'.format(display_value, self[nid].identifier, str(self[nid].data)))

        queue_filter = self._Tree__real_true if (queue_filter is None) else queue_filter

        if level == self.ROOT:
            func(label.encode('utf8'))
        else:
            leading = ''.join(map(lambda x: DT_VLINE + ' ' * 3 if not x else ' ' * 4, iflast[0:-1]))
            lasting = DT_LINE_COR if iflast[-1] else DT_LINE_BOX
            func('{0}{1}{2}'.format(leading, lasting, label).encode('utf-8'))

        if queue_filter(self[nid]) and self[nid].expanded:
            queue = [self[i] for i in self[nid].fpointer if queue_filter(self[i])]
            key = (lambda x: x) if (key is None) else key
            queue.sort(key=key, reverse=reverse)
            level += 1
            for element in queue:
                iflast.append(queue.index(element) == len(queue) - 1)
                self._Tree__print_backend(element.identifier, level, idhidden,
                                          queue_filter, key, reverse, line_type, data_property, func, iflast)
                iflast.pop()


class Printable(object):
    def __str__(self):
        pp = pprint.PrettyPrinter(indent=4)
        return pp.pformat(self.__dict__)

    def __repr__(self):
        name = self.__class__.__name__
        values = ", ".join("{}={}".format(k, repr(v)) for k, v in self.__dict__.items())
        return "{}({})".format(name, values)


class UTC(pytz.UTC):
    def __repr__(self):
        return "UTC"


MIN_DATE = datetime.min.replace(tzinfo=UTC)
MAX_DATE = datetime.max.replace(tzinfo=UTC)


def utcnow():
    return datetime.utcnow().replace(tzinfo=UTC)
