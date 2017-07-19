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

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os
import sys

import json
from bidict import bidict, ValueDuplicationError
from future.utils import python_2_unicode_compatible
from treelib.tree import Tree, NodePropertyAbsentError, NodeIDAbsentError


# To restore default stdout and stderr channels after reloading sys
# source: https://github.com/ipython/ipython/issues/8354
# Backing up references to the current stdout and stderr
default_stdout = sys.stdout
default_stderr = sys.stderr

# The next two lines are to fix the "UnicodeDecodeError: 'ascii' codec can't
# decode byte" error
# http://stackoverflow.com/questions/21129020/how-to-fix-unicodedecodeerror-ascii-codec-cant-decode-byte
reload(sys)
# noinspection PyUnresolvedReferences
sys.setdefaultencoding('utf8')

# Restoring references to the previous stdout and stderr
sys.stdout = default_stdout
sys.stderr = default_stderr


@python_2_unicode_compatible
class MetaDataTree(Tree):
    (ROOT, DEPTH, WIDTH, ZIGZAG) = list(range(4))

    def __str__(self):
        self.reader = "\n"

        def write(line):
            self.reader += line.decode('utf-8') + "\n"

        self._Tree__print_backend(func=write, idhidden=False)
        return self.reader

    def __repr__(self):
        return "{} with {} nodes and depth {}".format(self.__class__.__name__, len(self.nodes), self.tree_depth)

    @property
    def tree_depth(self):
        return max(self.depth(n) for n in self.nodes)

    # noinspection PyPep8Naming,SpellCheckingInspection
    def _Tree__print_backend(self, nid=None, level=ROOT, idhidden=True, queue_filter=None,
                             key=None, reverse=False, line_type='ascii-ex',
                             data_property=None, func=print, iflast=None):
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

        if not iflast:
            iflast = []

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


@python_2_unicode_compatible
class Printable(object):
    """
    A base class for default printing
    """
    def __str__(self):
        # pp = pprint.PrettyPrinter(indent=4)
        # return pp.pformat({self.__class__.__name__: self.__dict__})
        return repr(self)

    def __repr__(self):
        name = self.__class__.__name__
        values = ", ".join("{}={}".format(k, repr(v)) for k, v in sorted(self.__dict__.items()) if k[0] != "_")
        return "{}({})".format(name, values)


class Hashable(object):
    _name = None
    """
    A base class that creates hashes based on the __dict__
    Requires keys to be strings to work properly. It will first try to use json.dumps, but if that fails because one of
    the values is not json serializable (e.g. datetime.datetime) then it will fall back on repr
    """
    @property
    def name(self):
        return self._name if self._name is not None else self.__class__.__name__

    @name.setter
    def name(self, name):
        self._name = str(name)

    def __hash__(self):
        try:
            return hash((self.name, json.dumps(self.__dict__, sort_keys=True)))
        except TypeError:
            return hash((self.name, repr(sorted(self.__dict__.items()))))


class TypedBiDict(Printable):
    """
    Custom strongly typed bi-directional dictionary where keys and values must be a specific type.
    Raises ValueDuplicationError if the same value is added again
    """

    def __init__(self, key_type, value_type, *args, **kwargs):
        if not isinstance(key_type, type):
            raise ValueError("expected type, got {}", type(key_type))
        if not isinstance(value_type, type):
            raise ValueError("expected type, got {}", type(value_type))

        self._store = bidict(*args, **kwargs)
        self.key_type = key_type
        self.value_type = value_type

    def __repr__(self):
        return "{}(key_type={}, value_type={})".format(
            self.__class__.__name__,
            repr(self.key_type),
            repr(self.value_type))

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)

    def __getitem__(self, key):
        if not isinstance(key, self.key_type):
            raise TypeError("expected {}, got {}".format(self.key_type, type(key)))
        try:
            return self._store[key]
        except KeyError as e:
            # for debugging
            raise e

    def __setitem__(self, key, value):
        if not isinstance(key, self.key_type):
            raise TypeError("expected {}, got {}".format(self.key_type, type(key)))
        if not isinstance(value, self.value_type):
            raise ValueError("expected {}, got {}".format(self.value_type, type(value)))
        try:
            self._store[key] = value
        except ValueDuplicationError as e:
            # TODO: debugging
            raise e

    def __contains__(self, item):
        return item in self._store

    def keys(self):
        return self._store.keys()

    def values(self):
        return self._store.values()

    def items(self):
        return self._store.items()

    def iterkeys(self):
        return self._store.iterkeys()

    def itervalues(self):
        return self._store.itervalues()

    def iteritems(self):
        return self._store.iteritems()


class FrozenKeyDict(dict):
    def __setitem__(self, key, value):
        if key in self:
            # Try to reconcile the new value with the old
            old = self[key]
            if isinstance(value, dict) and isinstance(old, dict):
                for k in value:
                    if k in old:
                        try:
                            if value[k] != old[k]:
                                raise KeyError(
                                    "Key {} has already been set with value {}, new value {}"
                                    .format(key, self[key], value))
                        except ValueError:
                            try:
                                # TODO: possible bug below - using all() on bool?
                                if not all(value[k] == old[k]):
                                    raise KeyError(
                                        "Key {} has already been set with value {}, new value {}"
                                        .format(key, self[key], value))
                            except ValueError as e:
                                logging.error('Unable to compare values for key {}:'
                                              ' old {}, new {}, error {}, overwriting'
                                              .format(key, self[key], value, e.message))
                                self[key][k] = value[k]
                        continue
                    self[key][k] = value[k]
            else:
                if self[key] != value:
                    raise KeyError("Key {} has already been set with value {}, new value {}".format(
                                   key, self[key], value))
            return
        super(FrozenKeyDict, self).__setitem__(key, value)


class TypedFrozenKeyDict(FrozenKeyDict):
    def __init__(self, key_type, *args, **kwargs):
        if not isinstance(key_type, type):
            raise ValueError("Expected type, got {}".format(type(key_type)))
        self.key_type = key_type
        super(TypedFrozenKeyDict, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        if not isinstance(key, self.key_type):
            raise KeyError("Expected type {}, got {}".format(self.key_type, type(key)))
        super(TypedFrozenKeyDict, self).__setitem__(key, value)


def touch(full_name, times=None):
    """
    Touch the file
    
    :type full_name: str | unicode
    :type times: tuple | None
    :param full_name: The full file path
    :param times: Tuple of (atime, mtime) access and modified time of the file
    """
    with open(full_name, 'a'):
        os.utime(full_name, times)


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
