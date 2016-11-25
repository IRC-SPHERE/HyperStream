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
"""
Plate definition.
"""
from ..utils import Printable


class Plate(Printable):
    """
    A plate in the execution graph. This can be thought of as a "for loop" over the streams in a node
    """
    def __init__(self, plate_id, meta_data_id, values, parent_plate=None):
        """
        Initialise the plate
        :param plate_id: Plate ID
        :param meta_data_id: Meta data ID - corresponds to the tag in the meta data definitions
        :param values: The plate values - corresponds to the data in the meta data definitions
        :param parent_plate: The parent plate (object reference)
        :type parent_plate: Plate
        """
        self.plate_id = plate_id
        self.meta_data_id = meta_data_id

        self._values = []
        self._cardinality = 0
        for pv in values:
            self._values.append(tuple(sorted(pv.items())))
            if self._cardinality == 0:
                self._cardinality = len(pv)
            else:
                if len(pv) != self._cardinality:
                    raise ValueError("Plate values have inconsistent cardinality")

        # self._values = [tuple(sorted(pv.items())) for pv in values]
        self._parent = parent_plate

    @property
    def parent(self):
        return self._parent

    @property
    def values(self):
        return self._values

    def _get_ancestors(self, current=None):
        """
        Gets the ancestors of this node
        :param current: The current ancestor list
        :return: The ancestor list
        :type current: list[Plate] | list[str] | list[unicode] | None
        :rtype: list[Plate] | list[str] | list[unicode]
        """
        if not current:
            current = []
        current.insert(0, self)
        if self.is_root:
            if current:
                return current
        else:
            return self.parent._get_ancestors(current)

    @property
    def ancestor_plate_ids(self):
        """
        The plate ids of all ancestor plates in the tree
        """
        return [p.plate_id for p in self.ancestor_plates]

    @property
    def ancestor_plates(self):
        """
        All ancestor plates in the tree
        """
        return self._get_ancestors(current=None)

    @property
    def ancestor_meta_data_ids(self):
        """
        The meta data ids of all ancestor plates in the tree
        """
        return [p.meta_data_id for p in self.ancestor_plates]

    @property
    def is_root(self):
        """
        True if this plate is at the root of the tree, i.e. has no parent plate
        """
        return self.parent is None

    def is_sub_plate(self, other):
        """
        Determines if this plate is a sub-plate of another plate -
        i.e. has the same meta data but a restricted set of values

        :param other: The other plate
        :return: True if this plate is a sub-plate of the other plate
        """
        if all(v in set(other.values) for v in self.values):
            return True
        if all(any(all(spv in m for spv in v) for m in map(set, other.values)) for v in self.values):
            return True
        return False

    def is_super_plate(self, other):
        """
        Determines if this plate is a super-plate of another plate -
        i.e. has the same meta data but a larger set of values

        :param other: The other plate
        :return: True if this plate is a super-plate of the other plate
        """
        return other.is_sub_plate(self)
