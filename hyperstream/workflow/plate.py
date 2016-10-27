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
Plate and plate manager definitions.
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

        self.values = []
        self.cardinality = 0
        for pv in values:
            self.values.append(tuple(sorted(pv.items())))
            if self.cardinality == 0:
                self.cardinality = len(pv)
            else:
                if len(pv) != self.cardinality:
                    raise ValueError("Plate values have inconsistent cardinality")

        self.values = [tuple(sorted(pv.items())) for pv in values]
        self.parent = parent_plate

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
        return [p.plate_id for p in self.ancestor_plates]

    @property
    def ancestor_plates(self):
        return self._get_ancestors(current=None)

    @property
    def ancestor_meta_data_ids(self):
        return [p.meta_data_id for p in self.ancestor_plates]

    @property
    def is_root(self):
        return self.parent is None
