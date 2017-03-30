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
from hyperstream.utils import Printable

import itertools
from collections import deque


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
            # self._values.append(tuple(sorted(pv.items())))
            # TD: take 'sorted' out
            self._values.append(tuple(pv))
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

    def _get_identifier(self, current=None):
        if not current:
            current = []
        current.insert(0, self.identifier)

    @property
    def identifier(self):
        return None

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
        if other in self.ancestor_plates: # added by MK, but still not sure whether all cases are covered
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

    def is_ancestor(self, other):
        """
        Determines if this plate is an ancestor plate of the other (i.e. other is contained in the ancestors)

        :param other: The other plate
        :return: True if this plate is a ancestor of the other plate
        """
        return other in self.ancestor_plates[:-1]

    def is_descendant(self, other):
        """
        Determines if this plate is an descendant plate of the other (i.e. self is contained in the other's ancestors)

        :param other: The other plate
        :type other: Plate
        :return: True if this plate is a descendant of the other plate
        """
        return self in other.ancestor_plates[:-1]

    def is_parent(self, other):
        """
        Determines if this plate is a parent plate of the other

        :param other: The other plate
        :type other: Plate
        :return: True if this plate is a parent of the other plate
        """
        return self.parent == other

    def is_child(self, other):
        """
        Determines if this plate is a child plate of the other

        :param other: The other plate
        :type other: Plate
        :return: True if this plate is a child of the other plate
        """
        return self == other.parent

    @staticmethod
    def combine_values(parent_plate_value, plate_value):
        """
        Combine the plate value(s) with the parent plate value(s)
        :param parent_plate_value: The parent plate value(s)
        :param plate_value: The plate value(s)
        :return: The combined plate values
        """
        if parent_plate_value:
            if isinstance(plate_value[0], (str, unicode)):
                combined_plate_value = parent_plate_value + (plate_value,)
            elif isinstance(plate_value[0], tuple):
                combined_plate_value = parent_plate_value + plate_value
            else:
                raise TypeError("Unknown plate value type")
        else:
            combined_plate_value = plate_value

        return tuple(sorted(combined_plate_value))

    @staticmethod
    def get_overlapping_values(plates):
        """
        Need to find where in the tree the two plates intersect, e.g.

        We are given as input plates D, E, whose positions in the tree are:

        root -> A -> B -> C -> D
        root -> A -> B -> E

        The results should then be the cartesian product between C, D, E looped over A and B

        If there's a shared plate in the hierarchy, we need to join on this shared plate, e.g.:

        [self.plates[p].values for p in plate_ids][0] =
          [(('house', '1'), ('location', 'hallway'), ('wearable', 'A')),
           (('house', '1'), ('location', 'kitchen'), ('wearable', 'A'))]
        [self.plates[p].values for p in plate_ids][1] =
          [(('house', '1'), ('scripted', '15')),
           (('house', '1'), ('scripted', '13'))]

        Result should be one stream for each of:
          [(('house', '1'), ('location', 'hallway'), ('wearable', 'A'), ('scripted', '15)),
           (('house', '1'), ('location', 'hallway'), ('wearable', 'A'), ('scripted', '13)),
           (('house', '1'), ('location', 'kitchen'), ('wearable', 'A'), ('scripted', '15)),
           (('house', '1'), ('location', 'kitchen'), ('wearable', 'A'), ('scripted', '13))]

        :param plates: The input plates
        :return: The plate values
        :type plates: list[Plate] | list[Plate]
        """
        if not plates:
            return None

        if len(plates) == 1:
            return plates[0].values

        if len(plates) > 2:
            raise NotImplementedError

        # First check for the simple case where one of the plates has no parent
        # and does not share meta data with the other
        plates_sorted = sorted(plates, key=lambda item: len(item.ancestor_plates))
        if plates_sorted[0].is_root:
            if plates_sorted[0].meta_data_id not in plates_sorted[1].ancestor_meta_data_ids:
                return map(lambda x: tuple(itertools.chain(*x)), itertools.product(plates[0].values, plates[1].values))

        # Get all of the ancestors zipped together, padded with None
        ancestors = deque(itertools.izip_longest(*(p.ancestor_plates for p in plates)))

        last_values = []
        while len(ancestors) > 0:
            current = ancestors.popleft()
            if current[0] == current[1]:
                # Plates are identical, take all values valid for matching parents
                if last_values:
                    raise NotImplementedError
                else:
                    last_values.extend(current[0].values)

            elif current[0] is not None and current[1] is not None \
                    and current[0].meta_data_id == current[1].meta_data_id:
                # Not identical, but same meta data id. Take all overlapping values valid for matching parents
                if last_values:
                    raise NotImplementedError
                else:
                    raise NotImplementedError
            else:
                # Different plates, take cartesian product of values with matching parents.
                # Note that one of them may be none
                if last_values:
                    tmp = []
                    for v in last_values:
                        # Get the valid ones based on v
                        # valid = [filter(lambda x: all(xx in v for xx in x[:-1]), c.values)
                        #          for c in current if c is not None]
                        valid = [filter(lambda x: all(vv in x for vv in v), c.values)
                                 for c in current if c is not None]

                        # Strip out v from the valid ones
                        stripped = [map(lambda y: tuple(itertools.chain(*(yy for yy in y if yy not in v))), val)
                                    for val in valid]

                        # Get the cartesian product. Note that this still works if one of the current is None
                        prod = list(itertools.product(*stripped))

                        # Now update the last values be the product with v put back in
                        new_values = [v + p for p in prod]
                        if new_values:
                            tmp.append(new_values)

                    last_values = list(itertools.chain(*tmp))
                    if not last_values:
                        raise ValueError("Plate value computation failed - possibly there were no shared plate values")
                else:
                    raise NotImplementedError

        if not last_values:
            raise ValueError("Plate value computation failed - possibly there were no shared plate values")

        return last_values
