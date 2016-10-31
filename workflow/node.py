# The MIT License (MIT)
# Copyright (c) 2014-2017 University of Bristol
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.
"""
Node module.

Nodes are a collection of streams defined by shared meta-data keys (plates), and are connected in the
computational graph by factors.
"""
from ..stream import StreamId
from ..utils import Printable

import logging
import itertools
from collections import deque


class Node(Printable):
    """
    A node in the graph. This consists of a set of streams defined over a set of plates
    """
    def __init__(self, node_id, streams, plates):
        """
        Initialise the node

        When defining streams, it will be useful to be able to query node objects
        to determine the streams that have metadata of a particular value.
        Use Node.reverse_lookup as follows:
            meta_data = {'a': 1, 'b': 1}

        :param node_id: The node id
        :param streams: The streams, organised as a nested dictionary with plate objects as keys at the top level,
        and then plate values (tuple(sorted(plate_values.items())) as the keys at the next level
        :param plates: The plates over which this node is defined
        """
        self.node_id = node_id
        self.streams = streams
        for stream in streams.values():
            stream.parent_node = self

        self._factor = None  # reference to the factor that defines this node. Required for upstream computation
        self.plates = plates if plates else []
        self._is_leaf = True  # All nodes are leaf nodes until they are declared as a source node in a factor

    @property
    def plate_ids(self):
        return [p.plate_id for p in self.plates]

    @property
    def plate_values(self):
        # return list(itertools.chain(*[p.values for p in self.plates]))
        return get_overlapping_plate_values(self.plates)

    @property
    def factor(self):
        return self._factor

    @factor.setter
    def factor(self, value):
        self._factor = value

    @property
    def is_leaf(self):
        # return not self.factor
        return self._is_leaf

    @is_leaf.setter
    def is_leaf(self, value):
        self._is_leaf = value

    def intersection(self, meta):
        """
        Get the intersection between the meta data given and the meta data contained within the plates.
        Since all of the streams have the same meta data keys (but differing values) we only need to consider
        the first stream.
        :param meta: The meta data to compare
        :return: A stream id with the intersection between this node's meta data and the given meta data
        :type meta: dict
        :rtype: StreamId
        """
        keys = self.streams[0].stream_id.meta_data.keys()
        return StreamId(self.node_id, dict(*zip((kk, meta[kk]) for kk in keys)))

    def difference(self, other):
        """
        Summarise the differences between this node and the other node.

        :param other: The other node
        :return: A tuple containing the diff, the counts of the diff, and whether this plate is a sub-plate of the other
        :type other: Node
        """
        diff = (tuple(set(self.plates) - set(other.plates)), tuple(set(other.plates) - set(self.plates)))
        counts = map(len, diff)
        is_sub_plate = counts == [1, 1] and diff[1][0].is_sub_plate(diff[0][0])
        return diff, counts, is_sub_plate

    def print_head(self, parent_plate_value, plate_values, interval, n=10, print_func=logging.info):
        """
        Print the first n values from the streams in the given time interval.
        The parent plate value is the value of the parent plate,
        and then the plate values are the values for the plate that are to be printed.
         e.g. print_head()
        :param parent_plate_value: The (fixed) parent plate value
        :param plate_values: The plate values over which to loop
        :param interval: The time interval
        :param n: The maximum number of elements to print
        :param print_func: The function used for printing (e.g. logging.info() or print())
        :return: None
        """
        # Deferred import to avoid circular dependence
        from . import Plate
        if isinstance(plate_values, Plate):
            self.print_head(parent_plate_value, plate_values.values, interval, n, print_func)
            return

        if len(plate_values) == 1 and len(plate_values[0]) == 2 and isinstance(plate_values[0][0], str):
            self.print_head(parent_plate_value, (plate_values,), interval, n, print_func)
            return

        found = False
        for plate_value in plate_values:
            combined_plate_value = self.combine_plate_values(parent_plate_value, plate_value)

            if combined_plate_value not in self.streams:
                # This can happen if we have created a compound plate and only certain plate values are valid
                continue

            found = True
            print_func("Plate value: {}".format(combined_plate_value))
            data = False
            for k, v in self.streams[combined_plate_value].window(interval).head(n):
                data = True
                print_func("{}, {}".format(k, v))
            if not data:
                print_func("No data")
            print_func("")
        if not found:
            print_func("No streams found for the given plate values")

    @staticmethod
    def combine_plate_values(parent_plate_value, plate_value):
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


def get_overlapping_plate_values(plates):
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
                    valid = [filter(lambda x: all(xx in v for xx in x[:-1]), c.values)
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
                    raise Exception
            else:
                raise NotImplementedError

    if not last_values:
        raise ValueError("Plate value computation failed - possibly there were no shared plate values")

    return last_values
