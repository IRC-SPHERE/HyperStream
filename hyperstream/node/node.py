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
import logging

from ..plate import Plate
from ..stream import StreamId
from ..utils import Printable


class Node(Printable):
    """
    A node in the graph. This consists of a set of streams defined over a set of plates
    """
    def __init__(self, channel, node_id, streams, plates):
        """
        Initialise the node

        When defining streams, it will be useful to be able to query node objects
        to determine the streams that have metadata of a particular value.
        Use Node.reverse_lookup as follows:
            meta_data = {'a': 1, 'b': 1}

        :param channel: The channel to which this node belongs
        :param node_id: The node id
        :param streams: The streams, organised as a nested dictionary with plate objects as keys at the top level,
        and then plate values (tuple(sorted(plate_values.items())) as the keys at the next level
        :param plates: The plates over which this node is defined
        """
        self._channel = channel
        self.node_id = node_id
        self._streams = streams
        for stream in streams.values():
            stream.parent_node = self

        self._factor = None  # reference to the factor that defines this node. Required for upstream computation
        self.plates = plates if plates else []
        self._is_leaf = True  # All nodes are leaf nodes until they are declared as a source node in a factor

    @property
    def streams(self):
        return self._streams

    @property
    def plate_ids(self):
        return [p.plate_id for p in self.plates]

    @property
    def plate_values(self):
        # return list(itertools.chain(*[p.values for p in self.plates]))
        return Plate.get_overlapping_values(self.plates)

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
        keys = self._streams[0].stream_id.meta_data.keys()
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
        # is_sub_plate = counts == [1, 1] and diff[1][0].is_sub_plate(diff[0][0])
        is_sub_plate = counts == [1, 1] and diff[0][0].is_sub_plate(diff[1][0])  # MK fixed
        if len(other.plates)==1 and counts==[1,0] and diff[0][0].parent==other.plates[0].parent:
            is_sub_plate = True
        return diff, counts, is_sub_plate

    def print_head(self, parent_plate_value, plate_values, interval, n=10, print_func=logging.info):
        """
        Print the first n values from the streams in the given time interval.
        The parent plate value is the value of the parent plate,
        and then the plate values are the values for the plate that are to be printed.
        e.g. print_head(None, ("house", "1"))

        :param parent_plate_value: The (fixed) parent plate value
        :param plate_values: The plate values over which to loop
        :param interval: The time interval
        :param n: The maximum number of elements to print
        :param print_func: The function used for printing (e.g. logging.info() or print())
        :return: None
        """
        if isinstance(plate_values, Plate):
            self.print_head(parent_plate_value, plate_values.values, interval, n, print_func)
            return

        if len(plate_values) == 1 and len(plate_values[0]) == 2 and isinstance(plate_values[0][0], str):
            self.print_head(parent_plate_value, (plate_values,), interval, n, print_func)
            return

        found = False
        for plate_value in plate_values:
            combined_plate_value = Plate.combine_values(parent_plate_value, plate_value)

            if combined_plate_value not in self._streams:
                # This can happen if we have created a compound plate and only certain plate values are valid
                continue

            found = True
            print_func("Plate value: {}".format(combined_plate_value))
            data = False
            for k, v in self._streams[combined_plate_value].window(interval).head(n):
                data = True
                print_func("{}, {}".format(k, v))
            if not data:
                print_func("No data")
            print_func("")
        if not found:
            print_func("No streams found for the given plate values")
