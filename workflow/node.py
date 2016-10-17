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
from ..stream import StreamId
from ..utils import Printable

import logging


class Node(Printable):
    """
    A node in the graph. This consists of a set of streams defined over a set of plates
    """

    def __init__(self, node_id, streams, plate_ids):
        """
        Initialise the node
        :param node_id: The node id
        :param streams: The streams, organised as a nested dictionary with plate objects as keys at the top level,
        and then plate values (tuple(sorted(plate_values.items())) as the keys at the next level
        :param plate_ids: The plate ids
        """
        self.node_id = node_id
        self.streams = streams
        # TODO: Remove plate IDs
        self.plate_ids = tuple(plate_ids) if plate_ids else tuple()

        """
        When defining streams, it will be useful to be able to query node objects
        to determine the streams that have metadata of a particular value.
        Use Node.reverse_lookup as follows:
            meta_data = {'a': 1, 'b': 1}

        """

    # def __iter__(self):
    #     return iter(self.streams)

    def intersection(self, meta):
        # TODO: Why only streams[0] below?
        keys = self.streams[0].stream_id.meta_data.keys()
        return StreamId(self.node_id, dict(*zip((kk, meta[kk]) for kk in keys)))

    def print_head(self, parent_plate_value, plate_values, interval, n=10, print_func=logging.info):
        """
        Print the first n values from the streams in the given time interval.
        The parent plate value is the value of the parent plate,
        and then the plate values are the values for the plate that are to be printed.
         e.g. print_head()
        :param parent_plate_value: The parent plate value
        :param plate_values:
        :param interval:
        :param n:
        :param print_func:
        :return:
        """
        if len(plate_values) == 1 and len(plate_values[0]) == 2 and isinstance(plate_values[0][0], str):
            self.print_head(parent_plate_value, (plate_values,), interval, n)
            return

        for plate_value in plate_values:
            if parent_plate_value:
                plate_value = parent_plate_value + (plate_value,)

            print_func("Plate value: {}".format(plate_value))
            data = False
            for k, v in self.streams[tuple(sorted(plate_value))].window(interval).head(n):
                data = True
                print_func("{}, {}".format(k, v))
            if not data:
                print_func("No data")
            print_func("")
