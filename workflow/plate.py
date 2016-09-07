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

from ..utils import Printable, MetaDataTree
from ..models import PlateDefinitionModel

import logging


class Plate(Printable):
    def __init__(self, meta_data_id, values):
        self.meta_data_id = meta_data_id
        self.values = [tuple(sorted(pv.items())) for pv in values]


class PlateManager(Printable):
    def __init__(self, meta_data):
        self.plates = {}

        self.global_plate_definitions = MetaDataTree()

        # Populate the global plate definitions from dict given in the config file
        for item in meta_data:
            self.global_plate_definitions.create_node(**item)

        logging.info("Global plate definitions: ")
        logging.info(self.global_plate_definitions)

        # Plate definitions (arrays of streams)
        for p in PlateDefinitionModel.objects:
            if not p.values and not p.complement:
                raise ValueError("Empty values in plate definition and complement=False")

            """
            Want to get meta-data dictionaries for all plate combinations
            e.g.

            H plate, want

            {'H': [
              {'house': '1'},
              {'house': '2'}
            ]}

            H1 plate, want

            {'H1': [{'house': '1'}]}

            H.R plate, want

            {'H.R': [
              {'house': '1', 'resident': '1'},
              {'house': '1': 'resident': '2'},
              {'house': '2': 'resident': '1'}
            }
            """

            if p.plate_id in (u'H1', u'H1.kitchen', u'H1_str'):
                logging.debug(p.plate_id)

            values = []
            for n in self.global_plate_definitions.all_nodes():
                if n.tag == p.meta_data_id:
                    if not p.values or n.data in p.values:
                        if p.parent_plate:
                            # This plate has parent plates, so we need to get parent data for the node
                            values.insert(0, self.get_parent_data(self.global_plate_definitions, n, {n.tag: n.data}))
                        else:
                            values.insert(0, {n.tag: n.data})
                    # else:
                    #     logging.warn('Invalid plate value "{}" for node id "{}"'.format(n.data, n.identifier))
            if not values:
                raise ValueError("Plate values for {} empty".format(p.plate_id))

            self.plates[p.plate_id] = Plate(meta_data_id=p.meta_data_id, values=values)

    @staticmethod
    def get_parent_data(tree, node, d):
        """
        Recurse up the tree getting parent data
        :param tree: The tree
        :param node: The current node
        :param d: The initial dictionary
        :return: The hierarchical dictionary
        """
        parent = tree.parent(node.identifier)
        if parent.is_root():
            return d
        d[parent.tag] = parent.data
        return PlateManager.get_parent_data(tree, parent, d)
