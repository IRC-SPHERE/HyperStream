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

from ..utils import Printable, MetaDataTree
from ..models import MetaDataModel

import logging
from mongoengine.context_managers import switch_db
from treelib.tree import NodeIDAbsentError


class MetaDataManager(Printable):
    def __init__(self):
        self.global_plate_definitions = MetaDataTree()
        self.global_plate_definitions.create_node(identifier="root")

        to_be_added = dict((i, d) for i, d, in enumerate(self.global_meta_data))
        passes = 0

        # Populate the global plate definitions from dict given in the database
        while len(to_be_added) > 0:
            passes += 1
            if passes > 1000:
                raise NodeIDAbsentError("Nodes absent for ids {}"
                                        .format(", ".join(map(lambda x: x['identifier'], to_be_added.values()))))
            for i, item in to_be_added.items():
                try:
                    self.global_plate_definitions.create_node(**item)
                    del to_be_added[i]
                except NodeIDAbsentError:
                    pass

        logging.info("Global plate definitions: ")
        logging.info(self.global_plate_definitions)

    @property
    def global_meta_data(self):
        """
        Get the global meta data, which will be stored in a tree structure

        :return: The global meta data
        """
        with switch_db(MetaDataModel, 'hyperstream'):
            return sorted(map(lambda x: x.to_dict(), MetaDataModel.objects),
                          key=lambda x: len(x['identifier'].split('.')),
                          reverse=True)

    def contains(self, identifier):
        """
        Determines if the meta data with the given identifier is in the database

        :param identifier: The identifier
        :return: Whether the identifier is present
        """
        return self.global_plate_definitions.contains(identifier)

    def insert(self, tag, identifier, parent, data):
        """
        Insert the given meta data into the database

        :param tag: The tag (equates to meta_data_id)
        :param identifier: The identifier (a combination of the meta_data_id and the plate value)
        :param parent: The parent plate identifier
        :param data: The data (plate value)
        :return: None
        """
        # First try to add it into the tree
        if self.global_plate_definitions.contains(identifier):
            raise KeyError("Identifier {} already exists in tree".format(identifier))

        self.global_plate_definitions.create_node(tag=tag, identifier=identifier, parent=parent, data=data)

        # Now try to add it into the database
        with switch_db(MetaDataModel, 'hyperstream'):
            meta_data = MetaDataModel(tag=tag, parent=parent, data=data)
            meta_data.save()

        logging.info("Meta data {} inserted".format(identifier))

    def delete(self, identifier):
        """
        Delete the meta data with the given identifier from the database

        :param identifier: The identifier
        :return: None
        """

        node = self.global_plate_definitions[identifier]
        self.global_plate_definitions.remove_node(identifier)

        with switch_db(MetaDataModel, 'hyperstream'):
            meta_data = MetaDataModel.objects(tag=node.tag, data=node.data, parent=node.parent).first()
            meta_data.delete()
