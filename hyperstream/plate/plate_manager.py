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

import logging

from mongoengine import DoesNotExist, MultipleObjectsReturned
from mongoengine.context_managers import switch_db

from ..meta_data import MetaDataManager
from ..models import PlateDefinitionModel
from ..plate import Plate
from ..utils import Printable, PlateDefinitionError


class PlateManager(Printable):
    """
    Plate manager. Manages the mapping between plates defined in the database with the global meta data definition.
    """
    def __init__(self):
        """
        Initialise the manager

        Want to get meta-data dictionaries for all plate combinations
        e.g.

        H plate, want

        {'H': [
          (('house', '1'),),
          {'house': '2'}
        ]}

        H1 plate, want

        {'H1': [(('house', '1'),)]}

        H.R plate, want

        {'H.R': [
          {'house': '1', 'resident': '1'},
          {'house': '1': 'resident': '2'},
          {'house': '2': 'resident': '1'}
        }
        """
        self.plates = {}
        self.meta_data_manager = MetaDataManager()

        # Plate definitions (arrays of streams)
        with switch_db(PlateDefinitionModel, db_alias="hyperstream"):
            for p in PlateDefinitionModel.objects:
                self.add_plate(p)
                
    def delete_plate(self, plate_id):
        """
        Delete a plate from the database
        
        :param plate_id:
        :return:
        """

        with switch_db(PlateDefinitionModel, "hyperstream"):
            try:
                p = PlateDefinitionModel.objects.get(plate_id=plate_id)
                p.delete()
            except DoesNotExist as e:
                logging.warn(e.message)

    def create_plate(self, plate_id, description, meta_data_id, values, complement, parent_plate):
        """
        Create a new plate, and commit it to the database

        :param plate_id: The plate id - required to be unique
        :param description: A human readable description
        :param meta_data_id: The meta data id, which should correspond to the tag in the global meta data
        :param values: Either a list of string values, or the empty list (for use with complement)
        :param complement: If complement is true, then the complement of the values list will be used when getting
        values from the global meta data
        :param parent_plate: The parent plate identifier
        :return: The newly created plate
        :type plate_id: str | unicode
        :type complement: bool
        :type values: list | tuple
        """
        # Make sure the plate id doesn't already exist
        with switch_db(PlateDefinitionModel, db_alias='hyperstream'):
            try:
                p = PlateDefinitionModel.objects.get(plate_id=plate_id)
                if p:
                    logging.info("Plate with id {} already exists".format(plate_id))
                    return self.plates[plate_id]
            except DoesNotExist:
                pass
            except MultipleObjectsReturned:
                raise

            plate_definition = PlateDefinitionModel(
                plate_id=plate_id,
                description=description,
                meta_data_id=meta_data_id,
                values=values,
                complement=complement,
                parent_plate=parent_plate
            )

            self.add_plate(plate_definition)
            plate_definition.save()
            return self.plates[plate_id]

    def add_plate(self, plate_definition):
        """
        Add a plate using the plate definition
        :param plate_definition: The plate definition
        :return: None
        :type plate_definition: PlateDefinitionModel
        """
        values = self.get_plate_values(plate_definition)

        # TODO: We should also be checking that the plate is defined over all of the values of the parent plate
        self.plates[plate_definition.plate_id] = Plate(
            plate_id=plate_definition.plate_id,
            meta_data_id=plate_definition.meta_data_id,
            values=values,
            parent_plate=self.plates[plate_definition.parent_plate] if plate_definition.parent_plate else None)

        logging.debug("Added plate: {}".format(self.plates[plate_definition.plate_id]))

    def get_plate_values(self, plate_definition):
        """
        Gets the plate values from the global meta data according to the given plate definition
        :param plate_definition: The plate definition
        :return: The plate values
        :type plate_definition: PlateDefinitionModel
        """
        if not plate_definition.values and not plate_definition.complement:
            raise PlateDefinitionError()

        values = []
        for n in self.meta_data_manager.global_plate_definitions.all_nodes():
            if n.tag == plate_definition.meta_data_id:
                if not plate_definition.values or n.data in plate_definition.values:
                    if plate_definition.parent_plate:
                        # This plate has parent plates, so we need to get parent data for the node
                        parent_plate_value = self.get_parent_plate_value(
                            self.meta_data_manager.global_plate_definitions, n)
                        # if tuple(parent_plate_value) not in self.plates[plate_definition.parent_plate].values:
                        if set(parent_plate_value) not in map(set, self.plates[plate_definition.parent_plate].values):
                            continue
                        values.insert(0, self.get_parent_data(
                            self.meta_data_manager.global_plate_definitions, n, [(n.tag, n.data), ]))
                    else:
                        values.insert(0, [(n.tag, n.data), ])
        if not values:
            # raise PlateEmptyError(plate_definition.plate_id)
            logging.warn("Plate {} is empty during the creation".format(plate_definition.plate_id))
        return values

    @staticmethod
    def get_parent_plate_value(tree, node, value=None):
        """
        Recurse up the tree getting parent plate values
        :param tree: The tree
        :param node: The current node
        :param value: The initial plate value
        :return: The plate value as a list of tuples
        """
        if value is None:
            value = []
        parent = tree.parent(node.identifier)
        if parent.is_root():
            # value.append((parent.tag, parent.identifier))
            return value
        value = PlateManager.get_parent_plate_value(tree, parent, value)
        if "." in parent.identifier:
            pass
        value.append((parent.tag, parent.data))
        return value

    @staticmethod
    def get_parent_data(tree, node, current):
        """
        Recurse up the tree getting parent data
        :param tree: The tree
        :param node: The current node
        :param current: The current list
        :return: The hierarchical dictionary
        """
        if not current:
            current = []

        parent = tree.parent(node.identifier)
        if parent.is_root():
            return current

        current.insert(0, (parent.tag, parent.data))
        return PlateManager.get_parent_data(tree, parent, current)
