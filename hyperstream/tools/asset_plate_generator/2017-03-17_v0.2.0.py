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

from hyperstream.stream import StreamInstance, StreamMetaInstance
from hyperstream.tool import PlateCreationTool

import logging


class AssetPlateGenerator(PlateCreationTool):
    def __init__(self, element=None, filters=None, use_value_instead_of_key=False):
        """
        Special tool to extract data from the asset channel
        :param element: The element to extract
        :param use_value_instead_of_key: The plate values are to be taken from the values of the dict, rather than from
        the keys (default: False)
        :param filters: Filters for which meta data are used. If this is None or empty, then all meta data will be used,
        otherwise these filters will act as a white-list
        Note that if the documents are lists instead of dicts then this option has no effect
        """
        super(AssetPlateGenerator, self).__init__(element=element, filters=filters,
                                                  use_value_instead_of_key=use_value_instead_of_key)

    def _execute(self, source, interval):
        source_last_doc = source.window(interval, force_calculation=True).last()
        if not source_last_doc:
            return
        timestamp, data = source_last_doc
        if self.element is None:
            data_element = data
        else:
            if self.element in data:
                data_element = data[self.element]
            else:
                return
        try:
            # try if data_element can be used as a dict
            for key, value in data_element.items():
                v = value if self.use_value_instead_of_key else key
                instance = self.generate_instance(timestamp, value, v)
                if instance:
                    yield instance
        except AttributeError:
            # otherwise assume that data_element can be used as a list
            for value in data_element:
                instance = self.generate_instance(timestamp, value, value)
                if instance:
                    yield instance

    def generate_instance(self, timestamp, value, meta_data):
        """
        Generate a stream instance, checking if the filters are matched
        :param timestamp: The timestamp
        :param value: The value
        :param meta_data: The meta data
        :return: The stream instance (or None)
        """
        if not self.filters or meta_data in self.filters:
            if self.filters:
                logging.debug('Filtered in  for {} from {}'.format(meta_data, self.filters))
            return StreamMetaInstance(StreamInstance(timestamp=timestamp, value=value), meta_data)
        if self.filters:
            logging.debug('Filtered out for {} from {}'.format(meta_data, self.filters))
        return None

