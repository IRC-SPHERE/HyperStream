# The MIT License (MIT) # Copyright (c) 2014-2017 University of Bristol
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
# OR OTHER DEALINGS IN THE SOFTWARE.
"""
The main hyperstream client connection that is used for storing runtime
information.  Note that this is also used by the default database channel,
although other database channels (connecting to different database types) can
also be used.
"""

import sys
import logging
import pymongo
from mongoengine import connect, connection
try:
    from pymongo.errors import ServerSelectionTimeoutError
except ImportError:
    ServerSelectionTimeoutError = None

from utils import Printable


class Client(Printable):
    """
    The main mongo client
    """
    client = None
    db = None
    session = None

    def __init__(self, server_config, auto_connect=True):
        """Initialise the client

        :param server_config: The server configuration
        :param auto_connect: Whether to automatically connect
        """
        self.server_config = server_config

        if auto_connect:
            if ServerSelectionTimeoutError:
                try:
                    self.connect(server_config)
                except ServerSelectionTimeoutError as e:
                    logging.warn(e.message)
                    sys.exit()
            else:
                self.connect(server_config)

    def connect(self, server_config):
        """Connect using the configuration given

        :param server_config: The server configuration
        """
        if 'connection_string' in server_config:
            self.client = pymongo.MongoClient(
                    server_config['connection_string'])
            self.db = self.client[server_config['db']]
        else:
            self.client = pymongo.MongoClient(
                server_config['host'],
                server_config['port'],
                tz_aware=self.get_config_value('tz_aware', True))

            self.db = self.client[server_config['db']]

        if ('authentication_database' in server_config and
                server_config['authentication_database']):
            self.db.authenticate(
                    server_config['username'], server_config['password'],
                    source=server_config['authentication_database'])
        else:
            if 'username' in server_config:
                if 'password' in server_config:
                    self.db.authenticate(server_config['username'],
                                         server_config['password'])
                else:
                    self.db.authenticate(server_config['username'])

        # Mongo Engine connection
        d = dict((k, v) for k, v in server_config.items()
                 if k not in ['modalities', 'summaries'])
        if 'authentication_database' in d:
            d['authentication_source'] = d['authentication_database']
            del d['authentication_database']

        self.session = connect(alias="hyperstream", **d)

        # TODO: This sets the default connection of mongoengine, but seems to be a bit of a hack
        if "default" not in connection._connections:
            connection._connections["default"] = connection._connections["hyperstream"]
            connection._connection_settings["default"] = connection._connection_settings["hyperstream"]

    def get_config_value(self, key, default=None):
        """Get a specific value from the configuration

        :param key: The of the item
        :param default: A default value if not found
        :return: The found value or the default
        """
        return self.server_config.get(key, default)
