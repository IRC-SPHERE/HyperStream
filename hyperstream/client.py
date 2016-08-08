"""
The MIT License (MIT)
Copyright (c) 2014-2015 University of Bristol

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.
"""
import sys
import logging
import pymongo
from mongoengine import connect
from sphere_connector_package.sphere_connector.utils import get_value_safe
try:
    from pymongo.errors import ServerSelectionTimeoutError
except ImportError:
    ServerSelectionTimeoutError = None


class Client(object):
    client = None
    db = None
    session = None

    def __init__(self, server_config, auto_connect=True):
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
        if 'connection_string' in server_config:
            self.client = pymongo.MongoClient(server_config['connection_string'])
            self.db = self.client[server_config['db']]
        else:
            self.client = pymongo.MongoClient(
                server_config['host'],
                server_config['port'],
                tz_aware=self.get_config_value('tz_aware', True))

            self.db = self.client[server_config['db']]

        if 'authentication_database' in server_config and server_config['authentication_database']:
            self.db.authenticate(server_config['username'], server_config['password'],
                                 source=server_config['authentication_database'])
        else:
            self.db.authenticate(server_config['username'], server_config['password'])

        # Mongo Engine connection
        d = dict((k, v) for k, v in server_config.items() if k not in ['modalities', 'summaries'])
        if 'authentication_database' in d:
            d['authentication_source'] = d['authentication_database']
            del d['authentication_database']

        self.session = connect(**d)

    def get_config_value(self, key, default=None):
        return get_value_safe(self.server_config, key, default)
