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
import pymongo
from sphere_connector_package.sphere_connector.utils import get_value_safe


class Client(object):
    def __init__(self, server_config):
        self.server_config = server_config
        
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

    def get_config_value(self, key, default=None):
        return get_value_safe(self.server_config, key, default)
