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
import os
import sys
from mqtthandler import MQTTHandler
from collections import namedtuple, OrderedDict
import json
from datetime import datetime

from . import Printable
from utils import touch, handle_exception
from ..version import __version__


class HyperStreamLogger(Printable):
    def __init__(self, default_loglevel=logging.DEBUG, console_logger=None, file_logger=None, mqtt_logger=None):
        """
        Initialise the hyperstream logger
        
        :type console_logger: bool | dict | None
        :type file_logger: bool | dict | None
        :type mqtt_logger: dict | None
        :param default_loglevel: The default logging level 
        :param file_logger: Whether to use a file logger. Either specify "True" in which case defaults are used, 
        otherwise a dict optionally containing path, filename, loglevel
        :param console_logger: The console logger. Either specify "True" in which case defaults are used, 
        otherwise a dict optionally containing loglevel
        :param mqtt_logger: Dict containing mqtt server, topic, and optionally loglevel
        """
        log_formatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
        self.root_logger = logging.getLogger()
        self.root_logger.setLevel(default_loglevel)

        # Note that if we are doing unit tests there will be a memory handler as follows:
        nose_handler_only = len(self.root_logger.handlers) == 1 \
            and str(type(self.root_logger.handlers[0])) == \
            "<class \'nose.plugins.logcapture.MyMemoryHandler\'>"

        if not self.root_logger.handlers or nose_handler_only:
            # create the handlers and call logger.addHandler(logging_handler)

            if file_logger:
                try:
                    path = file_logger['path']
                except (KeyError, TypeError):
                    path = '/tmp'

                try:
                    filename = file_logger['filename']
                except (KeyError, TypeError):
                    filename = 'hyperstream'

                try:
                    loglevel = file_logger['loglevel']
                except (KeyError, TypeError):
                    loglevel = default_loglevel

                if not os.path.exists(path):
                    os.makedirs(path)

                if not filename.endswith('.log'):
                    filename += '.log'
                full_name = os.path.join(path, filename)
                touch(full_name)

                file_handler = logging.FileHandler(full_name)
                file_handler.setFormatter(log_formatter)
                file_handler.setLevel(loglevel)
                self.root_logger.addHandler(file_handler)

            if console_logger:
                try:
                    level = console_logger['level']
                except (KeyError, TypeError):
                    level = default_loglevel

                console_handler = logging.StreamHandler()
                console_handler.setFormatter(log_formatter)
                console_handler.setLevel(level)
                self.root_logger.addHandler(console_handler)

            if mqtt_logger:
                # No sensible default for server and topic
                if 'loglevel' in mqtt_logger:
                    level = mqtt_logger.pop('loglevel')
                else:
                    level = default_loglevel

                if 'formatter' in mqtt_logger:
                    formatter = mqtt_logger.pop('formatter')
                else:
                    formatter = log_formatter

                mqtt_handler = MQTTHandler(**mqtt_logger)
                mqtt_handler.setFormatter(formatter)
                mqtt_handler.setLevel(level)
                self.root_logger.addHandler(mqtt_handler)

            # stream_handler = logging.StreamHandler()
            # stream_handler.setFormatter(log_formatter)
            # memory_handler = logging.handlers.MemoryHandler(1024 * 10, root_logger.level, stream_handler)
            # root_logger.addHandler(memory_handler)

            # Capture warnings
            logging.captureWarnings(True)

            # Capture uncaught exceptions
            sys.excepthook = handle_exception

            # logging.config.dictConfig(LOGGING)
            logging.info("HyperStream version: " + __version__)

    def set_level(self, loglevel):
        self.root_logger.setLevel(loglevel)


# Log level for monitoring data
MON = 51
logging.addLevelName(MON, "MON")


def monitor(self, message, *args, **kws):
    """
    Define a monitoring logger that will be added to Logger
    
    :param self: The logging object 
    :param message: The logging message
    :param args: Positional arguments
    :param kws: Keyword arguments
    :return: 
    """
    if self.isEnabledFor(MON):
        # Yes, logger takes its '*args' as 'args'.
        self._log(MON, message, args, **kws)


logging.Logger.monitor = monitor


def monitor(msg, *args, **kwargs):
    """
    Log a message with severity 'MON' on the root logger.
    """
    if len(logging.root.handlers) == 0:
        logging.basicConfig()
    logging.root.monitor(msg, *args, **kwargs)


logging.monitor = monitor

cls = namedtuple("SenMLValue", "n v")


class SenMLFormatter(logging.Formatter):
    """
    Formatter that matches the SenML format https://tools.ietf.org/html/draft-jennings-senml-10
    """

    def format(self, record):
        """
        The formatting function
        :param record: The record object 
        :return: The string representation of the record 
        """

        if not hasattr(record, 'n'):
            record.n = 'default'

        senml = OrderedDict(
            uid="hyperstream",
            bt=datetime.utcfromtimestamp(record.created).isoformat()[:-3] + 'Z',
            e=[OrderedDict(n=record.n, v=record.message)]
        )

        formatted_json = json.dumps(senml)
        return formatted_json
