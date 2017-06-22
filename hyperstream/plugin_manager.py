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
"""
Plugin manager module for additional user added channels and tools.
"""

import imp
import os
import sys
from collections import namedtuple

from .utils import Printable, utcnow
from .channels import ToolChannel, AssetsChannel2


class Plugin(namedtuple("PluginBase", "channel_id_prefix path channel_names has_tools has_assets"), Printable):
    """
    Plugin class - simple wrapper over namedtuple
    """

    def load_channels(self):
        """
        Loads the channels and tools given the plugin path specified

        :return: The loaded channels, including a tool channel, for the tools found.
        """
        channels = []

        # Try to get channels
        for channel_name in self.channel_names:
            channel_path = os.path.join(self.path, "channels")
            sys.path.append(self.path)
            mod = imp.load_module(channel_name, *imp.find_module(channel_name, [channel_path]))
            cls = getattr(mod, channel_name.title().replace("_", ""))
            channel_id = channel_name.split("_")[0]
            # TODO: what about up_to_timestamp?
            try:
                channels.append(cls(channel_id, up_to_timestamp=None))
            except TypeError:
                channels.append(cls(channel_id))

        # Try to get tools
        if self.has_tools:
            tool_path = os.path.join(self.path, "tools")
            # Create a tool channel using this path
            channel_id = self.channel_id_prefix + "_" + "tools"
            channel = ToolChannel(channel_id, tool_path, up_to_timestamp=utcnow())
            channels.append(channel)

        if self.has_assets:
            assset_path = os.path.join(os.path.abspath(self.path), "assets")
            channel_id = self.channel_id_prefix + "_" + "assets"
            channel = AssetsChannel2(channel_id, assset_path, up_to_timestamp=utcnow())
            channels.append(channel)
            #
            # from . import TimeInterval
            # channel.streams.values()[0].window(TimeInterval.up_to_now()).items()

        return channels
