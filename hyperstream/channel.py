"""
The MIT License (MIT)
Copyright (c) 2014-2017 University of Bristol

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
from utils import Printable, utcnow
from channels import ToolChannel, SphereChannel, MemoryChannel, DatabaseChannel
from errors import StreamNotFoundError


class ChannelCollection(Printable):
    def __init__(self, tool_path):
        self.tool_channel = ToolChannel("tools", tool_path, up_to_timestamp=utcnow())
        self.sphere_channel = SphereChannel("sphere")
        self.memory_channel = MemoryChannel("memory")
        self.database_channel = DatabaseChannel("mongo")

    def __getitem__(self, item):
        if item in self.memory_channel.streams:
            return self.memory_channel.streams[item]
        if item in self.database_channel.streams:
            return self.database_channel.streams[item]
        # if item in self.tool_channel.streams:
        #     return self.tool_channel.streams[item]
        if item in self.sphere_channel.streams:
            return self.sphere_channel.streams[item]
        raise StreamNotFoundError("{} not found in channels".format(item))
