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
from . import ChannelManager, HyperStreamConfig, PlateManager, WorkflowManager, Client, Workflow, HyperStreamLogger

import logging


class HyperStream(object):
    def __init__(self):
        self.logger = HyperStreamLogger(path='/tmp', filename='hyperstream', loglevel=logging.DEBUG)
        self.config = HyperStreamConfig()
        self.client = Client(self.config.mongo)

        # Define some managers
        self.channel_manager = ChannelManager(self.config.tool_path)
        self.plate_manager = PlateManager(self.config.meta_data)
        self.workflow_manager = WorkflowManager(channel_manager=self.channel_manager,
                                                plates=self.plate_manager.plates)

    def create_workflow(self, workflow_id, name, owner, description):
        w = Workflow(
            channels=self.channel_manager,
            plates=self.plate_manager.plates,
            workflow_id=workflow_id,
            name=name,
            owner=owner,
            description=description
        )
        self.workflow_manager.add_workflow(w)

        return w
