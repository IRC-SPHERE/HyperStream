"""
The MIT License (MIT)
Copyright (c) 2014-2016 University of Bristol

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
import logging
from utils import Printable
from time_range import compare_time_ranges
from stream_status import StreamStatusModel
from datetime import datetime


class Stream(Printable):
    completed = False
    current_status = None

    def __init__(self, stream_id, kernel, sources, parameters, filters, stream_type):
        self.stream_id = stream_id
        self.kernel = kernel
        self.sources = sources
        self.parameters = parameters
        self.filters = filters
        self.stream_type = stream_type

    def execute(self, clients, configs, time_ranges):
        logging.info("Executing stream " + self.stream_id)

        if not time_ranges:
            raise RuntimeError("No time ranges for stream " + self.stream_id)

        status_docs = StreamStatusModel.objects(
            stream_id=self.stream_id,
            kernel_version=self.kernel.version
        )

        if not status_docs:
            # This has never been run before
            self.current_status = StreamStatusModel(
                stream_id=self.stream_id,
                kernel_version=self.kernel.version,
                stream_type=self.stream_type,
                computed_ranges=[],
                filters=self.filters
            )
            # (don't) Add this to the database (yet)
            # self.current_status.save()
        else:
            self.current_status = status_docs[0]

        # Ensure all sources have been executed, if not, execute
        if self.sources:
            logging.info("Looping through sources")
            for s in self.sources:
                # TODO: more logic here
                s.execute(clients, configs, time_ranges)

        time_ranges = compare_time_ranges(desired_ranges=time_ranges, computed_ranges=self.current_status.time_ranges)

        # Now execute the kernel
        for time_range in time_ranges:
            self.kernel.runner.execute(self, clients, configs, time_range)
            self.current_status.add_time_range(time_range)
            self.current_status.last_updated = datetime.utcnow()
            self.current_status.save()

    def __repr__(self):
        return str(self)
