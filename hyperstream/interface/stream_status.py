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
from mongoengine import Document, DateTimeField, StringField, DictField, EmbeddedDocumentListField
# from ..utils import Printable
from ..utils import TimeRange


class DateTimeRange(Document):
    start = DateTimeField(required=True)
    end = DateTimeField(required=True)


class StreamStatusModel(Document):
    stream_id = StringField(required=True, min_length=1, max_length=512)
    stream_type = StringField(required=True, min_length=1, max_length=512)
    last_updated = DateTimeField(required=True)
    computed_ranges = EmbeddedDocumentListField(document_type=DateTimeRange, required=True)
    filters = DictField(required=False)
    kernel_version = StringField(required=True, min_length=1, max_length=512)

    meta = {
        'collection': 'streams',
        'indexes': [{'fields': ['stream_id']}],
        'ordering': ['start']
    }

    @property
    def time_ranges(self):
        return [TimeRange(dt.start, dt.end) for dt in self.computed_ranges]

    @time_ranges.setter
    def time_ranges(self, ranges):
        self.computed_ranges = [DateTimeRange(DateTimeField(dt.start), DateTimeField(dt.end)) for dt in ranges]

    def add_time_range(self, time_range):
        self.time_ranges.append(time_range)
        self.update_time_ranges()

    def update_time_ranges(self):
        sorted_by_lower_bound = sorted(self.time_ranges, key=lambda r: r.start)
        merged = []

        for higher in sorted_by_lower_bound:
            if not merged:
                merged.append(higher)
            else:
                lower = merged[-1]
                # test for intersection between lower and higher:
                # we know via sorting that lower[0] <= higher[0]
                if higher[0] <= lower[1]:
                    upper_bound = max(lower[1], higher[1])
                    merged[-1] = (lower[0], upper_bound)  # replace by merged interval
                else:
                    merged.append(higher)
        self.time_ranges = merged

# class StreamStatus(Printable):
#     def __init__(self, stream_id, stream_type, last_updated, filters, computed_ranges, version):
#         self.stream_id = stream_id
#         self.stream_type = stream_type
#         self.last_updated = last_updated
#         self.computed_ranges = computed_ranges
#         self.filters = filters
#         self.version = version
#
#     def to_model(self):
#         return StreamStatusModel(**self.__dict__)
