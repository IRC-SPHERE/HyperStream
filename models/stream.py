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
from mongoengine import Document, DateTimeField, StringField, DictField, DynamicField, MapField, EmbeddedDocument, \
    EmbeddedDocumentField, EmbeddedDocumentListField
from time_range import TimeRangeModel


class StreamInstanceModel(Document):
    stream_id = StringField(required=True, min_length=1, max_length=512)
    stream_type = StringField(required=True, min_length=1, max_length=512)
    datetime = DateTimeField(required=True)
    filters = DictField(required=False)
    metadata = DictField(required=False)
    version = StringField(required=True, min_length=1, max_length=512)
    value = DynamicField(required=True)

    meta = {
        'collection': 'streams',
        'indexes': [{'fields': ['stream_id']}],
        'ordering': ['start']
    }


class StreamParameterModel(EmbeddedDocument):
    dtype = StringField(required=True, min_length=1, max_length=32)
    value = DictField(required=True)


class StreamDefinitionModel(Document):
    stream_id = StringField(required=True, min_length=1, max_length=512)
    last_updated = DateTimeField(required=True)
    tool_name = StringField(required=True, min_length=1, max_length=512)
    tool_version = StringField(required=True, min_length=1, max_length=512)
    parameters = MapField(EmbeddedDocumentField(StreamParameterModel))
    sandbox = StringField()
    meta_data = DictField()

    meta = {
        'collection': 'stream_definitions',
        'indexes': [{'fields': ['stream_id', 'tool_version']}],
        'ordering': ['last_updated']
    }


class StreamStatusModel(Document):
    stream_id = StringField(required=True, min_length=1, max_length=512)
    stream_type = StringField(required=True, min_length=1, max_length=512)
    last_updated = DateTimeField(required=True)
    last_accessed = DateTimeField(required=False)
    computed_ranges = EmbeddedDocumentListField(document_type=TimeRangeModel, required=True)

    meta = {
        'collection': 'stream_status',
        'indexes': [{'fields': ['stream_id']}],
        'ordering': ['start']
    }

    def add_time_range(self, time_range):
        if not isinstance(time_range, TimeRangeModel):
            raise RuntimeError("Can only add TimeRangeModel objects")

        self.computed_ranges.append(time_range)
        self.update_time_ranges()
        return self.computed_ranges

    def update_time_ranges(self):
        sorted_by_lower_bound = sorted(self.computed_ranges, key=lambda r: r.start)
        merged = []

        for higher in sorted_by_lower_bound:
            if not merged:
                merged.append(higher)
            else:
                lower = merged[-1]
                # test for intersection between lower and higher:
                # we know via sorting that lower.start <= higher.start
                if higher.start <= lower.end:
                    upper_bound = max(lower.end, higher.end)
                    merged[-1] = TimeRangeModel(start=lower.start, end=upper_bound)  # replace by merged interval
                else:
                    merged.append(higher)
        self.computed_ranges = merged
