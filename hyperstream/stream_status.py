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
from mongoengine import Document, DateTimeField, StringField, DictField, EmbeddedDocument, EmbeddedDocumentListField
# from ..utils import Printable


class DateTimeRange(EmbeddedDocument):
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
