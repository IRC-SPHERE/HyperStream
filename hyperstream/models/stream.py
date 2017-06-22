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

from mongoengine import Document, DateTimeField, StringField, DynamicField, EmbeddedDocumentListField, \
    EmbeddedDocument, EmbeddedDocumentField, ListField
from .time_interval import TimeIntervalModel
from ..time_interval import TimeInterval, TimeIntervals


class StreamIdField(EmbeddedDocument):
    name = StringField(required=True, min_length=1, max_length=512)
    meta_data = ListField(required=False)


class StreamInstanceModel(Document):
    stream_id = EmbeddedDocumentField(document_type=StreamIdField, required=True)
    stream_type = StringField(required=False, min_length=1, max_length=512)
    datetime = DateTimeField(required=True)
    # tool_version = StringField(required=True, min_length=1, max_length=512)
    value = DynamicField(required=True)
    
    meta = {
        'collection': 'streams',
        'indexes': [
            {'fields': ['stream_id']},
            {'fields': ['stream_id', 'datetime'], 'unique': True}
        ],
        'ordering': ['datetime']
    }


# TODO: This needs to be removed and put into plugins
class SummaryInstanceModel(Document):
    stream_id = EmbeddedDocumentField(document_type=StreamIdField, required=True)
    stream_type = StringField(required=False, min_length=1, max_length=512)
    datetime = DateTimeField(required=True)
    # tool_version = StringField(required=True, min_length=1, max_length=512)
    value = DynamicField(required=True)

    meta = {
        'collection': 'summaries',
        'indexes': [
            {'fields': ['stream_id']},
            {'fields': ['stream_id', 'datetime'], 'unique': True}
        ],
        'ordering': ['datetime']
    }


class StreamDefinitionModel(Document):
    stream_id = EmbeddedDocumentField(document_type=StreamIdField, required=True)  # , unique=True)
    stream_type = StringField(required=False, min_length=1, max_length=512)
    channel_id = StringField(required=True, min_length=1, max_length=512)
    last_updated = DateTimeField(required=False)
    last_accessed = DateTimeField(required=False)
    calculated_intervals = EmbeddedDocumentListField(document_type=TimeIntervalModel, required=False)
    sandbox = StringField()

    meta = {
        'collection': 'stream_definitions',
        'indexes': [{'fields': ['stream_id'], 'unique': True}],
    }

    def get_calculated_intervals(self):
        return TimeIntervals(map(lambda x: TimeInterval(x.start, x.end), self.calculated_intervals))

    def set_calculated_intervals(self, intervals):
        if intervals is None:
            intervals = []
        self.calculated_intervals = tuple(map(lambda x: TimeIntervalModel(start=x.start, end=x.end), intervals))

# class StreamStatusModel(Document):
#     """
#     Stream status model
#     Note that the calculated intervals is not required, since at first instantiation it is empty, so is equally
#     represented by None or an empty list
#     """
#     stream_id = EmbeddedDocumentField(document_type=StreamIdField, required=True)  # , unique=True)
#     last_updated = DateTimeField(required=True)
#     last_accessed = DateTimeField(required=False)
#     calculated_intervals = EmbeddedDocumentListField(document_type=TimeIntervalModel, required=False)
#
#     meta = {
#         'collection': 'stream_status',
#         'indexes': [{'fields': ['stream_id'], 'unique': True}],
#         'ordering': ['last_updated']
#     }
