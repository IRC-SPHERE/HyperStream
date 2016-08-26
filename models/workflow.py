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

from mongoengine import Document, StringField, EmbeddedDocument, EmbeddedDocumentListField, ListField, DateTimeField, \
    BooleanField, DynamicField
from time_range import TimeRangeModel


class PlateDefinitionModel(Document):
    plate_id = StringField(required=True, min_length=1, max_length=512)
    meta_data_id = StringField(required=True, min_length=1, max_length=512)
    description = StringField(required=False, min_length=0, max_length=512, default="")
    # TODO: Really want to say either int or str but nothing else
    values = ListField()  # field=IntField(min_value=0))
    complement = BooleanField(required=False, default=False)
    parent_plate = StringField(required=False, min_length=1, max_length=512, default="")

    meta = {
        'collection': 'plate_definitions',
        'indexes': [{'fields': ['plate_id'], 'unique': True}],
        'ordering': ['plate_id']
    }


class PlateModel(EmbeddedDocument):
    meta_data_id = StringField(required=True, min_length=1, max_length=512)
    # TODO: Really want to say either int or str but nothing else
    values = ListField()  # field=IntField(min_value=0))
    complement = BooleanField(default=False)


class NodeDefinitionModel(EmbeddedDocument):
    stream_name = StringField(required=True, min_length=1, max_length=512)
    plate_ids = ListField(field=StringField(min_length=1, max_length=512))


class FactorDefinitionModel(EmbeddedDocument):
    tool = StringField(required=True, min_length=1, max_length=512)
    sources = ListField(field=StringField(min_length=1, max_length=512), required=False)
    sink = StringField(min_length=1, max_length=512, required=False)


class WorkflowDefinitionModel(Document):
    workflow_id = StringField(required=True, min_length=1, max_length=512)
    name = StringField(required=True, min_length=1, max_length=512)
    description = StringField(required=True, min_length=1, max_length=4096)
    nodes = EmbeddedDocumentListField(document_type=NodeDefinitionModel, required=False)
    nodes_old = DynamicField(required=False)
    # plates = MapField(field=EmbeddedDocumentListField(document_type=PlateDefinitionModel))
    factors = EmbeddedDocumentListField(document_type=FactorDefinitionModel, required=True)
    owner = StringField(required=False, min_length=1, max_length=512)

    meta = {
        'collection': 'workflow_definitions',
        'indexes': [{'fields': ['workflow_id']}],
        'ordering': ['workflow_id']
    }


class WorkflowStatusModel(Document):
    stream_id = StringField(required=True, min_length=1, max_length=512)
    stream_type = StringField(required=True, min_length=1, max_length=512)
    last_updated = DateTimeField(required=True)
    last_accessed = DateTimeField(required=False)

    # Time ranges requested for computation for this workflow
    requested_ranges = EmbeddedDocumentListField(document_type=TimeRangeModel, required=True)

    # Actual ranges that have been computed. Note that this can be empty to begin with
    computed_ranges = EmbeddedDocumentListField(document_type=TimeRangeModel, required=False)

    meta = {
        'collection': 'workflow_status',
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
