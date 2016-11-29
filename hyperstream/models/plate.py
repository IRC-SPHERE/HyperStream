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
from mongoengine import Document, StringField, ListField, BooleanField, EmbeddedDocument


class PlateDefinitionModel(Document):
    plate_id = StringField(required=True, min_length=1, max_length=512)
    meta_data_id = StringField(required=True, min_length=1, max_length=512)
    description = StringField(required=False, min_length=0, max_length=512, default="")
    values = ListField(field=StringField(min_length=1, max_length=512))
    complement = BooleanField(required=False, default=False)
    parent_plate = StringField(required=False, min_length=1, max_length=512, default=None)

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
