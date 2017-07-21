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
from mongoengine import EmbeddedDocument, EmbeddedDocumentField, ListField, StringField, BooleanField

from .tool import ToolModel


class OutputPlateDefinitionModel(EmbeddedDocument):
    plate_id = StringField(min_length=1, max_length=512, required=True)
    meta_data_id = StringField(min_length=1, max_length=512, required=True)
    description = StringField(min_length=1, max_length=512, required=True)
    use_provided_values = BooleanField(required=True)


class FactorDefinitionModel(EmbeddedDocument):
    tool = EmbeddedDocumentField(document_type=ToolModel, required=True)
    factor_type = StringField(required=True)
    sources = ListField(field=StringField(min_length=1, max_length=512), required=False)
    sinks = ListField(field=StringField(min_length=1, max_length=512), required=False)
    alignment_node = StringField(min_length=1, max_length=512, required=False)
    splitting_node = StringField(min_length=1, max_length=512, required=False)
    output_plate = EmbeddedDocumentField(document_type=OutputPlateDefinitionModel, required=False)
