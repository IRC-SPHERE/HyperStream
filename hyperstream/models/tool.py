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
from mongoengine import StringField, EmbeddedDocument, BooleanField, EmbeddedDocumentListField, DynamicField


class ToolParameterModel(EmbeddedDocument):
    key = StringField(required=True, min_length=1, max_length=512)
    value = DynamicField(required=False)  # This is false to allow None as a parameter value
    is_function = BooleanField(required=True, default=False)
    is_set = BooleanField(required=True, default=False)


class ToolModel(EmbeddedDocument):
    name = StringField(required=True, min_length=1, max_length=512)
    version = StringField(required=True, min_length=1, max_length=512)
    parameters = EmbeddedDocumentListField(document_type=ToolParameterModel, required=False)  # To allow empty lists
