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
from datetime import timedelta, datetime
from mongoengine import connect, EmbeddedDocument
from mongoengine import Document, DateTimeField, StringField, EmbeddedDocumentListField, IntField, EmbeddedDocumentField
import logging
# from collections import defaultdict
import pytz


# class Instance(Document):
#     streamId = StringField(required=True, min_length=1, max_length=512),
#     streamType = StringField(required=True, min_length=1, max_length=512),
#     filters = EmbeddedDocumentField(required=True),
#     version = StringField(required=True, min_length=1, max_length=512),
#     value = EmbeddedDocumentField(required=True)
#
#     meta = {
#         'collection': 'streams',
#         'indexes': [{'fields': ['streamId']}],
#         'ordering': ['start']
#     }


class Interface(object):
    def __init__(self): #, session, batch_input_function, iterable_input_function, batch_output_function, iterable_output_function):
        pass