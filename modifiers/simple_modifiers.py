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

from modifier import Modifier
from ..stream import StreamInstance


# class Identity(Modifier):
#     def __init__(self):
#         super(Identity, self).__init__()
#         self.types = {'data_gen': 'data_gen', 'doc_gen': 'doc_gen', 'data': 'data', 'doc': 'doc'}
#
#     def execute(self, data):
#         return data


# class First(Modifier):
#     def __init__(self):
#         super(First, self).__init__()
#         self.types = {'data_gen': 'data', 'doc_gen': 'doc'}
#
#     def execute(self, data):
#         for d in data:
#             return d
#         return None
#
#
# class Last(Modifier):
#     def __init__(self):
#         super(Last, self).__init__()
#         self.types = {'data_gen': 'data', 'doc_gen': 'doc'}
#
#     def execute(self, data):
#         res = None
#         for d in data:
#             res = d
#         return res
#
#
# class Head(Modifier):
#     def __init__(self, n):
#         super(Head, self).__init__()
#         self.types = {'data_gen': 'data_gen', 'doc_gen': 'doc_gen'}
#         self.n = n
#
#     def execute(self, data):
#         i = 0
#         for d in data:
#             i += 1
#             if i > self.n:
#                 break
#             yield d
#
#
# class Tail(Modifier):
#     def __init__(self, n):
#         super(Tail, self).__init__()
#         self.types = {'data_gen': 'data_gen', 'doc_gen': 'doc_gen'}
#         self.n = n
#
#     def execute(self, data):
#         tail = ()
#         for d in data:
#             tail = (tail + (d,))[-self.n:]
#         for d in tail:
#             yield d


# class Data(Modifier):
#     def __init__(self):
#         super(Data, self).__init__()
#         self.types = {'doc_gen': 'data_gen'}
#
#     def execute(self, doc_gen):
#         for (time, data) in doc_gen:
#             yield data


class Time(Modifier):
    def __init__(self):
        super(Time, self).__init__()
        self.types = {'doc_gen': 'data_gen'}

    def execute(self, doc_gen):
        for (time, data) in doc_gen:
            yield time


class IData(Modifier):
    def __init__(self):
        super(IData, self).__init__()
        self.types = {'doc': 'data'}

    def execute(self, doc):
        (time, data) = doc
        return data


class ITime(Modifier):
    def __init__(self):
        super(ITime, self).__init__()
        self.types = {'doc': 'data'}

    def execute(self, doc):
        (time, data) = doc
        return time


# class List(Modifier):
#     def __init__(self):
#         super(List, self).__init__()
#         self.types = {'data_gen': 'data', 'doc_gen': 'data'}
#
#     def execute(self, data):
#         return list(data)


class Component(Modifier):
    def __init__(self, key):
        super(Component, self).__init__()
        self.types = {'doc_gen': 'doc_gen'}
        self.key = key

    def execute(self, doc_gen):
        for (time, data) in doc_gen:
            if self.key in data:
                yield StreamInstance(time, data[self.key])


class ComponentFilter(Modifier):
    def __init__(self, key, values):
        super(ComponentFilter, self).__init__()
        self.types = {'doc_gen': 'doc_gen'}
        self.key = key
        self.values = values

    def execute(self, doc_gen):
        for (time, data) in doc_gen:
            if data[self.key] in self.values:
                yield StreamInstance(time, data)
                
            # TODO: this try/excelt seems unnecessary
            # try:
            #     if data[self.key] in self.values:
            #         yield StreamInstance(time, data)
            # except KeyError:
            #     pass


class DeleteNones(Modifier):
    def __init__(self):
        super(DeleteNones, self).__init__()
        self.types = {'doc_gen': 'doc_gen'}

    def execute(self, doc_gen):
        for (time, data) in doc_gen:
            data2 = {}
            for (key, value) in data.items():
                if value is not None:
                    data2[key] = value
            yield StreamInstance(time, data2)


# class Sum(Modifier):
#     def __init__(self, first=0):
#         super(Sum, self).__init__()
#         self.types = {'data_gen': 'data'}
#         self.first = first
#
#     def execute(self, data_gen):
#         res = self.first
#         for data in data_gen:
#             res = res + data
#         return res


# class Product(Modifier):
#     def __init__(self, first=1):
#         super(Product, self).__init__()
#         self.types = {'data_gen': 'data'}
#         self.first = first
#
#     def execute(self, data_gen):
#         res = self.first
#         for data in data_gen:
#             res = res * data
#         return res


# class Average(Modifier):
#     def __init__(self):
#         super(Average, self).__init__()
#         self.types = {'data_gen': 'data'}
#
#     def execute(self, data_gen):
#         res = 0
#         count = 0
#         for data in data_gen:
#             res = res + data
#             count += 1
#         return float(res) / count if count > 0 else float('nan')


# class Count(Modifier):
#     def __init__(self):
#         super(Count, self).__init__()
#         self.types = {'data_gen': 'data'}
#
#     def execute(self, data_gen):
#         count = 0
#         for _ in data_gen:
#             count += 1
#         return count
