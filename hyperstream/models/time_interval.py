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

from mongoengine import EmbeddedDocument, DateTimeField, EmbeddedDocumentListField


class TimeIntervalModel(EmbeddedDocument):
    start = DateTimeField(required=True)
    end = DateTimeField(required=True)


class TimeRangeListModel(EmbeddedDocumentListField):
    def add_time_interval(self, time_interval):
        if not isinstance(time_interval, TimeIntervalModel):
            raise RuntimeError("Can only add TimeIntervalModel objects")

        self._data.append(time_interval)
        self.update_time_intervals()
        return self._data

    def update_time_intervals(self):
        sorted_by_lower_bound = sorted(self.value, key=lambda r: r.start)
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
                    merged[-1] = TimeIntervalModel(start=lower.start, end=upper_bound)  # replace by merged interval
                else:
                    merged.append(higher)
        self.value = merged
