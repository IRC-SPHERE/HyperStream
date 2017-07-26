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

from hyperstream import MultiOutputTool, StreamInstance, StreamMetaInstance

from dateutil.parser import parse


class CsvMultiReader(MultiOutputTool):
    def __init__(self, filename_template, datetime_parser=parse,
                 datetime_column=0, skip_rows=0, header=True):
        super(CsvMultiReader, self).__init__(
            filename_template=filename_template,
            datetime_parser=datetime_parser,
            datetime_column=datetime_column,
            skip_rows=skip_rows,
            header=header
        )

    def _execute(self, source, splitting_stream, interval, meta_data_id, output_plate_values):

        # Let's make the assumption that the first field is the timestamp

        for pv in output_plate_values:
            count = 0

            if len(pv) > 1:
                raise NotImplementedError("Nested plates not supported for this tool")

            # Unpack the plate value
            ((meta_data_id, plate_value),) = pv

            filename = self.filename_template.format(plate_value)

            with open(filename, 'rU') as f:
                for line in f.readlines():
                    if count == 0:
                        if self.header:
                            colnames = [name.replace('\n', '') for name in line.split(',')]
                            del colnames[self.datetime_column]
                        count += 1
                        continue
                    count += 1
                    if count < self.skip_rows + 1:
                        continue
                    elements = line.split(',')
                    dt = self.datetime_parser(elements[self.datetime_column])
                    del elements[self.datetime_column]
                    if dt in interval:
                        if self.header:
                            values = dict(zip(colnames, map(float, elements)))
                        else:
                            values = map(float, elements)
                        instance = StreamInstance(dt, values)
                        yield StreamMetaInstance(instance, (meta_data_id, plate_value))
