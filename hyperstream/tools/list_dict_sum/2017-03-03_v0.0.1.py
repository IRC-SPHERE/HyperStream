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

from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count
import logging


class ListDictSum(Tool):
    """
    Take the component-wise mean of dicts in the list
    """
    def __init__(self,start_dict=None,log_new_keys=True,insert_new_keys=True):
        super(ListDictSum, self).__init__(start_dict=start_dict,log_new_keys=log_new_keys,insert_new_keys=insert_new_keys)

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        for time, data in sources[0].window(interval, force_calculation=True):
            dict_sum = self.start_dict
            if dict_sum is None:
                dict_sum = dict()
            if len(data)==0:
                yield StreamInstance(time, dict_sum)
            for item in data:
                for key in item.keys():
                    try:
                        dict_sum[key] = dict_sum[key] + item[key]
                    except KeyError:
                        if self.log_new_keys:
                            logging.warn('Novel key in ListDictSum: {}'.format(key))
                        if self.insert_new_keys:
                            dict_sum[key] = item[key]
            yield StreamInstance(time, dict_sum)
