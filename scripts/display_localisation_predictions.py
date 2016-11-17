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

from __future__ import print_function

import arrow
import logging
from datetime import timedelta
from time import sleep


def run(house, wearables):
    from hyperstream import HyperStream, StreamId, TimeInterval
    from hyperstream.utils import utcnow

    hyperstream = HyperStream(loglevel=logging.CRITICAL)

    # Various channels
    D = hyperstream.channel_manager.mongo

    time_interval = TimeInterval(start=utcnow() - timedelta(seconds=10), end=utcnow())

    for wearable in wearables:
        stream_id = StreamId('predicted_locations_broadcasted', meta_data=dict(house=house, wearable=wearable))
        predictions = D[stream_id].window(time_interval).last()

        if predictions:
            print("Wearable {}:{}\t{}".format(
                wearable,
                predictions.timestamp,
                arrow.get(predictions.timestamp).humanize()))
            for k in sorted(predictions.value):
                print("{}:\t{}".format(k, predictions.value[k]))
        else:
            print("No predictions in interval {} for wearable {}".format(time_interval, wearable))

    print()

if __name__ == '__main__':
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    wearables = 'ABCD'

    if len(sys.argv) > 1:
        try:
            wearables = sys.argv[1]
        except ValueError:
            pass

    house = 1

    while True:
        run(house, wearables)
        sleep(1)
