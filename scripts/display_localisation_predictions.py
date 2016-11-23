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

import os
import arrow
import logging
from datetime import timedelta
from time import sleep

globs = {
    'sphere_connector': None,
    'hyperstream': None,
    'wearables': 'ABCD',
    'house': 1
}


def display_predictions(hyperstream, house, wearables):
    from hyperstream import StreamId, TimeInterval
    from hyperstream.utils import utcnow

    time_interval = TimeInterval(start=utcnow() - timedelta(seconds=10), end=utcnow())

    for wearable in wearables:
        stream_id = StreamId('predicted_locations_broadcasted', meta_data=dict(house=house, wearable=wearable))
        predictions = hyperstream.channel_manager.mongo[stream_id].window(time_interval).last()

        if predictions:
            print("Wearable {}:{}\t{}".format(
                wearable,
                predictions.timestamp,
                arrow.get(predictions.timestamp).humanize()))
            for k in sorted(predictions.value):
                print("{:>20}:\t{:0.2f}".format(k, predictions.value[k]))
        else:
            print("No predictions in interval {} for wearable {}".format(time_interval, wearable))


def display_access_points():
    from hyperstream.utils import utcnow
    from sphere_connector_package.sphere_connector import SphereConnector

    if not globs['sphere_connector']:
        globs['sphere_connector'] = SphereConnector(
            config_filename='config.json',
            include_mongo=True,
            include_redcap=False,
            sphere_logger=None)

    sphere_connector = globs['sphere_connector']

    dtf = sphere_connector.basic_config.mongo['modalities']['wearable4']['date_time_field']
    aids = sphere_connector.client.collections['wearable4'] \
        .find({dtf: {'$gt': utcnow() - timedelta(seconds=5)}}).distinct('aid')

    if aids:
        print("Access points: ")
        for i, aid in enumerate(aids):
            print("{}: {}".format(i, aid))
    else:
        print("No access points found")


def run(loglevel=logging.CRITICAL):
    from hyperstream import HyperStream
    globs['hyperstream'] = HyperStream(loglevel=loglevel)

    display_predictions(globs['hyperstream'], globs['house'], globs['wearables'])
    print()

    display_access_points()
    print()
    # db.getCollection('WEARABLE-ISO-TIME').distinct('aid',{wts:{$gt:ISODate("2016-11-17T16:40")})


if __name__ == '__main__':
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.append(path)

    if len(sys.argv) > 1:
        try:
            globs['wearables'] = sys.argv[1]
        except ValueError:
            pass

    while True:
        run()
        sleep(1)
