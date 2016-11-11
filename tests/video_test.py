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

from sphere_connector_package.sphere_connector import SphereConnector, DataWindow
from dateutil.parser import parse


OLD = {
    'start': parse("2016-07-08T12:00:00.000Z"),
    'end': parse("2016-07-08T12:01:00.000Z")
}

NEW = {
    'start': parse("2016-04-28T12:00:00.000Z"),
    'end': parse("2016-04-28T12:01:00.000Z")
}


if __name__ == '__main__':
    sphere_connector = SphereConnector(config_filename='config_strauss.json', include_mongo=True, include_redcap=False)

    windows = [DataWindow(sphere_connector, **d) for d in [OLD, NEW]]

    for element in ["2Dbb", "silhouette"]:
        for dw in windows:
            data = dw.video.get_data(elements={element}, filters={'bt': {'$mod': [10, 0]}}, rename_keys=False)
            print("")
            print(data[0].keys())
            print(data[0]['video-' + element])
            print(len(data))
