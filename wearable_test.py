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


RNG = {
    'start': parse("2016-04-28T20:00:00.000Z"),
    'end': parse("2016-04-28T20:01:00.000Z")
}


if __name__ == '__main__':
    sphere_connector = SphereConnector(config_filename='config_strauss.json', include_mongo=True, include_redcap=False)

    window = DataWindow(sphere_connector, **RNG)

    # for element in ["RSS"]:
    data = window.wearable.get_data()
    print("")
    print(data[0].keys())
    # print(data[0]['wearable-' + element])
    print(len(data))
