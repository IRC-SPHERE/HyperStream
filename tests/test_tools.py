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

import unittest

from hyperstream import HyperStream, Stream, StreamId, TimeInterval
from hyperstream.utils import MIN_DATE, utcnow
from helpers import *

RANDOM_VALUES = [
    0.9664535356921388, 0.4407325991753527, 0.007491470058587191, 0.9109759624491242, 0.939268997363764,
    0.5822275730589491, 0.6715634814879851, 0.08393822683708396, 0.7664809327917963, 0.23680977536311776,
    0.030814021726609964, 0.7887727172362835, 0.3460889655971231, 0.6232814750391685, 0.6158156951036152,
    0.14855463870828756, 0.18309064740993164, 0.11441296968868764, 0.014618780486909122, 0.48675154060475834,
    0.9649015609162157, 0.06456228097718608, 0.5410881855511303, 0.46589855900830957, 0.6014634495610515,
    0.08892882999066232, 0.5790026861873665, 0.26958550381944824, 0.5564325605562156, 0.6446342341782827,
    0.48103637136651844, 0.35523914744298335, 0.249152121361209, 0.9335154980423467, 0.45338801947649354,
    0.5301612069115903, 0.019299566309716853, 0.5081019257797922, 0.005780237417743139, 0.14376842759559538,
    0.47282692534740633, 0.3773474407725964, 0.05417519864614284, 0.5875285081310694, 0.1640032237419612,
    0.5573302374414681, 0.1442457216019083, 0.9373070846962247, 0.7709799715197749, 0.9569331223494054,
    0.14122776441649953, 0.3053927082876986, 0.03958962422796164, 0.27678369479169207, 0.8065125051156038,
    0.177343035278254, 0.15457051471078964, 0.9547186557023949, 0.154551400089043, 0.8338892941512318
]

SEA_ICE_SUMS = [
    15.48, 16.15, 17.259999999999998, 18.21, 19.38, 18.61, 18.869999999999997, 18.8, 20.380000000000003, 20.82, 17.96,
    15.88, 15.16, 15.89, 17.89, 19.21, 19.47, 18.78, 19.1, 18.84, 21.4, 20.54, 17.67, 15.66, 15.3, 15.8, 17.67, 18.9,
    19.79, 19.62, 19.43, 19.41, 21.45, 20.78, 17.66, 15.28, 14.84, 15.860000000000001, 17.77, 19.15, 19.479999999999997,
    18.8, 18.48, 19.14, 21.07, 21.21, 17.9, 16.060000000000002, 15.63, 16.6, 18.22, 19.72, 20.25, 19.88,
    19.490000000000002, 19.6, 21.15, 21.29, 18.38, 16.34, 15.420000000000002, 16.02, 17.67, 18.990000000000002,
    19.229999999999997, 18.52, 18.77, 18.8, 19.380000000000003, 20.33, 18.05, 15.5, 14.59, 15.39, 17.51, 19.79, 20.78,
    20.15, 19.450000000000003, 20.03, 21.34, 19.84, 17.0, 14.950000000000001, 15.02, 15.47, 17.240000000000002,
    18.990000000000002, 19.259999999999998, 19.07, 19.03, 19.33, 20.4, 20.490000000000002, 18.03, 15.64, 15.51, 16.14,
    17.8, 19.17, 19.36, 18.94, 18.59, 19.0, 21.1, 20.58, 17.46, 15.53, 15.08, 15.98, 18.13, 19.6, 19.92, 19.46, 18.82,
    18.75, 20.9, 20.61, 17.55, 15.010000000000002, 14.649999999999999, 15.7, 17.740000000000002, 19.65,
    19.939999999999998, 19.2, 19.26, 19.29, 20.8, 20.520000000000003, 17.67, 15.82, 15.52, 16.87, 18.21,
    19.240000000000002, 19.48, 18.71, 18.53, 18.82, 19.83, 19.9, 16.65, 15.0, 15.11, 15.649999999999999, 16.84, 18.2,
    18.73, 18.759999999999998, 17.880000000000003, 17.98, 19.93, 20.240000000000002, 18.05, 16.22, 15.76, 16.52, 17.86,
    19.17, 20.12, 19.14, 18.1, 18.060000000000002, 19.71, 19.86, 17.77, 15.879999999999999, 15.149999999999999, 16.03,
    17.41, 18.95, 19.98, 19.5, 18.72, 18.86, 20.38, 20.84, 17.67, 14.86, 14.18, 15.33, 17.28, 18.7, 18.939999999999998,
    18.44, 18.0, 18.79, 19.63, 20.42, 16.63, 14.24, 14.0, 14.379999999999999, 16.17, 17.47, 18.83, 18.5,
    18.689999999999998, 19.06, 20.32, 19.57, 16.38, 14.66, 14.41, 14.97, 16.37, 18.150000000000002, 18.54, 17.55, 17.05,
    18.07, 18.71, 20.03, 18.39, 15.8, 15.13, 16.79, 18.56, 19.34, 19.78, 18.5, 17.39, 17.2, 19.4, 20.509999999999998,
    17.75, 15.24, 14.71, 15.979999999999999, 18.4, 19.4, 19.83, 18.66, 18.08, 18.34, 19.04, 19.62, 17.04, 14.75,
    14.549999999999999, 15.69, 17.31, 18.67, 19.490000000000002, 18.81, 18.32, 17.26, 19.39, 19.810000000000002,
    16.689999999999998, 14.25, 14.120000000000001, 14.58
]


class TestTools(unittest.TestCase):
    def test_data_generators(self):
        hs = HyperStream()

        ti = TimeInterval.now_minus(minutes=1)

        M = hs.channel_manager.memory

        random = M.get_or_create_stream("memory")
        ticker = M.get_or_create_stream("ticker")

        clk = hs.tools.clock()
        rng = hs.plugins.data_generators.tools.random(seed=1234)

        clk.execute(sources=[], sink=ticker, interval=ti)
        rng.execute(sources=[], sink=random, interval=ti, alignment_stream=ticker)

        for item in random.window().items():
            print(item)

        self.assertListEqual(random.window().values(), RANDOM_VALUES)

    def test_data_importers(self):
        hs = HyperStream()
        reader = hs.plugins.data_importers.tools.csv_reader('plugins/data_importers/data/sea_ice.csv')
        ti = TimeInterval(datetime(1990, 1, 1).replace(tzinfo=UTC), datetime(2011, 4, 1).replace(tzinfo=UTC))

        sea_ice = hs.channel_manager.memory.get_or_create_stream("sea_ice")

        reader.execute(sources=[], sink=sea_ice, interval=ti)

        sea_ice_sums = hs.channel_manager.mongo.get_or_create_stream("sea_ice_sums")
        hs.tools.list_sum().execute(sources=[sea_ice], sink=sea_ice_sums, interval=ti)

        print(sea_ice_sums.window().values())

        self.assertListEqual(sea_ice_sums.window().values(), map(sum, sea_ice.window().values()))
