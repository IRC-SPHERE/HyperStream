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
