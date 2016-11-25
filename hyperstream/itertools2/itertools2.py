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

from ..stream import StreamInstance


def count(data):
    cnt = 0
    for _ in data:
        cnt += 1
    return cnt


def any_set(data):
    for dt, val in data:
        if val:
            return True
        
    return False


def online_sum(data, total=0.0):
    for x in data:
        total += x.value if isinstance(x, StreamInstance) else x
    return total


def online_product(data, total=1.0):
    for x in data:
        total *= x.value if isinstance(x, StreamInstance) else x
    return total


def online_average(data, n=0, mean=0.0):
    for x in data:
        n += 1
        delta = (x.value if isinstance(x, StreamInstance) else x) - mean
        mean += delta / n
    
    # TODO from niall: Possibly unnecessary bug: np.mean([1]) = 1. Suggest (n < 1)
    if n < 1:
        return float('nan')
    else:
        return mean


def online_variance(data, n=0, mean=0.0, m2=0.0):
    for x in data:
        n += 1
        x = x.value if isinstance(x, StreamInstance) else x
        delta = x - mean
        mean += delta / n
        m2 += delta * (x - mean)
    
    if n < 2:
        return float('nan')
    else:
        return m2 / (n - 1)
