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

import math


def flatten(a):
    if not a:
        return a
    if isinstance(a[0], list):
        return flatten(a[0]) + flatten(a[1:])
    return a[:1] + flatten(a[1:])


def percentile(a, q):
    """
    Compute the qth percentile of the data along the specified axis.
    Simpler version than the numpy version that always flattens input arrays.

    Examples
    --------
    >>> a = [[10, 7, 4], [3, 2, 1]]
    >>> percentile(a, 20)
    2.0
    >>> percentile(a, 50)
    3.5
    >>> percentile(a, [20, 80])
    [2.0, 7.0]
    >>> a = list(range(40))
    >>> percentile(a, 25)
    9.75

    :param a: Input array or object that can be converted to an array.
    :param q: Percentile to compute, which must be between 0 and 100 inclusive.
    :return: the qth percentile(s) of the array elements.
    """
    if not a:
        return None

    if isinstance(q, (float, int)):
        qq = [q]
    elif isinstance(q, (tuple, list)):
        qq = q
    else:
        raise ValueError("Quantile type {} not understood".format(type(q)))

    if isinstance(a, (float, int)):
        a = [a]

    for i in range(len(qq)):
        if qq[i] < 0. or qq[i] > 100.:
            raise ValueError("Percentiles must be in the range [0,100]")
        qq[i] /= 100.

    a = sorted(flatten(a))
    r = []

    for q in qq:
        k = (len(a) - 1) * q
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            r.append(float(a[int(k)]))
            continue
        d0 = a[int(f)] * (c - k)
        d1 = a[int(c)] * (k - f)

        r.append(float(d0 + d1))

    if len(r) == 1:
        return r[0]
    return r
