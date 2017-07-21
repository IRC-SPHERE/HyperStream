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
from collections import Counter
from bisect import bisect_left, bisect_right


def argsort(seq):
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)


def diff(a, n=1):
    """
    Calculate the n-th discrete difference along given axis.
    The first difference is given by ``out[n] = a[n+1] - a[n]`` along
    the given axis, higher differences are calculated by using `diff`
    recursively.

    :param a: The list to calculate the diff on
    :param n: The order of the difference
    :type a: list | tuple
    :type n: int
    :return: THe array of nth order differences
    """
    if n == 0:
        return a
    if n < 0:
        raise ValueError("order must be non-negative but got " + repr(n))

    b = map(lambda x: x[1] - x[0], zip(a[:-1], a[1:]))

    if n > 1:
        return diff(b, n-1)
    return b


def accumulate(lis):
    """
    Cumulative sum generator

    :param lis: This list to accumulate
    :return: The sum
    """
    total = 0
    for x in lis:
        total += x
        yield total


def histogram(a, bins):
    """
    Compute the histogram of a set of data.

    :param a: Input data
    :param bins: int or sequence of scalars or str, optional
    :type a: list | tuple
    :type bins: int | list[int] | list[str]
    :return:
    """
    if any(map(lambda x: x < 0, diff(bins))):
        raise ValueError(
            'bins must increase monotonically.')

    try:
        sa = sorted(a)
    except TypeError:
        # Perhaps just a single value? Treat as a list and carry on
        sa = sorted([a])

    # import numpy as np
    # nl = np.searchsorted(sa, bins[:-1], 'left')
    # nr = np.searchsorted(sa, bins[-1], 'right')
    # nn = np.r_[nl, nr]
    #
    # # cl = list(accumulate(Counter(map(lambda x: bisect_left(bins[:-1], x), sa)))
    # # print("cl")
    # # print([cl[i] for i in range(len(bins))])
    # print("nl")
    # print(list(nl))
    # # print(Counter(map(lambda x: bisect_right([bins[-1]], x), sa)))
    # print("nr")
    # print([nr])
    # print("nn")
    # print(list(nn))
    # print("hist")
    # print(list(np.diff(nn)))
    # print(list(np.histogram(a, bins)[0]))

    nl = list(accumulate([Counter(map(lambda x: bisect_left(bins[:-1], x), sa))[i] for i in range(len(bins) - 1)]))
    # print("nl")
    # print(nl)
    nr = Counter(map(lambda x: bisect_right([bins[1]], x), sa))[1]
    # print(nl)
    # print(nr)
    n = list(nl) + [nr]

    return diff(n), bins
