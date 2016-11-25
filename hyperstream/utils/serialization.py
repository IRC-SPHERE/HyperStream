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

import types
import marshal


def func_dump(func):
    """
    Serialize user defined function.
    :param func: The function
    :return: Tuple of code, defaults and closure
    """
    code = marshal.dumps(func.func_code)
    defaults = func.__defaults__
    if func.__closure__:
        closure = tuple(c.cell_contents for c in func.__closure__)
    else:
        closure = None
    return code, defaults, closure


def func_load(code, defaults=None, closure=None, globs=None):
    """
    Reload a function
    :param code: The code object
    :param defaults: Default values
    :param closure: The closure
    :param globs: globals
    :return:
    """
    if isinstance(code, (tuple, list)):  # unpack previous dump
        code, defaults, closure = code
    code = marshal.loads(code)
    if closure is not None:
        closure = func_reconstruct_closure(closure)
    if globs is None:
        globs = globals()
    return types.FunctionType(code, globs, name=code.co_name, argdefs=defaults, closure=closure)


def func_reconstruct_closure(values):
    """
    Deserialization helper that reconstructs a closure
    :param values: The closure values
    :return: The closure
    """
    nums = range(len(values))
    src = ["def func(arg):"]
    src += ["  _%d = arg[%d]" % (n, n) for n in nums]
    src += ["  return lambda:(%s)" % ','.join(["_%d" % n for n in nums]), ""]
    src = '\n'.join(src)
    try:
        exec(src, globals())
    except:
        raise SyntaxError(src)
    return func(values).__closure__
