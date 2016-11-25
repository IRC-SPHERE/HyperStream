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

import time
import logging


def timeit(f):
    def timed(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()

        logging.info('func:{} args:[{}, {}] took: {:2.4f} sec'.format(f.__name__, args, kw, te - ts))
        return result

    return timed


def check_output_format(expected_formats):
    """
    Decorator for stream outputs that checks the format of the outputs after modifiers have been applied
    :param expected_formats: The expected output formats
    :type expected_formats: tuple, set
    :return: the decorator
    """
    def output_format_decorator(func):
        def func_wrapper(*args, **kwargs):
            self = args[0]
            if self.output_format not in expected_formats:
                raise ValueError("expected output format {}, got {}".format('doc_gen', self.output_format))
            return func(*args, **kwargs)
        return func_wrapper
    return output_format_decorator


def check_tool_defined(func):
    """
    Decorator to check whether a tool stream has been defined before execution
    :return: the decorator
    """
    def func_wrapper(*args):
        self = args[0]
        # Deferred import to avoid circular dependency
        from ..channels import ToolChannel
        if isinstance(self.channel, ToolChannel) and not self.defined:
            raise RuntimeError("Tool not yet defined")
        return func(*args)
    return func_wrapper


def check_input_stream_count(expected_number_of_streams):
    """
    Decorator for Tool._execute that checks the number of input streams

    :param expected_number_of_streams: The expected number of streams
    :return: the decorator
    """

    def stream_count_decorator(func):
        def func_wrapper(*args, **kwargs):
            self = args[0]
            sources = kwargs['sources'] if 'sources' in kwargs else args[1]
            if expected_number_of_streams == 0:
                if sources:
                    raise ValueError("No input streams expected")
            else:
                given_number_of_streams = len(sources) if sources else 0
                if given_number_of_streams != expected_number_of_streams:
                    raise ValueError("{} tool takes {} stream(s) as input ({} given)".format(
                        self.__class__.__name__, expected_number_of_streams, given_number_of_streams))
            return func(*args, **kwargs)

        return func_wrapper

    return stream_count_decorator
