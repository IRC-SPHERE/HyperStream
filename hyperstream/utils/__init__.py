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

from utils import MetaDataTree, Hashable, Printable, TypedBiDict, FrozenKeyDict, TypedFrozenKeyDict
from hyperstream_logger import HyperStreamLogger
from time_utils import UTC, MIN_DATE, MAX_DATE, utcnow, get_timedelta, unix2datetime, construct_experiment_id, \
    duration2str, reconstruct_interval
from decorators import timeit, check_output_format, check_tool_defined, check_input_stream_count
from errors import StreamNotAvailableError, StreamAlreadyExistsError, StreamDataNotAvailableError, \
    StreamNotFoundError, IncompatiblePlatesError, ToolNotFoundError, ChannelNotFoundError, ToolExecutionError, \
    PlateEmptyError, PlateDefinitionError, LinkageError, FactorAlreadyExistsError, NodeAlreadyExistsError, \
    FactorDefinitionError, ChannelAlreadyExistsError, NodeDefinitionError, ToolInitialisationError, \
    IncompatibleToolError, MultipleStreamsFoundError, PlateNotFoundError
from serialization import func_dump, func_load
