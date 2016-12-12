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


class StreamNotFoundError(Exception):
    pass


class MultipleStreamsFoundError(Exception):
    pass


class StreamAlreadyExistsError(Exception):
    pass


class StreamDataNotAvailableError(Exception):
    pass


class ToolNotFoundError(Exception):
    pass


class ToolInitialisationError(Exception):
    pass


class IncompatibleToolError(Exception):
    pass


class ChannelNotFoundError(Exception):
    pass


class ChannelAlreadyExistsError(Exception):
    pass


class IncompatiblePlatesError(Exception):
    pass


class StreamNotAvailableError(Exception):
    message = 'The stream is not available after {} and cannot be calculated'

    def __init__(self, up_to_timestamp):
        super(StreamNotAvailableError, self).__init__(self.message.format(up_to_timestamp))


class ToolExecutionError(Exception):
    message = 'Tool execution did not cover the time interval {}.'

    def __init__(self, required_intervals):
        super(ToolExecutionError, self).__init__(self.message.format(required_intervals))


class PlateEmptyError(Exception):
    message = 'Plate values for {} empty'

    def __init__(self, plate_id):
        super(PlateEmptyError, self).__init__(self.message.format(plate_id))


class PlateDefinitionError(Exception):
    message = "Empty values in plate definition and complement=False"


class PlateNotFoundError(Exception):
    pass


class LinkageError(Exception):
    pass


class NodeAlreadyExistsError(Exception):
    message = "Cannot have duplicate nodes"


class NodeDefinitionError(Exception):
    pass


class FactorDefinitionError(Exception):
    pass


class FactorAlreadyExistsError(Exception):
    message = "Cannot have duplicate factors - a new factor object should be created"
