"""
The MIT License (MIT)
Copyright (c) 2014-2017 University of Bristol

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.
"""
from module_channel import ModuleChannel
from ..stream import StreamInstance


class ToolChannel(ModuleChannel):
    """
    Special case of the file/module channel to load the tools to execute other streams
    """

    # def get_tool(self, tool_name, tool_version, tool_parameters):
    #     if tool_version is not None:
    #         raise NotImplementedError("Can't yet deal with versions")
    #     # return self.get_results(self.streams[StreamId(name=tool_name, meta_data={})])
    #     stream_ref = self.streams[StreamId(name=tool_name)]
    #     (version, module_importer) = super(ToolChannel, self).get_results(stream_ref)
    #     module = module_importer()
    #     class_name = stream_ref.stream_id.name.title().replace("_", "")
    #     tool_class = getattr(module, class_name)
    #     tool = tool_class(**tool_parameters)
    #
    #     # TODO: because of the way this is returned, in order to use a tool the callee needs to do tool.tool.execute
    #     return Stream(
    #         channel=self,
    #         stream_id=StreamId(name=class_name, meta_data={}),
    #         time_interval=stream_ref.time_interval,
    #         calculated_intervals=TimeIntervals(),
    #         modifiers=Identity(),
    #         # get_results_func=None,
    #         tool=tool,
    #         input_streams=stream_ref.input_streams
    #     )

    def get_results(self, stream_ref):
        results = super(ToolChannel, self).get_results(stream_ref)
        for timestamp, (version, module_importer) in results:
            module = module_importer()
            class_name = stream_ref.stream_id.name.title().replace("_", "")
            tool_class = getattr(module, class_name)
            yield StreamInstance(timestamp, tool_class)
