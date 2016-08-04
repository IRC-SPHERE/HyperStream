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
from input import Input
import logging
from ..models.stream_instance import StreamInstance

from sphere_connector_package.sphere_connector.experiment import DataWindow


class RawInput(Input):
    def get_data(self, stream, sphere_connector, time_range):
        logging.debug("Getting data {} (raw input)".format(stream.stream_id))
        data_window = DataWindow(sphere_connector, time_range.start, time_range.end, window_id=stream.stream_id)

        if stream.parameters["modality"] == "annotations":
            modality = data_window.annotations
            data = data_window.annotations.get_data()
        elif stream.parameters["modality"] == "environmental":
            modality = data_window.environmental
            data = data_window.environmental.get_data()
        elif stream.parameters["modality"] == "video":
            modality = data_window.video
            data = data_window.video.get_data(elements={stream.parameters["element"]})
        elif stream.parameters["modality"] == "wearable":
            modality = data_window.wearable
            data = data_window.wearable.get_data(elements={stream.parameters["element"]})
        else:
            raise RuntimeError("Unknown modality for raw data input {}".format(stream.parameters["modality"]))

        # We need to so a small amount of processing to convert the sphere_connector format to the standard format
        return self.convert_to_standard_format(stream, modality, data)

    @staticmethod
    def convert_to_standard_format(stream, modality, data):
        data_standard = []
        for d in data:
            if len(modality.sensors) == 1:
                key = next(iter(modality.sensors))
                if key in d:
                    value = d[key]
                else:
                    raise RuntimeError("Sensor data missing: " + key)
            else:
                value = dict((v, d[v]) for v in modality.sensors)

            instance = StreamInstance(
                stream_id=stream.stream_id,
                stream_type=stream.stream_type,
                filters=stream.filters,
                metadata={"uid": d["uid"], "aid": d["aid"]} if "aid" in d else {"uid": d["uid"]},
                datetime=d["datetime"],
                value=value,
                version=stream.kernel.version
            )

            data_standard.append(instance)

        return {'raw': data_standard}
