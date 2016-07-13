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
from instance import Instance

from sphere_connector_package.sphere_connector.config import ExperimentConfig
from sphere_connector_package.sphere_connector.experiment import Experiment


class RawInput(Input):
    def get_data(self, stream, clients, configs):
        logging.debug("Getting data {} (raw input)".format(stream.stream_id))
        # logging.debug(stream.parameters)
        # logging.debug(stream.scope)
        # logging.debug(stream.sources)
        experiment_config = ExperimentConfig(experiment_start=stream.scope.start, experiment_end=stream.scope.end,
                                             experiment_id=stream.stream_id)

        client = clients['sphere']
        basic_config = configs['basic_config']
        experiment = Experiment(client, experiment_config, basic_config)

        if stream.parameters["modality"] == "annotations":
            modality = experiment.annotations
            data = experiment.annotations.get_data()
        elif stream.parameters["modality"] == "environmental":
            modality = experiment.environmental
            data = experiment.environmental.get_data()
        elif stream.parameters["modality"] == "video":
            modality = experiment.video
            data = experiment.video.get_data(elements={stream.parameters["element"]})
        elif stream.parameters["modality"] == "wearable":
            modality = experiment.wearable
            data = experiment.wearable.get_data(elements={stream.parameters["element"]})
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

            instance = Instance(
                stream_id=stream.stream_id,
                stream_type=stream.stream_type,
                filters=stream.scope.filters,
                metadata={"uid": d["uid"], "aid": d["aid"]} if "aid" in d else {"uid": d["uid"]},
                datetime=d["datetime"],
                value=value,
                version=stream.kernel.version
            )

            data_standard.append(instance)

        return {'raw': data_standard}
