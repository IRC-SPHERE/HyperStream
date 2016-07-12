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

from sphere_connector_package.sphere_connector.client import Client
from sphere_connector_package.sphere_connector.config import BasicConfig
from sphere_connector_package.sphere_connector.config import ExperimentConfig
from sphere_connector_package.sphere_connector.experiment import Experiment

basic_config = BasicConfig(include_redcap=False)
client = Client(basic_config.mongo)


class RawInput(Input):
    def get_data(self, stream):
        logging.debug("Getting data {} (raw input)".format(stream.stream_id))
        logging.debug(stream.parameters)
        logging.debug(stream.scope)
        logging.debug(stream.sources)
        experiment_config = ExperimentConfig(experiment_start=stream.scope.start, experiment_end=stream.scope.end,
                                             experiment_id=stream.stream_id)
        experiment = Experiment(client, experiment_config, basic_config)

        if stream.parameters["modality"] == "annotations":
            data = experiment.annotations.get_data()
        elif stream.parameters["modality"] == "environmental":
            data = experiment.environmental.get_data()
        elif stream.parameters["modality"] == "video":
            data = experiment.video.get_data()
        elif stream.parameters["modality"] == "wearable":
            data = experiment.wearable.get_data()
        else:
            raise RuntimeError("Unknown modality for raw data input {}".format(stream.parameters["modality"]))

        return data
