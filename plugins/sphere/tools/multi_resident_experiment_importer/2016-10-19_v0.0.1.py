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

import pandas as pd
import os
from hyperstream.tool import Tool, check_input_stream_count
from hyperstream.stream import StreamInstance
from hyperstream.utils import unix2datetime
import hyperstream  # just to get the path out of it

DATA_PATH = '../data/2016_10_17_multiresident_annotations_from_sion_anonymised/location_annotations_unique_dt.csv'
META_PATH = '../data/2016_10_17_multiresident_annotations_from_sion_anonymised/occurrence_times_manually.txt'


def reformat(doc):
    dt = unix2datetime(doc['dt'])
    del doc['dt']
    return StreamInstance(dt, doc)


class MultiResidentExperimentImporter(Tool):
    def __init__(self):
        super(MultiResidentExperimentImporter, self).__init__()
        path = os.path.dirname(hyperstream.__file__)
        self.data = pd.read_csv(os.path.join(path, DATA_PATH))
        self.experiment_metadata = pd.read_csv(os.path.join(path, META_PATH))

    @check_input_stream_count(0)
    def _execute(self, sources, alignment_stream, interval):
        for i, row in self.data.iterrows():
            dt = unix2datetime(row["dt"])
            if dt in interval:
                yield StreamInstance(dt, dict(
                    camera_id=row["camera_id"],
                    exper_id=row["exper_id"],
                    person_id=row["person_id"],
                    wearable_id=row["wearable_id"]))
