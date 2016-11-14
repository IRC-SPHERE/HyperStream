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

import logging
import sys

from hyperstream import HyperStream, StreamId, TimeInterval

from workflows.display_experiments import create_workflow_list_technicians_walkarounds
from workflows.learn_localisation_model import create_workflow_lda_localisation_model_learner


def run(selection):
    hyperstream = HyperStream(loglevel=logging.INFO)
    M = hyperstream.channel_manager.memory

    workflow_id0 = "list_technicians_walkarounds"
    # hyperstream.workflow_manager.delete_workflow(workflow_id)
    try:
        w0 = hyperstream.workflow_manager.workflows[workflow_id0]
    except KeyError:
        w0 = create_workflow_list_technicians_walkarounds(hyperstream, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id0)
    w0.execute(TimeInterval.all_time())

    df = M[StreamId('experiments_dataframe', dict(house=1))].window(TimeInterval.all_time()).values()[0]
    experiment_ids = set([df['experiment_id'][i - 1] for i in selection])

    hyperstream.plate_manager.create_plate(
        plate_id="H1.SelectedLocalisationExperiment",
        description="Localisation experiments selected by the technician in SPHERE house",
        meta_data_id="localisation-experiment",
        values=experiment_ids,
        complement=False,
        parent_plate="H1"
    )

    # Ensure the model is overwritten if it's already there
    hyperstream.channel_manager.mongo.purge_stream(
            StreamId(name="location_prediction_lda_mk1", meta_data=dict(house=1)))

    workflow_id1 = "lda_localisation_model_learner"
    hyperstream.workflow_manager.delete_workflow(workflow_id1)
    try:
        w1 = hyperstream.workflow_manager.workflows[workflow_id1]
    except KeyError:
        w1 = create_workflow_lda_localisation_model_learner(
            hyperstream, house=1, experiment_ids=experiment_ids, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id1)

    w1.execute(TimeInterval.all_time())

    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Expected two integer ids")
        exit(0)

    try:
        technicians_selection = map(int, sys.argv[1:])
    except ValueError:
        print("Expected two integer ids")
        technicians_selection = None  # just to keep lint happy
        exit(0)

    run(technicians_selection)
