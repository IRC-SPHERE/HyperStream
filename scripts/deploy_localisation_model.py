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
from datetime import datetime, timedelta


def run(house, selection, delete_existing_workflows=True, loglevel=logging.INFO):
    from hyperstream import HyperStream, StreamId, TimeInterval, UTC
    from workflows.display_experiments import create_workflow_list_technicians_walkarounds
    from workflows.deploy_localisation_model import create_workflow_localisation_predict

    hyperstream = HyperStream(loglevel=loglevel)
    M = hyperstream.channel_manager.memory

    workflow_id0 = "list_technicians_walkarounds"

    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id0)

    try:
        w0 = hyperstream.workflow_manager.workflows[workflow_id0]
    except KeyError:
        w0 = create_workflow_list_technicians_walkarounds(hyperstream, house=house, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id0)
    time_interval = TimeInterval.up_to_now()
    w0.execute(time_interval)

    df = M[StreamId('experiments_dataframe', dict(house=house))].window(time_interval).values()[0]
    experiment_ids = set([df['experiment_id'][i - 1] for i in selection])

    hyperstream.plate_manager.delete_plate("H.SelectedLocalisationExperiment")
    hyperstream.plate_manager.create_plate(
        plate_id="H.SelectedLocalisationExperiment",
        description="Localisation experiments selected by the technician in SPHERE house",
        meta_data_id="localisation-experiment",
        values=[],
        complement=True,
        parent_plate="H"
    )

    hyperstream.plate_manager.delete_plate("H.W")
    hyperstream.plate_manager.create_plate(
        plate_id="H.W",
        description="All wearables in all houses",
        meta_data_id="wearable",
        values=[],
        complement=True,
        parent_plate="H"
    )

    experiment_ids_str = '_'.join(experiment_ids)
    workflow_id = "lda_localisation_model_predict_"+experiment_ids_str

    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id)

    try:
        w = hyperstream.workflow_manager.workflows[workflow_id]
    except KeyError:
        w = create_workflow_localisation_predict(hyperstream, house=house,experiment_ids=experiment_ids, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id)

    time_interval = TimeInterval.up_to_now()
    w.execute(time_interval)

    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))

    # for wearable in 'ABCD':
    #     sid = StreamId('predicted_locations_broadcasted', dict(house=house, wearable=wearable))
    #     print sid
    #     print len(list(hyperstream.channel_manager.memory[sid].window(time_interval)))
    #     print '\n\n'

if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    if len(sys.argv) < 3:
        print("Expected at least two integer ids")
        exit(0)

    try:
        technicians_selection = map(int, sys.argv[1:])
    except ValueError:
        print("Expected at least two integer ids")
        technicians_selection = None  # just to keep lint happy
        exit(0)

    house = 1
    run(house, technicians_selection)
