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

from hyperstream import HyperStream,StreamId
from hyperstream.utils import all_time

from sphere_helpers import PredefinedTools

from workflows.display_experiments import create_workflow_list_technicians_walkarounds
from workflows.learn_localisation_model import create_workflow_lda_localisation_model_learner

if __name__ == '__main__':

    technicians_selection = {17,21}
    # ToDo: input technicians_selection from the user, for example using command-line arguments

    hyperstream = HyperStream(loglevel=logging.INFO)
    tools = PredefinedTools(hyperstream)

    # Various channels
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    T = hyperstream.channel_manager.tools
    D = hyperstream.channel_manager.mongo

    w0 = create_workflow_list_technicians_walkarounds(hyperstream, safe=False)
    w0.execute(all_time())
    df = M[StreamId('experiments_dataframe', dict(house=1))].window(all_time()).values()[0]
    exp_ids = set([df['experiment_id'][i-1] for i in technicians_selection])

    hyperstream.plate_manager.create_plate(
        plate_id="H1.SelectedLocalisationExperiment",
        description="Localisation experiments selected by the technician in SPHERE house",
        meta_data_id="localisation-experiment",
        values=exp_ids,
        complement=False,
        parent_plate="H1"
    )
    w = create_workflow_lda_localisation_model_learner(hyperstream, exp_ids=exp_ids, safe=False)
    w.execute(all_time())

    stream = M[StreamId('merged_2s',{'house':'1','localisation-experiment':'1476880283000-1476880901000'})]
    for (kk,vv) in stream.window(all_time()):
        print(kk)
        print(vv)
    stream = M[StreamId('merged_2s',{'house':'1','localisation-experiment':'1476884148117-1476884362837'})]
    for (kk,vv) in stream.window(all_time()):
        print(kk)
        print(vv)



    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))

