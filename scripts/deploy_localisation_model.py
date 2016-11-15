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


def run(house, delete_existing_workflows=True):
    from hyperstream import HyperStream, TimeInterval, StreamId, UTC
    from workflows.deploy_localisation_model import create_workflow_localisation_predict

    hyperstream = HyperStream(loglevel=logging.INFO)

    workflow_id = "lda_localisation_model_predict"

    if delete_existing_workflows:
        hyperstream.workflow_manager.delete_workflow(workflow_id)

    try:
        w = hyperstream.workflow_manager.workflows[workflow_id]
    except KeyError:
        w = create_workflow_localisation_predict(hyperstream, safe=False)
        hyperstream.workflow_manager.commit_workflow(workflow_id)

    # Set times for execution
    start_time = datetime(year=2016, month=10, day=19, hour=12, minute=28, tzinfo=UTC)
    duration = timedelta(minutes=1)

    end_time = start_time + duration
    time_interval = TimeInterval(start_time, end_time)

    # w.execute(TimeInterval.all_time())
    w.execute(time_interval)

    print('number of non_empty_streams: {}'.format(
        len(hyperstream.channel_manager.memory.non_empty_streams)))

    for wearable in 'ABCD':
        sid = StreamId('predicted_locations_broadcasted', dict(house=house, wearable=wearable))
        print sid
        print len(list(hyperstream.channel_manager.memory[sid].window(time_interval)))
        print '\n\n'

if __name__ == '__main__':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    house = 1
    run(house)
