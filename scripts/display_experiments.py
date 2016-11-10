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

from __future__ import print_function

from hyperstream import HyperStream, StreamId
from hyperstream.utils import all_time
import arrow
import logging

from workflows.display_experiments import create_workflow_localisation_from_tech_script


if __name__ == '__main__':

    hyperstream = HyperStream(loglevel=logging.INFO)

    # Various channels
    M = hyperstream.channel_manager.memory
    S = hyperstream.channel_manager.sphere
    T = hyperstream.channel_manager.tools
    D = hyperstream.channel_manager.mongo

    w = create_workflow_localisation_from_tech_script(hyperstream, safe=False)
    w.execute(all_time())

    print('number of sphere non_empty_streams: {}'.format(len(S.non_empty_streams)))
    print('number of memory non_empty_streams: {}'.format(len(M.non_empty_streams)))

    experiment_data = M[StreamId('experiments_list', dict(house=1))].window(all_time()).values()
    df = M[StreamId('experiments_dataframe', dict(house=1))].window(all_time()).values()[0]
    # arrow.get(x).humanize()
    # df['start'] = df['start'].map('{:%Y-%m-%d %H:%M:%S}'.format)
    df['duration'] = df['end']-df['start']
    df['start'] = map(lambda x:'{:%Y-%m-%d %H:%M:%S}'.format(x), df['start'])
    df['end'] = map(lambda x:'{:%Y-%m-%d %H:%M:%S}'.format(x), df['end'])
    # df['duration'] = map(lambda x:'{:%Mmin %Ssec}'.format(x),df['duration'])

    def duration2str(x):
        minutes, seconds = divmod(x.total_seconds(), 60)
        return '{} min {} sec'.format(int(minutes), int(seconds))

    df['start_as_text'] = map(lambda x:arrow.get(x).humanize(), df['start'])
    df['duration_as_text'] = map(lambda x:duration2str(x), df['duration'])
    print(df[['id', 'start_as_text', 'duration_as_text', 'start', 'end', 'direction', 'annotator']])
