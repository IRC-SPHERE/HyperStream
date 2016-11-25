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

from hyperstream.stream import StreamInstance
from hyperstream.tool import Tool, check_input_stream_count

from subprocess import PIPE, Popen
import logging
import os

R_COMMAND = ['/usr/local/bin/Rscript', '--vanilla']
R_SCRIPT = ['plugins/sphere/tools/r_rssi_comparison_plot/plot.r']


class RRssiComparisonPlot(Tool):
    """
    Converts the value part of the stream instances to json format
    """
    def __init__(self,
            output_path="output",
            filename_suffix="_rssi_comparison_plot.pdf",
            missing_impute_value=-110,
            n_histogram_bins=20,
            n_color_bins=7,
            width_inches=8,
            height_inches=6):
        super(RRssiComparisonPlot, self).__init__(
            output_path=output_path,
            filename_suffix=filename_suffix,
            missing_impute_value=missing_impute_value,
            n_histogram_bins=n_histogram_bins,
            n_color_bins=n_color_bins,
            width_inches=width_inches,
            height_inches=height_inches)

    @check_input_stream_count(1)
    def _execute(self, sources, alignment_stream, interval):
        for time, data in sources[0].window(interval, force_calculation=True):
            time_str = time.strftime("%Y-%m-%dT%H_%M_%S")
            filename = [os.path.join('output', time_str+self.filename_suffix)]
            params = filename+[
                      str(self.missing_impute_value),
                      str(self.n_histogram_bins),
                      str(self.n_color_bins),
                      str(self.width_inches),
                      str(self.height_inches)]
            cmd_with_params = ' '.join(R_COMMAND+R_SCRIPT+params)
            logging.info("starting {}".format(cmd_with_params))
            try:
                p = Popen(R_COMMAND+R_SCRIPT+params, stdin=PIPE)
                stdout_data = p.communicate(input=data)[0]
                logging.info(stdout_data)
            except:
                pass
            logging.info("finished {}".format(cmd_with_params))
            yield StreamInstance(time, filename)



