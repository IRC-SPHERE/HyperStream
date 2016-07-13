from hyperstream.interface import Interface, Instance
import logging
from datetime import timedelta
import pandas as pd


class Runner(Interface):
    logging.debug("sliding window runner created")

    def compute(self, stream):
        width = timedelta(seconds=stream.parameters['width'])
        increment = timedelta(seconds=stream.parameters['increment'])

        df = pd.DataFrame(d.__dict__ for d in self.input_data['raw'])
        df.set_index(["datetime"], inplace=True)
        df1 = pd.DataFrame(df.value)
        grouper = df1.groupby(pd.TimeGrouper('{}s'.format(width)))

        # TODO: INCLUDE INCREMENT
        # TODO: APPLY FILTERS
        # TODO: DEAL WITH META-DATA AGGREGATION
        # TODO: DEAL WITH MULTI-DIMENSIONAL ARRAYS

        self.output_data = []
        for g in grouper:
            result = Instance(
                datetime=g[1].index[-1].to_datetime(),
                value=g[1].values.ravel().tolist(),
                stream_id=stream.stream_id,
                stream_type=stream.stream_type,
                filters=stream.filters,
                version=stream.kernel.version,
                metadata={}
            )

            self.output_data.append(result)

        logging.debug("sliding window done")
