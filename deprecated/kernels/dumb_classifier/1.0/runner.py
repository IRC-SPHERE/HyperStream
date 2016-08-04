from hyperstream.interface import Interface, Instance
import logging


class Runner(Interface):
    logging.debug("dumb classifier runner created")

    def compute(self, stream):
        # Assume there is just one stream
        self.output_data = []
        for d in next(iter(self.input_data.values())):
            result = Instance(
                datetime=d.datetime,
                value=[1.0],
                stream_id=stream.stream_id,
                stream_type=stream.stream_type,
                filters=stream.filters,
                version=stream.kernel.version,
                metadata={}
            )

            self.output_data.append(result)

        logging.debug("dumb classifier finished")
