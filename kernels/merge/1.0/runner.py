from hyperstream.interface import Interface, Instance
import logging


class Runner(Interface):
    logging.debug("Merge runner created")

    def compute(self, stream):
        # TODO: For now we just assume that the two input sources are perfectly aligned, and create a dictionary of
        # their values
        # TODO: At least check for matching datetimes!!

        stream_ids = [s.stream_id for s in self.input_data]

        self.output_data = []

        for v in zip(*self.input_data.values()):
            result = Instance(
                datetime=v[0].datetime,
                value=dict((stream_ids[i], v[i].value) for i in range(len(stream_ids))),
                stream_id=stream.stream_id,
                stream_type=stream.stream_type,
                filters=stream.filters,
                version=stream.kernel.version,
                metadata={}
            )

            self.output_data.append(result)
