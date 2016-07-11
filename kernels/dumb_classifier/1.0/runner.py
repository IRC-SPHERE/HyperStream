from hyperstream.interface import Interface
import logging


class Runner(Interface):
    logging.debug("dumb classifier runner created")

    def compute(self):
        self.output_data = self.input_data
