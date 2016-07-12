from hyperstream.interface import Interface
import logging


class Runner(Interface):
    logging.debug("Merge runner created")

    def compute(self):
        self.output_data = self.input_data
