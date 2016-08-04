from hyperstream.interface import Interface, Instance


class Runner(Interface):
    def compute(self, stream):
        pass
