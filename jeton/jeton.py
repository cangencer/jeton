from py4j.java_gateway import JavaGateway

from pipe import Pipe

class Jeton:
    def __init__(self):
        self.gateway = JavaGateway()
        self.jet = self.gateway.entry_point.getJetInstance()

    def get_map(self, name):
        return self.jet.getMap(name)

    def new_pipe(self):
        return Pipe(self)

    def execute_pipe(self, pipe):
        self.gateway.entry_point.execute(pipe.transforms)