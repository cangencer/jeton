from py4j.java_gateway import JavaGateway

from pipe import Pipe


class Jeton:
    def __init__(self):
        self.gateway = JavaGateway()

    def new_pipe(self):
        return Pipe(self.gateway)

    def execute_pipe(self, pipe):
        self.gateway.entry_point.execute(pipe.transforms)