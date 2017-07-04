from cloudpickle import dumps

class Pipe:
    def __init__(self, gateway):
        self.gateway = gateway
        self.transforms = gateway.jvm.java.util.ArrayList()

    def map(self, map_function):
        params = self.gateway.jvm.java.util.ArrayList()
        params.add(bytearray(dumps(map_function)))
        transform = self.gateway.jvm.com.hazelcast.jet.jeton.Transform(u"map", params)
        self.transforms.add(transform)
        return self

    def execute(self):
        self.jeton
