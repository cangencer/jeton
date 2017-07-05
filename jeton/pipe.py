from cloudpickle import dumps


class Pipe:
    def __init__(self, jeton):
        self.jeton = jeton
        self.gateway = jeton.gateway
        self.transforms = self.gateway.jvm.java.util.ArrayList()

    def map(self, map):
        self._add_transform(u"map", self._dumps(map))
        return self

    def flat_map(self, flat_map):
        self._add_transform(u"flat_map", self._dumps(flat_map))
        return self

    def filter(self, predicate):
        self._add_transform(u"filter", self._dumps(predicate))
        return self

    def read_map(self, name):
        self._add_transform(u"read_map", name)
        return self

    def write_map(self, name):
        self._add_transform(u"write_map", name)
        return self

    def reduce(self, identity, accumulate, combine):
        self._add_transform(u"reduce", self._dumps(identity), self._dumps(accumulate), self._dumps(combine))
        return self

    def execute(self):
        self.jeton.execute_pipe(self)

    def _add_transform(self, name, *args):
        params = self.gateway.jvm.java.util.ArrayList()
        for x in args:
            params.add(x)
        self.transforms.add(self.gateway.jvm.com.hazelcast.jet.jeton.Transform(name, params))

    @staticmethod
    def _dumps(o):
        return bytearray(dumps(o))