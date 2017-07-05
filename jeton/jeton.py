from hazelcast.serialization.serializer import BaseSerializer
from py4j.java_gateway import JavaGateway

from pipe import Pipe

TYPE_ENTRY = -300

class Jeton:
    def __init__(self):
        self.gateway = JavaGateway()
        self.jet = self.gateway.entry_point.getJetInstance()

    def get_map(self, name):
        return self.jet.getMap(name)

    def get_list(self, name):
        return self.jet.getList(name)

    def new_pipe(self):
        return Pipe(self)

    def execute_pipe(self, pipe):
        self.gateway.entry_point.execute(pipe.transforms)


class Entry(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __repr__(self):
        return "Entry(key=%s, value=%s)" % (self.key, self.value)


class EntrySerializer(BaseSerializer):
    def read(self, inp):
        key = inp.read_object()
        value = inp.read_object()
        return Entry(key, value)

    def write(self, out, obj):
        out.write_object(obj.key)
        out.write_object(obj.value)

    def get_type_id(self):
        return TYPE_ENTRY
