import sys
import select

from hazelcast.serialization.service import SerializationServiceV1
from hazelcast.config import SerializationConfig, INTEGER_TYPE

from jeton import EntrySerializer, Entry

config = SerializationConfig()
config.set_custom_serializer(Entry, EntrySerializer)
service = SerializationServiceV1(config)

rlist = [sys.stdin]
wlist = [sys.stdout]

def main_loop():
    sys.stderr.write("Ready to read some bytes\n")
    while True:
        ready = select.select(rlist, [], [])[0]
        for f in ready:
            bytes = f.read(1)
            sys.stderr.write("Read %d bytes\n" % (len(bytes)))


if __name__ == "__main__":
    main_loop()


