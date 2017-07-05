import struct
import sys
import cloudpickle
import jeton

from hazelcast.config import SerializationConfig
from hazelcast.serialization.input import _ObjectDataInput
from hazelcast.serialization.service import SerializationServiceV1, FMT_BE_INT

from jeton.processor import MapP, TestP, FlatMapP, FilterP, AccumulateP, CombineP

config = SerializationConfig()
config.set_custom_serializer(jeton.Entry, jeton.EntrySerializer)
service = SerializationServiceV1(config)


def read_packet():
    sys.stderr.write("Reading packet\n")
    len_bytes = sys.stdin.read(4)
    length = struct.unpack_from(FMT_BE_INT, len_bytes)[0]
    sys.stderr.write("Reading packet length %d \n" % length)
    buff = sys.stdin.read(length)
    sys.stderr.write("Read packet length %d \n" % length)
    return _ObjectDataInput(buff, 0, service)


def main_loop():
    sys.stderr.write("Reading initial transform\n")
    init_input = read_packet()
    name = init_input.read_utf()
    sys.stderr.write("Read transform: " + name + "\n")
    params = init_input.read_object()
    sys.stderr.write("Read params: %s\n" % params)
    output = service._create_data_output()
    p = create_processor(name, params)
    for output_packet in process_inbox(p, [], output):
        sys.stderr.write("Writing packet of size %s\n" % len(output_packet))
        sys.stdout.write(output_packet)

    while True:
        inbox_input = read_packet()
        # deserialize inbox
        inbox = inbox_input.read_object()
        sys.stderr.write("Read inbox %s\n" % inbox)
        if inbox:
            for output_packet in process_inbox(p, inbox, output):
                sys.stderr.write("Writing packet of size %s\n" % len(output_packet))
                sys.stdout.write(output_packet)
        else:
            sys.stderr.write("Start complete\n")
            for output_packet in complete(p, output):
                sys.stderr.write("Writing packet of size %s\n" % len(output_packet))
                sys.stdout.write(output_packet)


def complete(p, output, limit=1024):
    output.set_position(0)
    output.write_int(0)  # length of packet placeholder
    output.write_int(0)  # num items placeholder
    count = 0
    for out_item in p.complete():
        sys.stderr.write("type of out item: %s\n" % out_item.__class__)
        output.write_object(out_item)
        count = count + 1
        if count == limit:
            output.write_int(output.position() - 4, 0)
            output.write_int(count, 4)
            count = 0
            output_packet = output.to_byte_array()
            output.set_position(8)
            yield output_packet

    output.write_int(output.position() - 4, 0)  # length of packet
    output.write_int(count, 4)  # counts
    yield output.to_byte_array()
    if count > 0:
        output.set_position(0)
        output.write_int(4)  # length of packet
        output.write_int(0)  # counts
        yield output.to_byte_array()

def process_inbox(p, inbox, output, limit=1024):
    output.set_position(0)
    output.write_int(0)  # length of packet placeholder
    output.write_int(0)  # num items placeholder
    count = 0
    for out_item in p.process(inbox):
        sys.stderr.write("type of out item: %s\n" % out_item.__class__)
        output.write_object(out_item)
        count = count + 1
        if count == limit:
            output.write_int(output.position(), 0)
            output.write_int(count, 4)
            count = 0
            output_packet = output.to_byte_array()
            output.set_position(8)
            yield output_packet

    output.write_int(output.position() - 4, 0)  # length of packet
    output.write_int(count, 4)  # counts
    yield output.to_byte_array()
    if count > 0:
        output.set_position(0)
        output.write_int(4)  # length of packet
        output.write_int(0)  # counts
        yield output.to_byte_array()


def create_processor(name, params):
    if name == "map":
        return MapP(unpickle(params[0]))
    elif name == "flat_map":
        return FlatMapP(unpickle(params[0]))
    elif name == "filter":
        return FilterP(unpickle(params[0]))
    elif name == "accumulate":
        return AccumulateP(unpickle(params[0]), unpickle(params[1]))
    elif name == "combine":
        return CombineP(unpickle(params[0]))
    elif name == "test":
        return TestP()
    else:
        raise "Unknown processor type"


def unpickle(param):
    return cloudpickle.loads(str(bytearray(param)))


if __name__ == "__main__":
    main_loop()
