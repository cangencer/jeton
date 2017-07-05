import struct
import sys
from itertools import islice

import cloudpickle
from hazelcast.config import SerializationConfig
from hazelcast.serialization.input import _ObjectDataInput
from hazelcast.serialization.service import SerializationServiceV1, FMT_BE_INT

import jeton
from jeton.processor import MapP, TestP, FlatMapP, FilterP, AccumulateP, CombineP

config = SerializationConfig()
config.set_custom_serializer(jeton.Entry, jeton.EntrySerializer)
service = SerializationServiceV1(config)


def read_packet():
    len_bytes = sys.stdin.read(4)
    length = struct.unpack_from(FMT_BE_INT, len_bytes)[0]
    buff = sys.stdin.read(length)
    # sys.stderr.write("Read packet length %d \n" % length)
    return _ObjectDataInput(buff, 0, service)


def main_loop():
    init_input = read_packet()
    name = init_input.read_utf()
    params = init_input.read_object()

    output = service._create_data_output()
    # initialize header for output
    output.write_int(0)  # length of packet placeholder
    output.write_int(0)  # num items placeholder

    p = create_processor(name, params)
    sys.stderr.write("Init processor %s\n" % p.__class__)
    for output_packet in process_generator([], output):
        # sys.stderr.write("Writing packet of size %s\n" % len(output_packet))
        sys.stdout.write(output_packet)

    while True:
        inbox_input = read_packet()
        # deserialize inbox
        inbox = inbox_input.read_object()
        generator = p.process(inbox) if inbox else p.complete()
        for output_packet in process_generator(generator, output):
            sys.stdout.write(output_packet)


def process_generator(generator, output, limit=1024):
    while True:
        output.set_position(8)
        items_written = 0
        for item in islice(generator, limit):
            items_written = items_written + 1
            output.write_object(item)

        if items_written > 0:
            output.write_int(output.position() - 4, 0) # write packet length
            output.write_int(items_written, 4) # write number of items
            yield output.to_byte_array()
        else:
            break

    # write an empty packet to indicate processing is finished
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
