import sys
from itertools import count, islice

from jeton import Entry


class Processor(object):
    def process(self, inbox, ordinal=0):
        return iter(())

    def complete(self):
        return iter(())


class TestP(Processor):
    def __init__(self):
        pass

    def process(self, inbox, ordinal=0):
        for item in inbox:
            yield item

    def complete(self):
        return islice(count(), 2000)

class MapP(Processor):
    def __init__(self, map_function):
        self.map_function = map_function

    def process(self, inbox, ordinal=0):
        for item in inbox:
            yield self.map_function(item)


class FlatMapP(Processor):
    def __init__(self, flat_map_function):
        self.flat_map_function = flat_map_function

    def process(self, inbox, ordinal=0):
        for item in inbox:
            iterable = self.flat_map_function(item)
            for mapped in iterable:
                yield mapped


class FilterP(Processor):
    def __init__(self, predicate):
        self.predicate = predicate

    def process(self, inbox, ordinal=0):
        for item in inbox:
            if self.predicate(item):
                yield item


class AccumulateP(Processor):
    def __init__(self, identity, accumulate):
        self.accumulate = accumulate
        self.identity = identity
        self.groups = {}

    def process(self, inbox, ordinal=0):
        for entry in inbox:
            self.groups[entry.key] = self.accumulate(self.groups.get(entry.key, self.identity), entry.value)
        return iter(())

    def complete(self):
        for key, value in self.groups.iteritems():
            yield Entry(key, value)


class CombineP(Processor):
    def __init__(self, combine):
        self.combine = combine
        self.groups = {}

    def process(self, inbox, ordinal=0):
        for entry in inbox:
            prev = self.groups.get(entry.key)
            if prev:
                self.groups[entry.key] = self.combine(prev, entry.value)
            else:
                self.groups[entry.key] = entry.value
        return iter(())

    def complete(self):
        for key, value in self.groups.iteritems():
            yield Entry(key, value)

if __name__ == "__main__":
    print("mapP")
    map = MapP(lambda e: e * e)
    for item in map.process(xrange(0,5)):
        print item

    print("flatMapP")
    flatMap = FlatMapP(lambda e: e.split())
    for item in flatMap.process("0 1 2 3 4 5 6"):
        print item

    print("filterP")
    filterP = FilterP(lambda e: e.strip())
    for item in filterP.process(["", " ", "  ", "a", "b"]):
        print item

    print("accumulateP")
    acc1 = AccumulateP(0, lambda a, n: a + 1)
    acc1.process([Entry("a", 1), Entry("b", 1), Entry("c", 1), Entry("a", 1)])

    acc2 = AccumulateP(0, lambda a, n: a + 1)
    acc2.process([Entry("a", 1), Entry("b", 1), Entry("c", 1), Entry("a", 1)])

    print("combineP")
    comb1 = CombineP(lambda l, r: l + r)
    comb1.process(acc1.complete())
    comb1.process(acc2.complete())

    for item in comb1.complete():
        print item
