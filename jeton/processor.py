class Processor(object):

    def process(self, inbox, ordinal=0):
        yield

    def complete(self):
        yield


class MapProcessor(Processor):
    def __init__(self, map_function):
        self.map_function = map_function

    def process(self, inbox, ordinal=0):
        for item in inbox:
            yield self.map_function(item)

    def complete(self):
        yield


class FlatMapProcessor(Processor):
    def __init__(self, flat_map_function):
        self.flat_map_function = flat_map_function

    def process(self, inbox, ordinal=0):
        for item in inbox:
            iterable = self.flat_map_function(item)
            for mapped in iterable:
                yield mapped


class FilterProcessor(Processor):
    def __init__(self, predicate):
        self.predicate = predicate

    def process(self, inbox, ordinal=0):
        for item in inbox:
            if self.predicate(item):
                yield item

