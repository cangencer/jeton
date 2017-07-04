from jeton import Jeton

jeton = Jeton()

map = jeton.get_map("words")
map.put(0, "0 1 1 2 2 3 3 4 4 5 5 6")

pipe = jeton.new_pipe()

pipe\
    .read_map("words")\
    .flat_map(lambda e: iter(e.value().split()))\
    .map(lambda n: (n, 1))\
    .reduce(lambda a, n: a + 1, lambda l, r: l + r)\
    .write_map("counts")\
    .execute()

