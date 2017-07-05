from jeton import Jeton
from jeton import Entry

jeton = Jeton()


words = jeton.get_map("words")
counts = jeton.get_map("counts")

pipe = jeton.new_pipe()

# map.put(0, 0)
# pipe.read_map("words").map(lambda e: Entry(e.key, e.value + 1)).write_map("words").execute()

words.put(0, "0 1 1 2 2 3 3 4 4 5 5 6")
print ("Executing word count")
pipe\
    .read_files("/Users/can/src/hazelcast-jet-code-samples/batch/sample-data/src/main/resources/books2")\
    .flat_map(lambda e: e.split())\
    .map(lambda w: Entry(w, 1)) \
    .reduce(0, lambda a, n: a + 1, lambda l, r: l + r) \
    .write_map("counts") \
    .execute()

print(counts)

