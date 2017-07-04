from jeton import Pipe, Jeton

jeton = Jeton()

p = jeton.new_pipe()

p.map(lambda x: x + 1)

jeton.execute_pipe(p)
