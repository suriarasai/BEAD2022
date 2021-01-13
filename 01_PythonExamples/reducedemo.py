from functools import reduce


def do_sum(x1, x2):
    return x1 + x2


print(reduce(do_sum, [1, 2, 3, 4]))
