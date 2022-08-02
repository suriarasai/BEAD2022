mult_3_5 = lambda x: x%3==0 or x%5==0
print(mult_3_5(3))
print(mult_3_5(4))
print(mult_3_5(5))
def add1():
    return lambda x: x + 1

f = add1()
print(f(2))
