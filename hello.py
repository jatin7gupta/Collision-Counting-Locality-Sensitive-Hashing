hash = [1,2,3]
query = [2,2,4]
def func(h, q):
    return abs(h-q)
x = map(func, hash, query)
for i in x:
    print(i)