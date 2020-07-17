from pyspark import SparkContext, SparkConf
from time import time
import pickle
import submission as submission

def createSC():
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("C2LSH")
    sc = SparkContext(conf = conf)
    return sc

with open("toy/toy_hashed_data", "rb") as file:
    data = pickle.load(file)

with open("toy/toy_hashed_query", "rb") as file:
    query_hashes = pickle.load(file)

import random


def generate_test_case(dim, count, seed, start=-1000, end=1000):
    random.seed(seed)

    data = [
        [
            random.randint(start, end)
            for _ in range(dim)
        ]
        for i in range(count)
    ]

    query = [random.randint(start, end) for _ in range(dim)]

    return data, query


def generate2(dimension, count, seed, start=0, end=100):
    data = [
        [n] * dimension
        for n in range(start, end)
        for i in range(count)
    ]

    query = [seed] * dimension

    return data, query


def generate3(dimension, count, seed, start=0, end=100):
    data = [
        [k + j for j in range(dimension)]
        for k in range(start, end)
        for i in range(count)
    ]

    query = [seed] * dimension

    return data, query

alpha_m, beta_n = 10, 10
data, query = generate_test_case(20, 20000, 7, -10000, 10000)

# alpha_m, beta_n = 10, 50
# data, query = generate( 13, 200, 100, -50000, 50000)

# alpha_m, beta_n = 10, 64
# data, query = generate2( 13, 9, 100, 0, 120)

# alpha_m, beta_n = 13, 25
# data, query = generate3( 13, 7, 100, 0, 120)

# alpha_m  = 10
# beta_n = 10

sc = createSC()
data_hashes = sc.parallelize([(index, x) for index, x in enumerate(data)])
query_hashes = query

start_time = time()
res = submission.c2lsh(data_hashes, query_hashes, alpha_m, beta_n).collect()
end_time = time()
sc.stop()

print('running time:', end_time - start_time)
print('Number of candidate: ', len(res))
print('set of candidate: ', set(res))
# print('set of candidate: ', res)