# Collision Counting Locality Sensitive Hashing using PySpark (C2LSH)
This is the implementation for C2LSH algorithm using PySpark with constraints. C2LSH algorithm searches the nearest neighbours (Euclidian space) from the given data points by accepting nearest neighbours using features in the datapoints.

There are four arguments as input to c2lsh():

1. data_hashes: is a rdd where each element (i.e., key,value pairs) in this rdd corresponds to (id, data_hash). id is an integer and data_hash is a python list that contains $m$ integers (i.e., hash values of the data point).
2. query_hashes is a python list that contains $m$ integers (i.e., hash values of the query).
3. alpha_m is an integer which indicates the minimum number of collide hash values between data and query (i.e., $\alpha m$).
4. beta_n is an integer which indicates the minimum number of candidates to be returned (i.e., $\beta n$).
 
## Constraints
Not allowed to use the following PySpark functions:
* aggregate, treeAggregate，aggregateByKey

* collect, collectAsMap

* countByKey， countByValue

* foreach

* reduce, treeReduce

* saveAs* (e.g. saveAsTextFile)

* take* (e.g. take, takeOrdered)

* top

* fold

# Optimization
Used binary search to search the search space which is a huge improvement from the given linear algorithm discussed in the original research paper.
