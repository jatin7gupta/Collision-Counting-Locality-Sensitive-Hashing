## import modules here

########## Question 1 ##########
# do not change the heading of the function


def c(hash, query, offset, alpha):
    counter = 0
    i = 0
    while i < len(query) and counter < alpha:
        if abs(hash[i] - query[i]) <= offset:
            counter += 1
        i = i+1
    if counter >= alpha:
        return True
    else:
        return False


def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):

    offset = 0
    found = False
    rdd = None
    while not found:
        rdd = data_hashes.filter(lambda h: c(h[1], query_hashes, offset, alpha_m))
        if rdd.count() >= beta_n:
            found = True
        else:
            offset += 1
    return rdd.map(lambda h: h[0])


# rdd = data_hashes.map(lambda hash: p(hash, query_hashes, offset, alpha_m)).filter(lambda x: x is not None)

def count_collisions(hash, query, offset):
    counter = 0
    for i in range(len(query)):
        if abs(hash[i] - query[i]) <= offset:
            counter += 1
    return counter


def p(hash, query, offset, alpha):
    if count_collisions(hash[1], query, offset) >= alpha:
        return hash[0]
