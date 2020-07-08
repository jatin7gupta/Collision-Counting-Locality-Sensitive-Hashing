## import modules here

########## Question 1 ##########
# do not change the heading of the function


def count_collisions(hash, query, offset):
    counter = 0
    for i in range(len(query)):
        if abs(hash[i] - query[i]) <= offset:
            counter += 1
    return counter


def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):

    def p(hash, query, offset, alpha):
        if count_collisions(hash[1], query, offset) >= alpha:
             return hash[0]
        # else:
        #     return -1

    offset = 0
    found = False
    rdd = None
    while not found:
        rdd = data_hashes.map(lambda hash: p(hash, query_hashes, offset, alpha_m)).filter(lambda x: x is not None)

        res = rdd.collect()
        s = set(res)
        if len(s) >= beta_n:
            found = True
        else:
            print(s)
            offset += 1
    return rdd

    # while not found:
        # for each data D in data hashes:
            # if count (D, query_hashes, offset) > alpha_m:
                # cand <- cand.union(i)
        # if count(cand) < beta_n:
            #offset += 1
        # else:
        #   found = True
        # pass
    #return cand