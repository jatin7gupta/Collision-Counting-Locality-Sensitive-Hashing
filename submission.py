## import modules here

########## Question 1 ##########
# do not change the heading of the function


def count_collision(hash, query, offset, alpha):
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


def first_pass(data_hashes, query_hashes, alpha_m, beta_n):
    offset = 0
    found = False
    rdd = None
    rdd_count = -1
    while not found:
        rdd = data_hashes.filter(lambda h: count_collision(h[1], query_hashes, offset, alpha_m))
        rdd_count = rdd.count()
        if rdd_count >= beta_n:
            found = True
        else:
            if offset == 0:
                offset = offset+1
            else:
                offset = 2*offset
    return offset, rdd_count, rdd


def binary_search(data_hashes, query_hashes, alpha_m, beta_n, offset):
    high = offset
    low = offset//2
    rdd = None
    while low <= high:
        mid = low + (high - low) // 2
        rdd = data_hashes.filter(lambda h: count_collision(h[1], query_hashes, mid, alpha_m))
        rdd_count = rdd.count()
        if rdd_count == beta_n:
            return rdd
        elif rdd_count > beta_n:
            high = mid-1
        else:
            low = mid+1
    return rdd


def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset, rdd_count, rdd = first_pass(data_hashes, query_hashes, alpha_m, beta_n)
    if offset == 0 and rdd_count >= beta_n:
        return rdd.map(lambda h: h[0])
    elif rdd_count == beta_n:
        return rdd.map(lambda h: h[0])
    else:
        rdd = binary_search(data_hashes, query_hashes, alpha_m, beta_n, offset)
        return rdd.map(lambda h: h[0])
