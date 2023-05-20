#!/usr/bin/env python3
"""reducer.py"""

import sys

product_user = {}
user_product = {}

# Store all user-product pairs in a set
seen_pairs = set()

for line in sys.stdin:
    line = line.strip()
    pair = tuple(line.split("\t"))

    # Skip this line if we've seen this user-product pair before
    if pair in seen_pairs:
        continue

    # Remember that we've seen this user-product pair
    seen_pairs.add(pair)

    userId, productId = pair

    if userId not in user_product:
        user_product[userId] = set()
    user_product[userId].add(productId)

    if productId not in product_user:
        product_user[productId] = set()
    product_user[productId].add(userId)

user_groups = []

for productId, usersId in product_user.items():
    if len(usersId) > 1:
        for user_group in user_groups:
            if any(user in user_group for user in usersId):
                user_group.update(usersId)
                break
        else:
            user_groups.append(set(usersId))

for user_group in user_groups:
    if len(user_group) > 2:
        common_products = set.intersection(*(user_product[user] for user in user_group))
        if len(common_products) >= 3:
            print('%s\t%s' % (','.join(sorted(user_group)), ','.join(sorted(common_products))))
