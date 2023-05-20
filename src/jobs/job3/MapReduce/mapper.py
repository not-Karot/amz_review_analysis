#!/usr/bin/env python3
"""mapper.py"""

import sys

for line in sys.stdin:
    # removing leading/trailing whitespaces
    line = line.strip()
    # split the current line into productId, userId, score
    productId, userId, score = line.split("\t")

    productId = productId.strip()
    userId = userId.strip()
    score = score.strip()
    try:
        score = int(score)

        if score >= 4:
            print('%s\t%s' % (userId, productId))
    except ValueError:
        continue
