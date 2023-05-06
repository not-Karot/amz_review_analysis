#!/usr/bin/env python
"""mapper.py"""

import sys

# read line from standard input
for line in sys.stdin:
    # removing leading/trailing whitespaces
    line = line.strip()

    # split the current line into fields
    user_id, helpfulness_numerator, helpfulness_denominator = line.split("\t")

    # calculate appreciation
    if int(helpfulness_denominator) != 0:
        utility = int(helpfulness_numerator) / int(helpfulness_denominator)
    else:
        utility = 0

    # print user_id as key, and appreciation as value
    print(f"{user_id}\t{utility}")
