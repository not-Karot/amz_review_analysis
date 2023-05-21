#!/usr/bin/env python3
"""mapper.py"""

import sys

# read line from standard input
for line in sys.stdin:

    # carica la riga in memoria con strip
    line = line.strip()

    # separa la riga nei campi richiesti
    user_id, helpfulness_numerator, helpfulness_denominator = line.split("\t")

    # calcola utilità
    if int(helpfulness_denominator) != 0:
        utility = int(helpfulness_numerator) / int(helpfulness_denominator)
    else:
        utility = 0

    # stampa user_id come chiave e utility come valore
    print(f"{user_id}\t{utility}")
