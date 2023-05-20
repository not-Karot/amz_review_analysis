#!/usr/bin/env python3
"""reducer.py"""

import sys
from collections import defaultdict, Counter
from operator import itemgetter

# Strutture dati per memorizzare le parole e le recensioni per prodotto e anno
word_counts = defaultdict(lambda: defaultdict(Counter))
review_counts = defaultdict(lambda: defaultdict(int))

# Legge i dati dal mapper
for line in sys.stdin:
    line = line.strip()
    key, word, count = line.split('\t')
    year, product_id = key.split('-')
    count = int(count)

    # Aggiorna il conteggio delle parole e delle recensioni per prodotto e anno
    word_counts[year][product_id][word] += count
    review_counts[year][product_id] += count

# Trova i 10 prodotti più recensiti per ogni anno
top_products = {}
for year in review_counts:
    top_products[year] = sorted(review_counts[year].items(), key=itemgetter(1), reverse=True)[:10]

# Calcola e stampa le 5 parole con frequenza maggiore per i 10 prodotti più recensiti per ogni anno
for year in word_counts:
    for product_id, word_count in word_counts[year].items():
        if (product_id, review_counts[year][product_id]) in top_products[year]:
            for word, count in sorted(word_count.items(), key=itemgetter(1), reverse=True)[:5]:
                print(f"{year}-{product_id}\t{word}\t{count}")
