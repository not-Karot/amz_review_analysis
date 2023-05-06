#!/usr/bin/env python

import sys
from collections import defaultdict, Counter

current_key = None
word_counts = defaultdict(int)

for line in sys.stdin:
    line = line.strip()

    # Estrai chiave e valori dall'input
    key, word, count = line.split('\t')
    count = int(count)

    if key == current_key:
        # Accumula il conteggio delle parole per la stessa chiave
        word_counts[word] += count
    else:
        if current_key:
            # Stampa le 5 parole più frequenti per la chiave corrente
            top_words = Counter(word_counts).most_common(5)
            for word, count in top_words:
                print(f"{current_key}\t{word}\t{count}")

        # Passa alla chiave successiva e resetta il conteggio delle parole
        current_key = key
        word_counts = defaultdict(int)
        word_counts[word] = count

# Stampa le 5 parole più frequenti per l'ultima chiave
if current_key:
    top_words = Counter(word_counts).most_common(5)
    for word, count in top_words:
        print(f"{current_key}\t{word}\t{count}")
