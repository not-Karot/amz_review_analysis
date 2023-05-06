#!/usr/bin/env python

import sys
import re
import time

for line in sys.stdin:
    # Rimuovi spazi bianchi iniziali e finali
    line = line.strip()

    # Dividi la riga in campi
    fields = line.split(',')

    # Estrai campi rilevanti
    product_id, unix_time, text = fields[1], int(fields[7]), fields[9]

    # Converti il timestamp in anno
    year = time.strftime('%Y', time.gmtime(unix_time))

    # Estrai parole dal campo "Text" delle recensioni
    words = re.findall(r'\w{4,}', text.lower())

    for word in words:
        # Stampa in output (year, product_id) come chiave e (word, 1) come valore
        print(f"{year}-{product_id}\t{word}\t1")
