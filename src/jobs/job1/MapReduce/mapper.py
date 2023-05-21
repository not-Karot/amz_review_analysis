#!/usr/bin/env python3
"""mapper.py"""
import string
import sys
import re
from datetime import datetime

CLEANR = re.compile("<.*?>")


def cleanhtml(raw_html):
    return re.sub(CLEANR, " ", raw_html)


for line in sys.stdin:

    # carica la riga in memoria con strip
    line = line.strip()

    # separa la riga nei campi richiesti
    product_id, time, text = line.split("\t")

    # converte timestamp in anno
    year = datetime.utcfromtimestamp(int(time)).strftime("%Y")
    # pulisce il testo dai tag html
    text = cleanhtml(text)

    # toglie la punteggiatura e spazi bianchi
    text = text.replace(".", " ")
    text = text.translate(str.maketrans("", "", string.punctuation))
    text = re.sub(" +", " ", text)
    text = text.strip()

    # Estrae parole con length()<=4 dal campo delle recensioni
    words = re.findall(r"\w{4,}", text.lower())

    for word in words:
        # Stampa in output (year, product_id) come chiave e (word, 1) come valore
        print(f"{year}-{product_id}\t{word}\t1")
