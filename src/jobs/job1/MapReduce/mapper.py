#!/usr/bin/env python
"""mapper.py"""
import string
import sys
import re
import time
import csv
from datetime import datetime

CLEANR = re.compile('<.*?>')


def cleanhtml(raw_html):
    return re.sub(CLEANR, ' ', raw_html)


# read line from standard input
for line in sys.stdin:

    # removing leading/trailing whitespaces
    line = line.strip()

    # split the current line into words
    product_id, time, text = line.split("\t")

    # get year
    year = datetime.utcfromtimestamp(int(time)).strftime('%Y')
    # remove html tag inside text
    text = cleanhtml(text)
    # replace dot with withespace
    text = text.replace(".", " ")

    # remove punctuation
    text = text.translate(str.maketrans('', '', string.punctuation))
    # remove withespace
    text = re.sub(' +', ' ', text)
    text = text.strip()

    # Estrai parole dal campo "Text" delle recensioni
    words = re.findall(r'\w{4,}', text.lower())

    for word in words:
        # Stampa in output (year, product_id) come chiave e (word, 1) come valore
        print(f"{year}-{product_id}\t{word}\t1")
