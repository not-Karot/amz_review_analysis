#!/usr/bin/env python3
"""spark application"""
import argparse
import string
import time
from datetime import datetime

from pyspark import SparkContext

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input-path", type=str, help="Input file path")
parser.add_argument("--output-path", type=str, help="Output file path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# Initialize SparkContext with the proper configuration
sc = SparkContext(appName="Amazon Review Analysis")
start_time = time.time()   # Ottieni il tempo di inizio

def clean_text(text):
    # Replace punctuation with spaces
    text = text.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
    # Split the text into words and return
    return text.lower().split()

# Caricare i dati in un RDD
data = sc.textFile(input_filepath)

# Estrarre i campi
data = data.map(lambda line: line.split("\t")).map(lambda fields: (fields[0], int(fields[1]), clean_text(fields[2])))

# Convertire timestamp in anno
data = data.map(lambda x: (datetime.utcfromtimestamp(x[1]).year, x[0], x[2]))

# Contare le parole con lenght()>=4 per prodotto per anno
word_counts = data.flatMap(lambda x: [((x[0], x[1], word), 1) for word in x[2] if len(word) >= 4]).reduceByKey(lambda a, b: a + b)

# Contare recensioni per prodotto per anno
review_counts = data.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda a, b: a + b)

# Ordina e prendi i top10 prodotti per anno
top_products = review_counts.sortBy(lambda x: (-x[1], x[0])).groupBy(lambda x: x[0][0]).map(lambda x: (x[0], sorted(list(x[1]), key=lambda y: y[1], reverse=True)[:10]))


top_products = top_products.flatMap(lambda x: [((x[0], product[0][1]), product[1]) for product in x[1]])

# Prendi le parole solo per i top prodotti
top_words = word_counts.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1]))).join(top_products).map(lambda x: (x[0], x[1][0]))

# Ordina e prendi le top 5 parole per ogni prodotto e per anno
top_words = top_words.sortBy(lambda x: (-x[1][1], x[0])).groupBy(lambda x: x[0]).map(lambda x: (x[0], sorted(list(x[1]), key=lambda y: y[1][1], reverse=True)[:5]))

# Salva i risultati su un txt
top_words.saveAsTextFile(output_filepath)


print("Tempo totale di esecuzione: {} secondi".format(time.time() - start_time))