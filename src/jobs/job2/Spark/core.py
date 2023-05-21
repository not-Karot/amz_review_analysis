import argparse
import time
from pyspark import SparkContext

# Inizializza SparkContext
sc = SparkContext(appName="Amazon Review Analysis - Job 2")
start_time = time.time()

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input-path", type=str, help="Input file path")
parser.add_argument("--output-path", type=str, help="Output file path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path


# Calcola utilità
def calculate_utility(numerator, denominator):
    if int(denominator) == 0:
        return 0.0
    else:
        return float(numerator) / float(denominator)


# Caricare i dati in un RDD
data = sc.textFile(input_filepath)

data = data.map(lambda line: line.split("\t")).map(
    lambda fields: (fields[0], int(fields[1]), int(fields[2]))
)

# Calcola l'utilità per ogni recensione
data = data.map(lambda x: (x[0], x[1], x[2], calculate_utility(x[1], x[2])))

# Calcola utilità media (apprezzamento) per ogni utente
user_utility_avg = (
    data.map(lambda x: (x[0], x[3])).groupByKey().mapValues(lambda v: sum(v) / len(v))
)

# Ordina gli utenti per apprezzamento in ordine decrescente
sorted_users = user_utility_avg.sortBy(lambda x: x[1], ascending=False)

sorted_users.map(lambda x: "{}\t{}".format(x[0], x[1])).saveAsTextFile(output_filepath)

print("Execution time: {} seconds".format(time.time() - start_time))

sc.stop()
