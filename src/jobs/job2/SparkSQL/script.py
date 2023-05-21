import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType

# Initialize SparkSQL Session
spark = SparkSession.builder.appName("Amazon Review Analysis - Job 2").getOrCreate()
start_time = time.time()


# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input-path", type=str, help="Input file path")
parser.add_argument("--output-path", type=str, help="Output file path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

df = (
    spark.read.option("delimiter", "\t")
    .csv(input_filepath)
    .toDF("UserId", "HelpfulnessNumerator", "HelpfulnessDenominator")
)

# Calcola utilità
def calculate_utility(numerator, denominator):
    if int(denominator) == 0:
        return 0.0
    else:
        return float(numerator) / float(denominator)


calculate_utility_udf = udf(calculate_utility, FloatType())
df = df.withColumn(
    "utility",
    calculate_utility_udf(col("HelpfulnessNumerator"), col("HelpfulnessDenominator")),
)

# Calcola utilità media (apprezzamento) per ogni utente
user_utility_avg = df.groupBy("UserId").avg("utility")

# Ordina gli utenti per apprezzamento in ordine decrescente
sorted_users = user_utility_avg.orderBy(col("avg(utility)").desc())


sorted_users.write.csv(output_filepath)

print("Execution time: {} seconds".format(time.time() - start_time))

spark.stop()
