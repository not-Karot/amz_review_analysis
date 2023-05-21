#!/usr/bin/env python3
"""spark application"""
import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    split,
    explode,
    length,
    year,
    from_unixtime,
    col,
    row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.functions import translate, lower
import string

punctuation = string.punctuation

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input-path", type=str, help="Input file path")
parser.add_argument("--output-path", type=str, help="Output file path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession.builder.appName("Amazon Review Analysis - Job 1").getOrCreate()
start_time = time.time()

df = (
    spark.read.option("delimiter", "\t")
    .csv(input_filepath)
    .toDF("ProductId", "Time", "Text")
)

# pulizia del testo e conversione in minuscolo
df = df.withColumn("Text", translate(df.Text, punctuation, " " * len(punctuation)))
df = df.withColumn("Text", lower(df.Text))

# Splitting the text into words
df = df.withColumn("Text", split(df.Text, " "))

df = df.withColumn("Time", from_unixtime(df.Time).cast("timestamp"))
df = df.withColumn("Year", year(df.Time))

df = df.withColumn("Text", explode(df.Text))

df = df.where(length(df.Text) >= 4)

word_counts = (
    df.groupBy("Year", "ProductId", "Text")
    .count()
    .withColumnRenamed("count", "word_count")
)

review_counts = (
    df.groupBy("Year", "ProductId").count().withColumnRenamed("count", "review_count")
)

window = Window.partitionBy(review_counts["Year"]).orderBy(
    review_counts["review_count"].desc()
)
top_products = review_counts.select(
    "*", row_number().over(window).alias("prod_rank")
).filter(col("prod_rank") <= 10)

joined = word_counts.join(top_products, ["Year", "ProductId"])

window2 = Window.partitionBy(joined["Year"], joined["ProductId"]).orderBy(
    joined["word_count"].desc()
)
top_words = joined.select("*", row_number().over(window2).alias("word_rank")).filter(
    col("word_rank") <= 5
)

top_words.write.csv(output_filepath)

print("Execution time: {} seconds".format(time.time() - start_time))
