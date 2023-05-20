import glob
import os
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, desc, split, explode, lower, length, count, row_number

path = '${INPUT}'




spark = SparkSession.builder \
    .appName("Amazon Review Analysis - Job 1") \
    .getOrCreate()


if path.endswith(".csv"):
    df = spark.read.csv(path, header=True, sep="\t")

    # Preprocess the data
    df = df.withColumn("Year", year(col("Time")))

    # Job 1: Find top 10 products with the most reviews per year

    # Job 1
    # Trova i 10 prodotti con il maggior numero di recensioni per anno
    top_products_per_year = df.groupBy("Year", "ProductId") \
        .count() \
        .withColumnRenamed("count", "TotalReviews") \
        .orderBy("Year", desc("TotalReviews")) \
        .groupBy("Year") \
        .head(10)

    # Estrai parole dal campo "Text" delle recensioni, pulisci e conta le occorrenze
    words_df = df.select("Year", "ProductId", "Text") \
        .withColumn("Words", split(lower(col("Text")), "\\W+")) \
        .select("Year", "ProductId", explode("Words").alias("Word")) \
        .filter(length(col("Word")) >= 4) \
        .groupBy("Year", "ProductId", "Word") \
        .agg(count("Word").alias("Occurrences"))

    # Trova le top 5 parole per ogni prodotto
    window_spec = Window.partitionBy("Year", "ProductId").orderBy(desc("Occurrences"))
    top_words_per_product = words_df.withColumn("Rank", row_number().over(window_spec)) \
        .filter(col("Rank") <= 5) \
        .drop("Rank")

    # Unisci i risultati per ottenere i 10 prodotti con il maggior numero di recensioni e le top 5 parole per anno
    final_result = top_products_per_year.join(top_words_per_product, ["Year", "ProductId"]) \
        .orderBy("Year", desc("TotalReviews"), "ProductId", desc("Occurrences"))

    # Write DataFrame to CSV file
    final_result.write.csv("${OUTPUT}")

    spark.stop()
