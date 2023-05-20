import glob
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, desc, split, explode, lower, length, count, row_number
folder_path = './input'

def read_job_files(job_number: str):
    folder = Path(folder_path)
    job_files = glob.glob(str(folder / f'*{job_number}*'))
    return job_files


# Inizializza la Spark session
spark = SparkSession.builder \
    .appName("Amazon Review Analysis - Job 1") \
    .getOrCreate()


paths = read_job_files('job1')
for path in paths:
    df = pd.read_csv(path)
    # Prepara il dataset: seleziona colonne utili e converti il timestamp
    prepared_df = df.select("ProductId", "Time", "Text") \
        .withColumn("Year", year(col("Time")))

    # Job 1
    # Trova i 10 prodotti con il maggior numero di recensioni per anno
    top_products_per_year = prepared_df.groupBy("Year", "ProductId") \
        .count() \
        .withColumnRenamed("count", "TotalReviews") \
        .orderBy("Year", desc("TotalReviews")) \
        .groupBy("Year") \
        .head(10)

    # Estrai parole dal campo "Text" delle recensioni, pulisci e conta le occorrenze
    words_df = prepared_df.select("Year", "ProductId", "Text") \
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

    # Salva il risultato su disco in formato CSV
    final_result.write.csv(f"output/{path}", header=True, mode="overwrite")

    # Stoppa la Spark session
    spark.stop()
