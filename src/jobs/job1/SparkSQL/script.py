import glob
from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import year, split, explode, lower, length, desc, row_number

folder_path = './input'


def read_job_files(job_number: str):
    folder = Path(folder_path)
    job_files = glob.glob(str(folder / f'*{job_number}*'))
    return job_files


spark = SparkSession.builder \
    .appName("Amazon Review Analysis - Job 1") \
    .getOrCreate()

paths = read_job_files('job1')
for path in paths:
    if path.endswith(".csv"):
        df = spark.read.csv(path, header=True, sep="\t")
        df = df.withColumn("Year", year(df.Time))


        df.createOrReplaceTempView("reviews")


        top_products_per_year = spark.sql(
            """
            SELECT Year, ProductId, COUNT(*) as TotalReviews 
            FROM reviews 
            GROUP BY Year, ProductId  
            ORDER BY Year, TotalReviews DESC
            LIMIT 10
            """
        )

        words_df = spark.sql(
            """
            SELECT Year, ProductId, explode(split(lower(Text), '\\W+')) as Word 
            FROM reviews 
            WHERE length(Word) >= 4
            """
        )

        words_df.createOrReplaceTempView("words")

        word_counts = spark.sql(
            """
            SELECT Year, ProductId, Word, COUNT(*) as Occurrences 
            FROM words 
            GROUP BY Year, ProductId, Word
            """
        )

        window_spec = Window.partitionBy(word_counts.Year, word_counts.ProductId).orderBy(desc(word_counts.Occurrences))
        top_words_per_product = word_counts.withColumn("Rank", row_number().over(window_spec)).filter("Rank <= 5").drop("Rank")

        final_result = top_products_per_year.join(top_words_per_product, ["Year", "ProductId"]).orderBy("Year", desc("TotalReviews"), "ProductId", desc("Occurrences"))

        final_result.write.csv("${OUTPUT}")

        spark.stop()
