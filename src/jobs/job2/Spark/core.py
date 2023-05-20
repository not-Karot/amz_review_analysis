from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, udf
from pyspark.sql.types import FloatType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Amazon Review Analysis - Job 2") \
    .getOrCreate()

path = '${INPUT}'
if path.endswith(".csv"):
    df = spark.read.csv(path, header=True, sep="\t")

    # Calculate utility
    def calculate_utility(numerator, denominator):
        return numerator / denominator if denominator != 0 else 0

    calculate_utility_udf = udf(calculate_utility, FloatType())
    df = df.withColumn('utility', calculate_utility_udf(col('HelpfulnessNumerator'), col('HelpfulnessDenominator')))

    # Calculate average utility for each user
    user_utility_avg = df.groupBy('UserId').avg('utility')

    # Order users by average utility in descending order
    sorted_users = user_utility_avg.orderBy(col('avg(utility)').desc())

    #Write DataFrame to CSV file
    sorted_users.write.csv("${OUTPUT}")

    # Stop the Spark session
    spark.stop()
