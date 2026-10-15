from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    min,
    max,
    round
)

# --------------------------------------------------
# CREATE SPARK SESSION
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("Rainfall Regional Analysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# READ CLEANED DATA FROM HDFS
# --------------------------------------------------

input_path = "hdfs://localhost:9000/rainfall/processed/rainfall_cleaned"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)


# --------------------------------------------------
# DISPLAY DATA
# --------------------------------------------------

print("\n========== REGIONAL RAINFALL ANALYSIS ==========\n")

print("Total Records:", df.count())

print("\nAvailable Regions/Subdivisions:\n")

df.select("SUBDIVISION") \
    .distinct() \
    .orderBy("SUBDIVISION") \
    .show(50, truncate=False)


# --------------------------------------------------
# CALCULATE REGIONAL STATISTICS
# --------------------------------------------------

regional = df.groupBy("SUBDIVISION") \
    .agg(
        round(avg("ANNUAL"), 2).alias("AVERAGE_ANNUAL_RAINFALL"),
        round(min("ANNUAL"), 2).alias("MIN_ANNUAL_RAINFALL"),
        round(max("ANNUAL"), 2).alias("MAX_ANNUAL_RAINFALL")
    )


# --------------------------------------------------
# TOP 10 WETTEST REGIONS
# --------------------------------------------------

print("\n========== TOP 10 WETTEST REGIONS ==========\n")

wettest = regional \
    .orderBy(col("AVERAGE_ANNUAL_RAINFALL").desc()) \
    .limit(10)

wettest.show(truncate=False)


# --------------------------------------------------
# TOP 10 DRIEST REGIONS
# --------------------------------------------------

print("\n========== TOP 10 DRIEST REGIONS ==========\n")

driest = regional \
    .orderBy(col("AVERAGE_ANNUAL_RAINFALL").asc()) \
    .limit(10)

driest.show(truncate=False)


# --------------------------------------------------
# SAVE RESULTS TO HDFS
# --------------------------------------------------

output_path = "hdfs://localhost:9000/rainfall/output/regional_analysis"

regional \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)


# --------------------------------------------------
# COMPLETION MESSAGE
# --------------------------------------------------

print("\n========== REGIONAL ANALYSIS COMPLETE ==========")

print("Output saved to:")
print(output_path)


# --------------------------------------------------
# STOP SPARK
# --------------------------------------------------

spark.stop()
