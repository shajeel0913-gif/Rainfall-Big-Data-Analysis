from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    round,
    min,
    max
)

import builtins

# --------------------------------------------------
# CREATE SPARK SESSION
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("Rainfall Trend Analysis") \
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
# YEAR-WISE AVERAGE RAINFALL
# --------------------------------------------------

yearly = df.groupBy("YEAR") \
    .agg(
        round(avg("ANNUAL"), 2).alias("AVERAGE_RAINFALL")
    ) \
    .orderBy("YEAR")


# --------------------------------------------------
# DISPLAY YEARLY RAINFALL
# --------------------------------------------------

print("\n========== YEAR-WISE RAINFALL ==========\n")

yearly.show(30, truncate=False)


# --------------------------------------------------
# GET FIRST AND LAST YEAR
# --------------------------------------------------

first_record = yearly.orderBy("YEAR").first()

last_record = yearly.orderBy(col("YEAR").desc()).first()


first_year = first_record["YEAR"]
first_rainfall = first_record["AVERAGE_RAINFALL"]

last_year = last_record["YEAR"]
last_rainfall = last_record["AVERAGE_RAINFALL"]


# --------------------------------------------------
# CALCULATE CHANGE
# --------------------------------------------------

absolute_change = last_rainfall - first_rainfall

percentage_change = (
    absolute_change / first_rainfall
) * 100


# --------------------------------------------------
# CALCULATE TREND USING COVARIANCE
# --------------------------------------------------

stats = yearly.select(
    avg("YEAR").alias("avg_year"),
    avg("AVERAGE_RAINFALL").alias("avg_rainfall")
).first()

avg_year = stats["avg_year"]
avg_rainfall = stats["avg_rainfall"]


numerator = yearly.select(
    ((col("YEAR") - avg_year) *
     (col("AVERAGE_RAINFALL") - avg_rainfall))
    .alias("numerator")
).agg(
    {"numerator": "sum"}
).first()[0]


denominator = yearly.select(
    ((col("YEAR") - avg_year) *
     (col("YEAR") - avg_year))
    .alias("denominator")
).agg(
    {"denominator": "sum"}
).first()[0]


slope = numerator / denominator


# --------------------------------------------------
# DETERMINE TREND
# --------------------------------------------------

if slope > 0:
    trend = "INCREASING"
elif slope < 0:
    trend = "DECREASING"
else:
    trend = "STABLE"


# --------------------------------------------------
# DISPLAY TREND RESULTS
# --------------------------------------------------

print("\n========== RAINFALL TREND ANALYSIS ==========\n")

print("First Year:", first_year)
print("Average Rainfall in First Year:", first_rainfall, "mm")

print("\nLast Year:", last_year)
print("Average Rainfall in Last Year:", last_rainfall, "mm")

print("\nAbsolute Change:", builtins.round(absolute_change, 2), "mm")

print("Percentage Change:", builtins.round(percentage_change, 2), "%")

print("\nTrend Slope:", builtins.round(slope, 4), "mm/year")

print("Overall Trend:", trend)


# --------------------------------------------------
# SAVE YEARLY TREND DATA
# --------------------------------------------------

output_path = "hdfs://localhost:9000/rainfall/output/trend_analysis"

yearly \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)


# --------------------------------------------------
# COMPLETION
# --------------------------------------------------

print("\n========== TREND ANALYSIS COMPLETE ==========")

print("Output saved to:")
print(output_path)


# --------------------------------------------------
# STOP SPARK
# --------------------------------------------------

spark.stop()
