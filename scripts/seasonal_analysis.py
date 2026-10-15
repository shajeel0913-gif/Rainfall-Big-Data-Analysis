from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max, stddev

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("Seasonal Rainfall Analysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 2. Read cleaned data from HDFS
# --------------------------------------------------

input_path = "hdfs://localhost:9000/rainfall/processed/rainfall_cleaned"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

# --------------------------------------------------
# 3. Define seasonal columns
# --------------------------------------------------

seasons = {
    "JF": "Winter (January-February)",
    "MAM": "Pre-Monsoon (March-May)",
    "JJAS": "Monsoon (June-September)",
    "OND": "Post-Monsoon (October-December)"
}

# --------------------------------------------------
# 4. Calculate seasonal averages
# --------------------------------------------------

results = []

for column, season_name in seasons.items():

    result = df.select(
        avg(column).alias("AVERAGE_RAINFALL"),
        min(column).alias("MIN_RAINFALL"),
        max(column).alias("MAX_RAINFALL"),
        stddev(column).alias("STDDEV_RAINFALL")
    ).collect()[0]

    results.append(
        (
            column,
            season_name,
            result["AVERAGE_RAINFALL"],
            result["MIN_RAINFALL"],
            result["MAX_RAINFALL"],
            result["STDDEV_RAINFALL"]
        )
    )

# --------------------------------------------------
# 5. Display results
# --------------------------------------------------

print("\n========== SEASONAL RAINFALL ANALYSIS ==========")

print(
    "\nPeriod\tSeason\t\t\tAverage\tMinimum\tMaximum"
)

print("-" * 85)

for row in results:

    period = row[0]
    season = row[1]
    average = row[2]
    minimum = row[3]
    maximum = row[4]

    print(
        f"{period}\t{season:28s}"
        f"{average:.2f}\t"
        f"{minimum:.2f}\t"
        f"{maximum:.2f}"
    )

# --------------------------------------------------
# 6. Find wettest and driest seasons
# --------------------------------------------------

wettest = __builtins__.max(
    results,
    key=lambda x: x[2]
)

driest = __builtins__.min(
    results,
    key=lambda x: x[2]
)

print("\n========== WETTEST SEASON ==========")

print(
    f"{wettest[1]} ({wettest[0]}) "
    f": {wettest[2]:.2f} mm"
)

print("\n========== DRIEST SEASON ==========")

print(
    f"{driest[1]} ({driest[0]}) "
    f": {driest[2]:.2f} mm"
)

# --------------------------------------------------
# 7. Save results to HDFS
# --------------------------------------------------

result_df = spark.createDataFrame(
    results,
    [
        "PERIOD",
        "SEASON",
        "AVERAGE_RAINFALL",
        "MIN_RAINFALL",
        "MAX_RAINFALL",
        "STDDEV_RAINFALL"
    ]
)

output_path = "hdfs://localhost:9000/rainfall/output/seasonal_analysis"

result_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("\n========== SEASONAL ANALYSIS COMPLETE ==========")

print("Output saved to:")
print(output_path)

spark.stop()
