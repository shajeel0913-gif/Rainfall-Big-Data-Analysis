from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round
import matplotlib.pyplot as plt
import os

# ============================================================
# REGIONAL RAINFALL VISUALIZATION
# ============================================================

spark = SparkSession.builder \
    .appName("RegionalRainfallVisualization") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# HDFS input path
input_path = "hdfs://localhost:9000/rainfall/processed/rainfall_cleaned"

# Output directory
output_dir = os.path.expanduser("~/rainfall_visualizations")
os.makedirs(output_dir, exist_ok=True)

print("\n========== REGIONAL RAINFALL VISUALIZATION ==========\n")

# ------------------------------------------------------------
# Read CSV data from HDFS
# ------------------------------------------------------------

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("Data loaded successfully.")

# Show columns
print("\nAvailable columns:")
print(df.columns)

# ------------------------------------------------------------
# Calculate annual rainfall by region
# ------------------------------------------------------------

regional_df = df.groupBy("SUBDIVISION") \
    .agg(
        spark_round(
            avg(
                col("ANNUAL")
            ),
            2
        ).alias("AVERAGE_ANNUAL_RAINFALL")
    ) \
    .orderBy(
        col("AVERAGE_ANNUAL_RAINFALL").desc()
    )

# ------------------------------------------------------------
# Get Top 10 Wettest Regions
# ------------------------------------------------------------

top10 = regional_df.limit(10)

print("\n========== TOP 10 WETTEST REGIONS ==========\n")
top10.show(truncate=False)

# Convert Spark DataFrame to Pandas
pandas_df = top10.toPandas()

# ------------------------------------------------------------
# Create plot
# ------------------------------------------------------------

plt.figure(figsize=(12, 7))

plt.barh(
    pandas_df["SUBDIVISION"],
    pandas_df["AVERAGE_ANNUAL_RAINFALL"]
)

plt.xlabel("Average Annual Rainfall (mm)")
plt.ylabel("Region")
plt.title("Top 10 Wettest Regions in India")

plt.gca().invert_yaxis()

plt.tight_layout()

# ------------------------------------------------------------
# Save graph
# ------------------------------------------------------------

output_file = os.path.join(
    output_dir,
    "regional_rainfall.png"
)

plt.savefig(
    output_file,
    dpi=300,
    bbox_inches="tight"
)

plt.close()

print("\nGraph saved to:")
print(output_file)

print("\n========== REGIONAL VISUALIZATION COMPLETE ==========\n")

spark.stop()
