from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("Monthly Rainfall Analysis") \
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
# 3. Calculate average rainfall for each month
# --------------------------------------------------

months = [
    "JAN", "FEB", "MAR", "APR", "MAY",
    "JUN", "JUL", "AUG", "SEP", "OCT",
    "NOV", "DEC"
]

print("\n========== MONTHLY RAINFALL ANALYSIS ==========")

monthly_results = []

for month in months:

    result = df.select(
        avg(month).alias("AVERAGE_RAINFALL")
    ).collect()[0]

    average_rainfall = result["AVERAGE_RAINFALL"]

    monthly_results.append(
        (month, average_rainfall)
    )

# --------------------------------------------------
# 4. Display results
# --------------------------------------------------

print("\nMonth\tAverage Rainfall (mm)")
print("--------------------------------")

for month, rainfall in monthly_results:
    print(f"{month}\t{rainfall:.2f}")

# --------------------------------------------------
# 5. Find wettest and driest months
# --------------------------------------------------

wettest_month = max(
    monthly_results,
    key=lambda x: x[1]
)

driest_month = min(
    monthly_results,
    key=lambda x: x[1]
)

print("\n========== WETTEST MONTH ==========")
print(
    f"{wettest_month[0]} : {wettest_month[1]:.2f} mm"
)

print("\n========== DRIEST MONTH ==========")
print(
    f"{driest_month[0]} : {driest_month[1]:.2f} mm"
)

# --------------------------------------------------
# 6. Save results to HDFS
# --------------------------------------------------

result_df = spark.createDataFrame(
    monthly_results,
    ["MONTH", "AVERAGE_RAINFALL"]
)

output_path = "hdfs://localhost:9000/rainfall/output/monthly_analysis"

result_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("\n========== MONTHLY ANALYSIS COMPLETE ==========")
print("Output saved to:")
print(output_path)

spark.stop()
