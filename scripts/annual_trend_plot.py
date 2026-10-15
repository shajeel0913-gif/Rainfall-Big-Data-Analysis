import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

# Find the Spark output CSV
files = glob.glob(
    os.path.expanduser(
        "~/rainfall_visualizations/part-*.csv"
    )
)

if not files:
    raise FileNotFoundError("Trend CSV file not found.")

file_path = files[0]

# Read data
df = pd.read_csv(file_path)

# Sort by year
df = df.sort_values("YEAR")

# Plot
plt.figure(figsize=(12, 6))

plt.plot(
    df["YEAR"],
    df["AVERAGE_RAINFALL"],
    linewidth=1.5
)

# Add trend line
import numpy as np

x = df["YEAR"].values
y = df["AVERAGE_RAINFALL"].values

slope, intercept = np.polyfit(x, y, 1)

trend_line = slope * x + intercept

plt.plot(
    x,
    trend_line,
    linestyle="--",
    linewidth=2,
    label=f"Trend line ({slope:.4f} mm/year)"
)

plt.title(
    "Annual Rainfall Trend in India (1901–2017)",
    fontsize=16
)

plt.xlabel("Year")
plt.ylabel("Average Annual Rainfall (mm)")

plt.grid(True, alpha=0.3)

plt.legend()

plt.tight_layout()

# Save graph
output = os.path.expanduser(
    "~/rainfall_visualizations/annual_rainfall_trend.png"
)

plt.savefig(output, dpi=300)

print("Graph saved to:")
print(output)

plt.show()
