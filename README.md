# Rainfall Big Data Analysis using Hadoop and Apache Spark

## 1. Project Overview

This project performs historical rainfall data analysis using Big Data technologies such as Apache Hadoop, HDFS, Apache Spark, and PySpark.

The objective is to analyze rainfall patterns across different years, months, seasons, and geographical regions of India.

The project demonstrates a complete Big Data workflow including:

- Data storage using HDFS
- Data processing using Apache Spark
- Statistical analysis using PySpark
- Trend analysis
- Data visualization using Python and Matplotlib

---

## 2. Technologies Used

- Apache Hadoop
- Hadoop Distributed File System (HDFS)
- Apache Spark
- PySpark
- Python
- Pandas
- Matplotlib
- Ubuntu WSL

---

## 3. Dataset

The project uses historical Indian rainfall data containing rainfall observations for different geographical subdivisions and years.

### Dataset Information

- Total records analyzed: 4188
- Time period: 1901 - 2017
- Geographical subdivisions: Indian regions/subdivisions
- Monthly rainfall data: January to December
- Annual rainfall data
- Seasonal rainfall data

### Important Columns

- SUBDIVISION
- YEAR
- JAN
- FEB
- MAR
- APR
- MAY
- JUN
- JUL
- AUG
- SEP
- OCT
- NOV
- DEC
- ANNUAL
- JF
- MAM
- JJAS
- OND

---

## 4. Project Architecture

```text
Raw Rainfall Dataset
        |
        v
Apache Hadoop HDFS
        |
        v
Data Cleaning and Processing
        |
        v
Apache Spark / PySpark
        |
        +-----------------------+
        |           |           |
        v           v           v
    Monthly     Seasonal     Regional
    Analysis    Analysis     Analysis
        |           |           |
        +-----------+-----------+
                    |
                    v
              Trend Analysis
                    |
                    v
              Visualization
                    |
                    v
             Project Results
```

---

## 5. Analyses Performed

### 5.1 Monthly Rainfall Analysis

Average rainfall was calculated for each month.

**Wettest Month:** July

**Average Rainfall:** 347.02 mm

**Driest Month:** January

**Average Rainfall:** 18.94 mm

---

### 5.2 Seasonal Rainfall Analysis

The rainfall data was grouped into four seasons:

- Winter: January-February
- Pre-Monsoon: March-May
- Monsoon: June-September
- Post-Monsoon: October-December

**Wettest Season:** Monsoon

**Average Rainfall:** 1063.85 mm

**Driest Season:** Winter

**Average Rainfall:** 40.52 mm

---

### 5.3 Regional Rainfall Analysis

The average annual rainfall was calculated for each geographical subdivision.

### Top 10 Wettest Regions

1. Arunachal Pradesh - 3403.96 mm
2. Coastal Karnataka - 3397.64 mm
3. Konkan & Goa - 2987.53 mm
4. Andaman & Nicobar Islands - 2926.37 mm
5. Kerala - 2914.25 mm
6. Sub Himalayan West Bengal & Sikkim - 2750.55 mm
7. Assam & Meghalaya - 2579.13 mm
8. Naga Mani Mizo Tripura - 2432.72 mm
9. Lakshadweep - 1587.29 mm
10. Gangetic West Bengal - 1490.61 mm

**Driest Region:** West Rajasthan

**Average Annual Rainfall:** 294.13 mm

---

### 5.4 Long-Term Trend Analysis

The historical rainfall trend was analyzed from 1901 to 2017.

**First Year:** 1901

**Average Rainfall in First Year:** 1284.21 mm

**Last Year:** 2017

**Average Rainfall in Last Year:** 1368.01 mm

**Absolute Change:** 83.8 mm

**Percentage Change:** 6.53%

**Fitted Trend Slope:** -0.2808 mm/year

The fitted regression trend is decreasing.

---

## 6. Visualizations

The project contains four major visualizations:

1. Annual Rainfall Trend
2. Monthly Rainfall
3. Seasonal Rainfall
4. Regional Rainfall

All visualizations are stored in:

```text
visualizations/
```

---

## 7. HDFS Outputs

The Spark analysis results are stored in Hadoop HDFS.

```text
/rainfall/output/annual_analysis
/rainfall/output/monthly_analysis
/rainfall/output/seasonal_analysis
/rainfall/output/regional_analysis
/rainfall/output/trend_analysis
```

---

## 8. Project Results

The major findings from the analysis are:

- July is the wettest month with an average rainfall of 347.02 mm.
- January is the driest month with an average rainfall of 18.94 mm.
- The Monsoon season is the wettest season with an average rainfall of 1063.85 mm.
- Winter is the driest season with an average rainfall of 40.52 mm.
- Arunachal Pradesh has the highest average annual rainfall among the analyzed regions with 3403.96 mm.
- Coastal Karnataka is the second wettest region with an average annual rainfall of 3397.64 mm.
- West Rajasthan has the lowest average annual rainfall with 294.13 mm.
- The dataset covers rainfall observations from 1901 to 2017.
- The average rainfall changed from 1284.21 mm in 1901 to 1368.01 mm in 2017.
- The fitted long-term regression trend has a slope of -0.2808 mm/year.

---

## 9. Project Structure

```text
rainfall_project/
│
├── README.md
│
├── scripts/
│   ├── annual_trend_plot.py
│   ├── monthly_analysis.py
│   ├── regional_analysis.py
│   ├── regional_rainfall_plot.py
│   ├── seasonal_analysis.py
│   └── trend_analysis.py
│
├── results/
│   ├── annual_analysis/
│   ├── monthly_analysis/
│   ├── seasonal_analysis/
│   ├── regional_analysis/
│   ├── trend_analysis/
│   └── project_summary.txt
│
└── visualizations/
    ├── annual_rainfall_trend.png
    ├── monthly_rainfall.png
    ├── regional_rainfall.png
    └── seasonal_rainfall.png
```

---

## 10. Big Data Processing Workflow

The project follows these major steps:

1. Upload rainfall data to HDFS.
2. Read the dataset using Apache Spark.
3. Perform data cleaning and transformation.
4. Calculate monthly rainfall statistics.
5. Calculate seasonal rainfall statistics.
6. Calculate regional rainfall statistics.
7. Perform long-term rainfall trend analysis.
8. Store analytical results in HDFS.
9. Generate visualizations using Python and Matplotlib.
10. Organize the final results and project documentation.

---

## 11. Advantages of the Project

This project demonstrates practical applications of Big Data technologies for environmental data analysis.

Key advantages include:

- Distributed data storage using HDFS
- Scalable data processing using Apache Spark
- Parallel processing of rainfall records
- Statistical analysis of historical rainfall patterns
- Regional and seasonal comparison
- Long-term trend analysis
- Automated visualization generation
- Organized storage of analytical results

---

## 12. Conclusion

This project demonstrates the use of Hadoop and Apache Spark for analyzing historical rainfall data across India.

The project successfully performs monthly, seasonal, regional, and long-term trend analysis. Hadoop HDFS is used for distributed data storage, while Apache Spark and PySpark are used for data processing and analysis.

Python and Matplotlib are used to generate visualizations that make the rainfall patterns easier to understand.

Overall, the project demonstrates a complete Big Data analytics workflow from data storage and processing to analysis, visualization, and result generation.

---

## 13. Future Scope

The project can be extended by:

- Implementing rainfall prediction using Machine Learning.
- Using Spark MLlib for predictive modeling.
- Adding interactive dashboards using Power BI or Tableau.
- Performing anomaly detection for extreme rainfall events.
- Integrating real-time rainfall data.
- Comparing rainfall with temperature and other climatic variables.
- Deploying the project on a distributed Hadoop/Spark cluster.

---

## 14. Author

**Sheikh Abdul Shajeel Warsi**

M.Sc. Data Science

Alliance University, Bengaluru
