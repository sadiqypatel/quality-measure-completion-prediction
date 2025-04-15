# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper_4_final_data_all_predictors_12_months")

# COMMAND ----------

df = df.withColumn("censusRegion", 
                        when((col("state").isin(['AZ','HI','ID','MT','NM','NV','UT','WA','WY','CA','CO','AK','OR'])), 'West')
                       .when((col("state").isin(['IL','IN','KS','MI','ND','OH','IA','KS','MN','MS','NE','ND','SD'])), 'Midwest')
                       .when((col("state").isin(['AL','KY','LA','MS','TN','WV','VA','MD','DC','DE','FL','GA','NC','SC','VA','WV','AK','OK','TX'])), 'South')                   
                       .otherwise('Northeast')) 

# Show the number of rows in the sampled DataFrame
print("Number of rows in the sampled DataFrame:", df.count())

# COMMAND ----------

# Import PySpark and create a SparkSession

spark = SparkSession.builder \
        .appName("ColumnPercentages") \
        .getOrCreate()

# Define a function to calculate column percentages for a single categorical column
def calculate_percentages(df, column_name):
    category_counts = df.groupBy(column_name).agg(count("*").alias("Count"))
    total_rows = df.count()
    category_percentages = category_counts.withColumn("Percentage", round((col("Count") / total_rows) * 100, 1))
    return category_percentages
  
# Read the table into a PySpark DataFrame
#df = spark.table("dua_058828_spa240.stage1_final_analysis")
print((df.count(), len(df.columns)))

# List of categorical columns
categorical_columns = ["censusRegion"]

# Calculate and display column percentages for each categorical column
for column_name in categorical_columns:
    print(f"Column Percentages for {column_name}:")
    calculate_percentages(df, column_name).show()
    
# Stop the Spark session
#spark.stop()

# COMMAND ----------

# Import PySpark and create a SparkSession

spark = SparkSession.builder \
        .appName("ColumnPercentages") \
        .getOrCreate()

# Define a function to calculate column percentages for a single categorical column
def calculate_percentages(df, column_name):
    category_counts = df.groupBy(column_name).agg(count("*").alias("Count"))
    total_rows = df.count()
    category_percentages = category_counts.withColumn("Percentage", round((col("Count") / total_rows) * 100, 1))
    return category_percentages
  
# Read the table into a PySpark DataFrame
#df = spark.table("dua_058828_spa240.stage1_final_analysis")
print((df.count(), len(df.columns)))

# List of categorical columns
categorical_columns = ["ageCat", "sex", "race","censusRegion","houseSize","fedPovLine","speakEnglish","married","UsCitizen","ssi","ssdi","tanf","disabled"]

# Calculate and display column percentages for each categorical column
for column_name in categorical_columns:
    print(f"Column Percentages for {column_name}:")
    calculate_percentages(df, column_name).show()
    
# Stop the Spark session
#spark.stop()

# COMMAND ----------

df = df.select(['beneID', 'state', 'saServRate', 'saFacRate', 'mhTreatRate', 'popDensity', 'povRate', 'publicAssistRate', 'highSchoolGradRate', 'goodAirDays','urgentCareRate', '100HeatDays', 'aprnRate'])

# COMMAND ----------

unique_counties = df.select("state").distinct().count()

print(f"Unique number of counties: {unique_counties}")

# COMMAND ----------

fips = spark.table("dua_058828_spa240.paper2_final_sample")
fips = fips.select('beneID','state','fips_code')
fips.show()

# COMMAND ----------

df = df.join(fips, on=['beneID','state'],how='left')
#df.show()

# COMMAND ----------

unique_fips_count1 = df.select("fips_code").distinct().count()
print("Number of unique fips_code values:", unique_fips_count1)

# COMMAND ----------

total_rows = df.count()

print(f"Total number of rows: {total_rows}")

# COMMAND ----------

df = df.drop("beneID","state")
df = df.dropDuplicates(["fips_code"])

# COMMAND ----------

print(df.count())

# COMMAND ----------

# List of continuous variables
variables = ['saServRate', 'mhTreatRate', 'aprnRate', 'urgentCareRate', 'publicAssistRate', 
             'popDensity', 'povRate', 'highSchoolGradRate', 'goodAirDays']

for var in variables:
    # Calculate mean
    mean_value = df.agg(F.mean(F.col(var)).alias("mean")).collect()[0]["mean"]

    # Calculate median using an approximate method
    median_value = df.approxQuantile(var, [0.5], 0.0001)[0]

    # Calculate 25th percentile
    percentile_25 = df.approxQuantile(var, [0.25], 0.0001)[0]

    # Calculate 75th percentile
    percentile_75 = df.approxQuantile(var, [0.75], 0.0001)[0]

    # Print the results
    print(f"Variable: {var}")
    print(f" Mean: {mean_value}")
    print(f" Median: {median_value}")
    print(f" 25th Percentile: {percentile_25}")
    print(f" 75th Percentile: {percentile_75}")
    print()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Import PySpark and create a SparkSession

# Read the table into a PySpark DataFrame
df = spark.table("dua_058828_spa240.finalSample2019")
print((df.count(), len(df.columns)))

# Create a DataFrame from the sample data
total_rows = df.count()

# Group by "state" and calculate the mean of the "month" column
result_df = df.groupBy("state").agg(round(mean("enrolledMonths"), 1).alias("mean_month"))

# Show the result
result_df.show(total_rows, truncate=False)
    
# Stop the Spark session
#spark.stop()

# COMMAND ----------

# Calculate the fraction to sample in order to get approximately 500k rows

fraction = 10000000 / df.count()

#fraction = 10000000 / df.count()

# Take a random sample from the DataFrame
df = df.sample(withReplacement=False, fraction=fraction, seed=42)

# Show the number of rows in the sampled DataFrame
print("Number of rows in the sampled DataFrame:", df.count())

# Continue with your analysis using the sampled DataFrame 'sampled_df'

# COMMAND ----------

# Import PySpark and create a SparkSession

spark = SparkSession.builder \
        .appName("ColumnPercentages") \
        .getOrCreate()

# Define a function to calculate column percentages for a single categorical column
def calculate_percentages(df, column_name):
    category_counts = df.groupBy(column_name).agg(count("*").alias("Count"))
    total_rows = df.count()
    category_percentages = category_counts.withColumn("Percentage", round((col("Count") / total_rows) * 100, 1))
    return category_percentages
  
# Read the table into a PySpark DataFrame
#df = spark.table("dua_058828_spa240.stage1_final_analysis")
print((df.count(), len(df.columns)))

# List of categorical columns
categorical_columns = ["ageCat", "sex", "race","censusRegion","houseSize","fedPovLine","speakEnglish","married","UsCitizen","ssi","ssdi","tanf","disabled"]

# Calculate and display column percentages for each categorical column
for column_name in categorical_columns:
    print(f"Column Percentages for {column_name}:")
    calculate_percentages(df, column_name).show()
    
# Stop the Spark session
#spark.stop()

# COMMAND ----------

