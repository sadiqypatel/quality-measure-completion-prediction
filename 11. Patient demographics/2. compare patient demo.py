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

df = df.withColumn("original", F.lit(1))

# COMMAND ----------

df1 = spark.table("dua_058828_spa240.paper_4_demo_file_at_least_1_month")
df1 = df1.withColumn("original", F.lit(0))

df1 = df1.withColumn("censusRegion", 
                        when((col("state").isin(['AZ','HI','ID','MT','NM','NV','UT','WA','WY','CA','CO','AK','OR'])), 'West')
                       .when((col("state").isin(['IL','IN','KS','MI','ND','OH','IA','KS','MN','MS','NE','ND','SD'])), 'Midwest')
                       .when((col("state").isin(['AL','KY','LA','MS','TN','WV','VA','MD','DC','DE','FL','GA','NC','SC','VA','WV','AK','OK','TX'])), 'South')                   
                       .otherwise('Northeast')) 

# Show the number of rows in the sampled DataFrame
print("Number of rows in the sampled DataFrame:", df1.count())

# COMMAND ----------

# List of categorical columns
categorical_columns = ["beneID","state","ageCat", "sex", "race", "censusRegion", "houseSize", "fedPovLine", 
                       "speakEnglish", "married", "USCitizen", "ssi", "ssdi", "tanf", "disabled","original"]

# Select only the categorical columns
df = df.select(categorical_columns)
df1 = df1.select(categorical_columns)

# COMMAND ----------

df.show()

# COMMAND ----------

df1.show()

# COMMAND ----------

final = df.union(df1)
print(final.count())

# COMMAND ----------

df = final

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# List of categorical columns
categorical_columns = ["ageCat", "sex", "race", "censusRegion", "houseSize", "fedPovLine", 
                       "speakEnglish", "married", "USCitizen", "ssi", "ssdi", "tanf", "disabled"]

# Function to calculate standardized difference for a categorical variable
def calculate_standardized_difference(df, column):
    # Calculate counts and proportions for original=1 and original=0
    group_counts = df.groupBy(column, "original").count()
    
    # Calculate the proportion for original=1
    p1 = group_counts.filter(F.col("original") == 1).withColumnRenamed("count", "count_1")
    p1 = p1.withColumn("p1", F.col("count_1") / F.sum("count_1").over(Window.partitionBy()))

    # Calculate the proportion for original=0
    p0 = group_counts.filter(F.col("original") == 0).withColumnRenamed("count", "count_0")
    p0 = p0.withColumn("p0", F.col("count_0") / F.sum("count_0").over(Window.partitionBy()))

    # Join p1 and p0 by the categorical variable
    joined = p1.join(p0, on=column, how="inner")
    
    # Calculate the standardized difference
    standardized_diff = joined.withColumn(
        "std_diff",
        (F.col("p1") - F.col("p0")) / F.sqrt(((F.col("p1") * (1 - F.col("p1"))) + (F.col("p0") * (1 - F.col("p0")))) / 2)
    )
    
    # Select only relevant columns
    result = standardized_diff.select(column, "p1", "p0", "std_diff")
    return result

# Calculate standardized difference for all categorical variables
std_diff_results = [calculate_standardized_difference(df, col) for col in categorical_columns]

# Display results for each variable
for result in std_diff_results:
    result.show()


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



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
variables = ['saFacRate', 'mhTreatRate', 'aprnRate','urgentCareRate','publicAssistRate', 'popDensity', 'povRate', 'highSchoolGradRate','goodAirDays']

for var in variables:
    # Calculate mean
    mean_value = df.agg(F.mean(F.col(var)).alias('mean')).collect()[0]['mean']
    
    # Calculate median using an approximate method
    median_value = df.approxQuantile(var, [0.5], 0.0001)[0]
    
    # Calculate 25th percentile
    percentile_25 = df.approxQuantile(var, [0.25], 0.0001)[0]
    
    # Calculate 75th percentile
    percentile_75 = df.approxQuantile(var, [0.75], 0.0001)[0]
    
    # Print the results
    print(f"Variable: {var}")
    print(f"  Mean: {mean_value}")
    print(f"  Median: {median_value}")
    print(f"  25th Percentile: {percentile_25}")
    print(f"  75th Percentile: {percentile_75}")
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

