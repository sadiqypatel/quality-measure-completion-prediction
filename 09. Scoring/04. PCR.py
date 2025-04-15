# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np 
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from pyspark.sql import SparkSession

# COMMAND ----------

import mlflow
from pyspark.sql.types import DoubleType
import warnings

warnings.filterwarnings("ignore")

#logged_model = 'dbfs:/databricks/mlflow-tracking/3067102219110915/2d3e349a5daa4e269abf6e7ec685bdd3/artifacts/prenatal_model_new.tar.gz'

logged_model = 'runs:/b130bc604c0c477a9a5efe2f41bb6787/pcr_model_new1.tar.gz'
model = mlflow.spark.load_model(logged_model)

# COMMAND ----------

vector = spark.table("dua_058828_spa240.paper4_demo_all_features_vector_new1")
outcome = spark.table("dua_058828_spa240.paper_4_pcr_outcome_12_months")

final_original = vector.join(outcome, on=["beneID","state"], how="inner")
#print(final_original.count())
final_original = final_original.withColumn("outcome", when(final_original.readmission_yes == 1, 1).otherwise(0))
#final_original.show()

from pyspark.sql import DataFrame

# Loop through each file (1 to 13) and create final_updated_1 to final_updated_13
for i in range(1, 14):
    # Load each original DataFrame
    df = spark.table(f"dua_058828_spa240.paper4_original_df_changed_sdoh_vector_{i}")
    
    # Perform the join and column update (same logic as your original code)
    final_updated = df.join(outcome, on=["beneID", "state"], how="inner")
    final_updated = final_updated.withColumn("outcome", when(final_updated.readmission_yes == 1, 1).otherwise(0))
    
    # Dynamically assign the result to a variable (e.g., final_updated_1, final_updated_2, ...)
    globals()[f"final_updated_{i}"] = final_updated

    # Optionally print or show something to verify
    print(f"final_updated_{i} created")

    # Load model as a Spark UDF (for inferencing)
scored_original = model.transform(final_original)

# Loop through final_updated_1 to final_updated_13 and create scored_updated_1 to scored_updated_13
for i in range(1, 14):
    # Dynamically get each final_updated DataFrame
    final_updated_df = globals()[f"final_updated_{i}"]
    
    # Apply the model transformation (same as in your screenshot)
    scored_updated_df = model.transform(final_updated_df)
    
    # Dynamically assign the result to a new DataFrame (e.g., scored_updated_1, scored_updated_2, ...)
    globals()[f"scored_updated_{i}"] = scored_updated_df

    # Optionally print or show something to verify
    print(f"scored_updated_{i} created")

# COMMAND ----------

# Assume scored_original is already defined somewhere in your environment
df_original = scored_original

# Select specific columns and rename the prediction column in df_original
df_original = df_original.select("beneID", "state", "outcome", "prediction")
df_original = df_original.withColumnRenamed("prediction", "prediction_original")

# Loop through scored_updated_1 to scored_updated_13
for i in range(1, 14):
    # Dynamically get each scored_updated DataFrame
    scored_updated_df = globals()[f"scored_updated_{i}"]
    
    # Select and rename columns for each updated DataFrame
    df_updated = scored_updated_df.select("beneID", "state", "prediction")
    df_updated = df_updated.withColumnRenamed("prediction", "prediction_updated")
    
    # Join with df_original
    df = df_original.join(df_updated, on=["beneID", "state"], how="left")
    
    # Dynamically assign the resulting DataFrame to a variable
    globals()[f"df_{i}"] = df
    
    # Optionally print or show something to verify
    print(f"df_{i} created")

# COMMAND ----------

for i in range(7, 8):
    df = globals()[f"df_{i}"]
    
    # Cache filtered DataFrame to avoid recomputing the same filter multiple times
    original_open = df.filter(df.prediction_original == 1).count()
    updated_open= df.filter(df.prediction_updated == 1).count()
    
    # Calculate percentage change using cached result
    pp= (original_open - updated_open) / df.count() *100
    pc = (original_open - updated_open) / original_open * 100
    
    # Print the result
    print(f"Percentage change in open gaps after changing SDOH for df_{i}: {pc}%")
    print(f"Percentage point change in open gaps after changing SDOH for df_{i}: {pp}%")