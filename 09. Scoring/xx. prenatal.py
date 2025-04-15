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

sample = spark.table("dua_058828_spa240.paper4_final_analysis_sample")
# print(sample.count())

# COMMAND ----------

vector = spark.table("dua_058828_spa240.paper4_demo_all_features_vector_new1")
outcome = spark.table("dua_058828_spa240.paper_4_post_partum_care_12_months_new")

final = vector.join(outcome, on=["beneID","state"], how="inner")
final = final.join(sample, on=["beneID","state"], how="inner")
print(final.count())

# COMMAND ----------

import mlflow
from pyspark.sql.types import DoubleType
import warnings

warnings.filterwarnings("ignore")

logged_model = 'dbfs:/databricks/mlflow-tracking/771013923553983/a017a91d828a4d94a7f36e56eff6210b/artifacts/prenatal_model'

# Load model as a Spark UDF (for inferencing)
model = mlflow.spark.load_model(logged_model)
scored = model.transform(final)

# COMMAND ----------

scored = model.transform(final)

# COMMAND ----------

display(scored)

# COMMAND ----------

scored.write.saveAsTable("dua_058828_spa240.paper_4_prenatal_score_original", mode='overwrite')

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper_4_prenatal_score_original")
df.show()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.ml.linalg import VectorUDT
 
# Define a UDF to extract the second element of the vector
@udf(returnType=FloatType())
def extract_second_element(vector):
    return float(vector[1])
 
# Apply the UDF to the 'probability' column and create a new column 'probability_2nd_value'
scored_df = df.withColumn('outcome_probability', extract_second_element('probability'))
scored_df.show()

# COMMAND ----------

# Select specific columns
scored_df = scored_df.select('beneID', 'state', 'outcome_probability', 'probability', 'rawPrediction', 'prediction','prenatal')
scored_df.show()

# COMMAND ----------

total_count = scored_df.count()
zero_count = scored_df.filter(scored_df.prediction == 0.0).count()

percentage_zero = (zero_count / total_count) * 100
print(percentage_zero)

# COMMAND ----------

from pyspark.sql import functions as F

percentiles = [0.1, 0.25, 0.5, 0.75, 0.9]

# Calculate percentiles
percentile_values = scored_df.approxQuantile("probability", percentiles, 0.01)

# Calculate min and max
min_value = scored_df.agg(F.min("probability")).collect()[0][0]
max_value = scored_df.agg(F.max("probability")).collect()[0][0]

# Combine results
summary = {
    'min': min_value,
    '10%': percentile_values[0],
    '25%': percentile_values[1],
    '50%': percentile_values[2],
    '75%': percentile_values[3],
    '90%': percentile_values[4],
    'max': max_value
}

summary

# COMMAND ----------

