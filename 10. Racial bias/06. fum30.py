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

# COMMAND ----------

test_df = spark.table("dua_058828_spa240.paper_4_fum30_test_xgboost_new9")
print(test_df.count())
df = spark.table("dua_058828_spa240.paper_4_final_data_all_predictors_12_months")
df = df.select("beneID","state","race")

# COMMAND ----------

# Perform the left merge on 'beneID' and 'state' columns
print((test_df.count(), len(test_df.columns)))
test_df = test_df.join(df, on=['beneID', 'state'], how='inner')
print((test_df.count(), len(test_df.columns)))
# Show the merged DataFrame
test_df.show()

# COMMAND ----------

# Count the occurrences of each race
race_counts = test_df.groupBy("race").agg(count("*").alias("count"))

# Show the results
race_counts.show()

# COMMAND ----------

# Create the new "race_category" column based on the conditions
df = test_df.withColumn(
    "race_category",
    when(col("race") == "white", "White")
    .when(col("race") == "missing", "missing")
    .when(col("race") == "black", "Black")
    .when(col("race") == "hispanic", "Hispanic")
    .otherwise("minority")
)

# Count the occurrences of each race
race_counts = df.groupBy("race_category").agg(count("*").alias("count"))

# Show the results
race_counts.show()

# COMMAND ----------

#df = df.select("beneID","state","race_category","prediction","outcome")

# COMMAND ----------

from pyspark.sql.functions import avg
from pyspark.sql.functions import sum, when, col

# Calculate specificity and sensitivity by race_category
metrics_by_race = df.groupBy("race_category").agg(
    (sum(when((col("prediction") == 1) & (col("outcome") == 1), 1))
     / sum(when(col("outcome") == 1, 1))).alias("sensitivity"),
    (sum(when((col("prediction") == 0) & (col("outcome") == 0), 1))
     / sum(when(col("outcome") == 0, 1))).alias("specificity")
)

# Show the results
metrics_by_race.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, sum

# Compute FPR and FNR by race
metrics_by_race = df.groupBy("race_category").agg(
    # FPR = FP / (FP + TN)
    (sum(((col("prediction") == 1) & (col("outcome") == 0)).cast("double")) /
     sum((col("outcome") == 0).cast("double"))).alias("FPR"),
    
    # FNR = FN / (FN + TP)
    (sum(((col("prediction") == 0) & (col("outcome") == 1)).cast("double")) /
     sum((col("outcome") == 1).cast("double"))).alias("FNR")
)

metrics_by_race.show()

# COMMAND ----------

import pandas as pd
from sklearn.metrics import roc_auc_score

# Step 1: Convert to pandas and extract class 1 probability
df_small = (
    df.select("race_category", "probability", "outcome")
      .where(col("race_category").isNotNull())
      .toPandas()
)

# Step 2: Extract p1 from DenseVector
df_small["prob_1"] = df_small["probability"].apply(lambda x: float(x[1]))

# Step 3: Drop rows missing needed values
df_small = df_small.dropna(subset=["race_category", "prob_1", "outcome"])

# Step 4: Define AUROC function
def compute_auc(group):
    if len(group["outcome"].unique()) < 2:
        return None  # Not enough variation to compute AUC
    return roc_auc_score(group["outcome"], group["prob_1"])

# Step 5: Compute AUROC by race
auroc_by_race = (
    df_small.groupby("race_category")
    .apply(compute_auc)
    .reset_index()
    .rename(columns={0: "AUROC"})
)

# Step 6: Round
auroc_by_race["AUROC"] = auroc_by_race["AUROC"].round(4)

# Show result
print(auroc_by_race)

# COMMAND ----------

import numpy as np
from scipy import stats
from pyspark.sql import functions as F

def calculate_sensitivity_specificity(df, prediction_col, truth_col, race_category):
    true_positive = df.filter((df[prediction_col] == 1) & (df[truth_col] == 1) & (df['race_category'] == race_category)).count()
    false_positive = df.filter((df[prediction_col] == 1) & (df[truth_col] == 0) & (df['race_category'] == race_category)).count()
    true_negative = df.filter((df[prediction_col] == 0) & (df[truth_col] == 0) & (df['race_category'] == race_category)).count()
    false_negative = df.filter((df[prediction_col] == 0) & (df[truth_col] == 1) & (df['race_category'] == race_category)).count()

    if (true_positive + false_negative) == 0:
        sensitivity = 0
    else:
        sensitivity = true_positive / (true_positive + false_negative)

    if (true_negative + false_positive) == 0:
        specificity = 0
    else:
        specificity = true_negative / (true_negative + false_positive)
    
    return sensitivity, specificity

def calculate_mean_confidence_interval(data):
    mean = np.mean(data)
    if len(data) > 1:
        confidence_interval = stats.norm.interval(0.95, loc=mean, scale=stats.sem(data))
    else:
        confidence_interval = (mean, mean)
    return mean, confidence_interval

# Example usage
race_categories = ['minority', 'missing', 'White', 'Black', 'Hispanic']  # Replace with your actual race categories

num_bootstraps = 100

results = {}

for race_category in race_categories:
    sensitivities = []
    specificities = []

    for _ in range(num_bootstraps):
        bootstrap_sample = df.sample(withReplacement=True, fraction=0.1)

        sensitivity, specificity = calculate_sensitivity_specificity(bootstrap_sample, 'prediction', 'outcome', race_category)
        sensitivities.append(sensitivity)
        specificities.append(specificity)

    mean_sensitivity, ci_sensitivity = calculate_mean_confidence_interval(sensitivities)
    mean_specificity, ci_specificity = calculate_mean_confidence_interval(specificities)

    results[race_category] = {
        'mean_sensitivity': mean_sensitivity,
        'ci_sensitivity': ci_sensitivity,
        'mean_specificity': mean_specificity,
        'ci_specificity': ci_specificity
    }

# Print the results
for race_category, metrics in results.items():
    print(f"Race Category: {race_category}")
    print(f"Mean Sensitivity: {metrics['mean_sensitivity']}")
    print(f"Sensitivity CI: {metrics['ci_sensitivity']}")
    print(f"Mean Specificity: {metrics['mean_specificity']}")
    print(f"Specificity CI: {metrics['ci_specificity']}")
    print("-------------")

# COMMAND ----------

