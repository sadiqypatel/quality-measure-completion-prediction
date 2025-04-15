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

test = spark.table("dua_058828_spa240.paper_4_wcv_test_xgboost_new1")
test.show()
print(test.count())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Function to compute metrics for a DataFrame
def compute_metrics(df):
    metrics = df.agg(
        F.sum(F.when((F.col("random_prediction") == 1) & (F.col("outcome") == 1), 1).otherwise(0)).alias("TP"),
        F.sum(F.when((F.col("random_prediction") == 0) & (F.col("outcome") == 0), 1).otherwise(0)).alias("TN"),
        F.sum(F.when((F.col("random_prediction") == 1) & (F.col("outcome") == 0), 1).otherwise(0)).alias("FP"),
        F.sum(F.when((F.col("random_prediction") == 0) & (F.col("outcome") == 1), 1).otherwise(0)).alias("FN")
    ).collect()[0]
    
    TP, TN, FP, FN = metrics.TP, metrics.TN, metrics.FP, metrics.FN

    # Calculate metrics
    accuracy = (TP + TN) / (TP + TN + FP + FN)
    precision = TP / (TP + FP) if TP + FP > 0 else 0  # PPV
    recall = TP / (TP + FN) if TP + FN > 0 else 0     # Sensitivity
    specificity = TN / (TN + FP) if TN + FP > 0 else 0
    npv = TN / (TN + FN) if TN + FN > 0 else 0
    f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    mcc = ((TP * TN) - (FP * FN)) / (
        ((TP + FP) * (TP + FN) * (TN + FP) * (TN + FN)) ** 0.5
    ) if (TP + FP) > 0 and (TP + FN) > 0 and (TN + FP) > 0 and (TN + FN) > 0 else 0

    return {
        "accuracy": accuracy,
        "precision": precision,  # PPV
        "recall": recall,        # Sensitivity
        "specificity": specificity,
        "npv": npv,
        "f1_score": f1_score,
        "mcc": mcc
    }

# Function to calculate AUC for random predictions
def calculate_auc(df):
    evaluator = BinaryClassificationEvaluator(
        labelCol="outcome", rawPredictionCol="random_prob", metricName="areaUnderROC"
    )
    auc = evaluator.evaluate(df)
    return auc

# Bootstrap sampling and metrics calculation
n_bootstrap = 500  # Number of bootstrap iterations
bootstrap_results = []

for _ in range(n_bootstrap):
    # Generate new random probabilities for each row
    test_with_random = test.withColumn("random_prob", F.rand(seed=None)) \
                           .withColumn("random_prediction", F.when(F.col("random_prob") > 0.5, 1).otherwise(0))
    
    # Sample with replacement
    sample_df = test_with_random.sample(withReplacement=False, fraction=1.00)
    
    # Compute metrics for the sample
    metrics = compute_metrics(sample_df)
    
    # Add AUC for random predictions
    auc = calculate_auc(sample_df)
    metrics["auc"] = auc
    
    # Append to results
    bootstrap_results.append(metrics)

# Convert bootstrap results to a DataFrame
bootstrap_df = spark.createDataFrame(bootstrap_results)

# Calculate mean and confidence intervals
summary = bootstrap_df.select(
    F.mean("auc").alias("auc_mean"),
    F.expr("percentile_approx(auc, 0.025)").alias("auc_lower_ci"),
    F.expr("percentile_approx(auc, 0.975)").alias("auc_upper_ci"),
    F.mean("accuracy").alias("accuracy_mean"),
    F.expr("percentile_approx(accuracy, 0.025)").alias("accuracy_lower_ci"),
    F.expr("percentile_approx(accuracy, 0.975)").alias("accuracy_upper_ci"),
    F.mean("mcc").alias("mcc_mean"),
    F.expr("percentile_approx(mcc, 0.025)").alias("mcc_lower_ci"),
    F.expr("percentile_approx(mcc, 0.975)").alias("mcc_upper_ci"),
    F.mean("f1_score").alias("f1_mean"),
    F.expr("percentile_approx(f1_score, 0.025)").alias("f1_lower_ci"),
    F.expr("percentile_approx(f1_score, 0.975)").alias("f1_upper_ci"),
    F.mean("recall").alias("sensitivity_mean"),
    F.expr("percentile_approx(recall, 0.025)").alias("sensitivity_lower_ci"),
    F.expr("percentile_approx(recall, 0.975)").alias("sensitivity_upper_ci"),
    F.mean("specificity").alias("specificity_mean"),
    F.expr("percentile_approx(specificity, 0.025)").alias("specificity_lower_ci"),
    F.expr("percentile_approx(specificity, 0.975)").alias("specificity_upper_ci"),
    F.mean("npv").alias("npv_mean"),
    F.expr("percentile_approx(npv, 0.025)").alias("npv_lower_ci"),
    F.expr("percentile_approx(npv, 0.975)").alias("npv_upper_ci"),
    F.mean("precision").alias("ppv_mean"),
    F.expr("percentile_approx(precision, 0.025)").alias("ppv_lower_ci"),
    F.expr("percentile_approx(precision, 0.975)").alias("ppv_upper_ci")
)

# Show the cleaner summary
formatted_summary = [
    ("AUC", summary.select("auc_mean").first()[0], summary.select("auc_lower_ci").first()[0], summary.select("auc_upper_ci").first()[0]),
    ("Accuracy", summary.select("accuracy_mean").first()[0], summary.select("accuracy_lower_ci").first()[0], summary.select("accuracy_upper_ci").first()[0]),
    ("MCC", summary.select("mcc_mean").first()[0], summary.select("mcc_lower_ci").first()[0], summary.select("mcc_upper_ci").first()[0]),
    ("F1 Score", summary.select("f1_mean").first()[0], summary.select("f1_lower_ci").first()[0], summary.select("f1_upper_ci").first()[0]),
    ("Sensitivity", summary.select("sensitivity_mean").first()[0], summary.select("sensitivity_lower_ci").first()[0], summary.select("sensitivity_upper_ci").first()[0]),
    ("Specificity", summary.select("specificity_mean").first()[0], summary.select("specificity_lower_ci").first()[0], summary.select("specificity_upper_ci").first()[0]),
    ("NPV", summary.select("npv_mean").first()[0], summary.select("npv_lower_ci").first()[0], summary.select("npv_upper_ci").first()[0]),
    ("PPV", summary.select("ppv_mean").first()[0], summary.select("ppv_lower_ci").first()[0], summary.select("ppv_upper_ci").first()[0])
]

formatted_df = spark.createDataFrame(formatted_summary, ["Metric", "Mean", "Lower CI", "Upper CI"])
formatted_df.show(truncate=False)