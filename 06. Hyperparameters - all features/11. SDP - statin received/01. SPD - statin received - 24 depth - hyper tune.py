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
import pandas as pd
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import numpy as np
from xgboost.spark import SparkXGBClassifier
import mlflow
import pandas as pd
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType
import mlflow
import mlflow.spark
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import numpy as np
import seaborn as sns # optional for plotting
import matplotlib.pyplot as plt # optional for plotting
# Example: Using SHAP with a random forest classifier
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import load_iris
import shap
import xgboost
import tensorflow as tf
from tensorflow.keras import layers, models
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, recall_score, confusion_matrix, matthews_corrcoef, precision_score, f1_score

# COMMAND ----------

outcome = spark.table("dua_058828_spa240.paper_4_spd_outcome1_12_months")
print(outcome.count())
outcome.show()

# COMMAND ----------

vector = spark.table("dua_058828_spa240.paper4_demo_all_features_vector_new1")
print(vector.count())
vector.show()

# COMMAND ----------

df = vector.join(outcome, on=["beneID","state"],how="inner")
print(df.count())

# COMMAND ----------

df.show()

# COMMAND ----------

df = df.select("beneID","state","features","anymed")

# COMMAND ----------

# Perform value counts
value_counts = df.groupBy("anymed").agg(count("anymed").alias("count"))

# Calculate total count
total_count = df.count()

# Add percentage column
value_counts_with_percentage = value_counts.withColumn("percentage", (col("count") / total_count) * 100)

# Show the result
value_counts_with_percentage.show()

# COMMAND ----------

from pyspark.sql.functions import when

df = df.withColumn("outcome", when(df.anymed == 1, 0).otherwise(1))

# Perform value counts
value_counts = df.groupBy("outcome").agg(count("outcome").alias("count"))

# Calculate total count
total_count = df.count()

# Add percentage column
value_counts_with_percentage = value_counts.withColumn("percentage", (col("count") / total_count) * 100)

# Show the result
value_counts_with_percentage.show()

# COMMAND ----------

# Calculate the fraction to sample in order to get approximately 500k rows

#fraction = df.count() / df.count()

fraction = 0.5

# Take a random sample from the DataFrame
sampled_df = df.sample(withReplacement=False, fraction=fraction, seed=42)

# # Show the number of rows in the sampled DataFrame
print("Number of rows in the sampled DataFrame:", sampled_df.count())

# COMMAND ----------

from pyspark.sql.functions import col

# Split the data into training and test sets with stratified sampling
fractions = sampled_df.groupBy("outcome").count().rdd.map(lambda x: (x[0], 0.8)).collectAsMap()
train_df = sampled_df.stat.sampleBy("outcome", fractions, seed=1234)
test_df = sampled_df.exceptAll(train_df)

# Calculate the percentage of total sample
train_counts = train_df.groupBy("outcome").count()
train_total = train_df.count()
train_percentages = train_counts.withColumn("percentage", (col("count") / train_total) * 100)

test_counts = test_df.groupBy("outcome").count()
test_total = test_df.count()
test_percentages = test_counts.withColumn("percentage", (col("count") / test_total) * 100)

print(train_percentages.show())
print(test_percentages.show())

# COMMAND ----------

# Perform an additional 80/20 train/validation split on the training set
train_val_ratio = 0.8
fractions = train_df.groupBy("outcome").count().rdd.map(lambda x: (x[0], train_val_ratio)).collectAsMap()
train_sub_df = train_df.stat.sampleBy("outcome", fractions, seed=42)
val_df = train_df.exceptAll(train_sub_df)

# Calculate the percentage of total sample
train_sub_counts = train_sub_df.groupBy("outcome").count()
train_sub_total = train_sub_df.count()
train_sub_percentages = train_sub_counts.withColumn("percentage", (col("count") / train_sub_total) * 100)

val_counts = val_df.groupBy("outcome").count()
val_total = val_df.count()
val_percentages = val_counts.withColumn("percentage", (col("count") / val_total) * 100)

print(train_sub_percentages.show())
print(val_percentages.show())

# COMMAND ----------

import xgboost as xgb
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.metrics import precision_score, recall_score, f1_score
import mlflow
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


xgb_classifier  = SparkXGBClassifier(
    objective="binary:logistic",
    eval_metric='logloss',
    label=1
)

# # grid search based on model
# param_grid = (
#     ParamGridBuilder()
#     .addGrid(xgb_classifier.learning_rate, [0.01])                 
#     .addGrid(xgb_classifier.subsample, [0.5, 0.6, 0.7, 0.8, 1.0])                       
#     .addGrid(xgb_classifier.min_child_weight, [0, 2.5, 5])                         
#     .addGrid(xgb_classifier.colsample_bytree, [0.5, 0.75, 1.0])                   
#     .addGrid(xgb_classifier.max_depth, [4, 8, 12, 16, 18, 20, 22])                        
#     .addGrid(xgb_classifier.reg_alpha, [0.0, 0.25, 0.5, 1.0])                           
#     .addGrid(xgb_classifier.reg_lambda, [0.0, 0.25, 0.5, 1.0])                          
#     .addGrid(xgb_classifier.gamma, [0.0, 0.5, 1.0])                                
#     .addGrid(xgb_classifier.colsample_bylevel, [0.5, 0.75, 1.0])                   
#     .build()
# )

# # grid search based on model
# param_grid = (
#     ParamGridBuilder()
#     .addGrid(xgb_classifier.learning_rate, [0.01])                 
#     .addGrid(xgb_classifier.subsample, [0.5, 0.75, 1.0])                       
#     .addGrid(xgb_classifier.min_child_weight, [0, 2.5, 5])                         
#     .addGrid(xgb_classifier.colsample_bytree, [0.5, 0.75, 1.0])                   
#     .addGrid(xgb_classifier.max_depth, [10, 12, 14, 16, 18, 20, 22, 24])                        
#     .addGrid(xgb_classifier.reg_alpha, [0.0, 0.5, 1.0])                           
#     .addGrid(xgb_classifier.reg_lambda, [0.0, 0.5, 1.0])                          
#     .addGrid(xgb_classifier.gamma, [0.0, 0.5, 1.0])                                
#     .addGrid(xgb_classifier.colsample_bylevel, [0.5, 0.75, 1.0])                   
#     .build()
# )

param_grid = (
    ParamGridBuilder()
    .addGrid(xgb_classifier.learning_rate, [0.01])                 
    .addGrid(xgb_classifier.subsample, [0.25, 0.5, 0.75, 1.0])                       
    .addGrid(xgb_classifier.min_child_weight, [0, 2.5, 5])                         
    .addGrid(xgb_classifier.colsample_bytree, [0.5, 0.75, 1.0])                   
    .addGrid(xgb_classifier.max_depth, [24])                        
    .addGrid(xgb_classifier.reg_alpha, [0.0, 0.5, 1.0])                           
    .addGrid(xgb_classifier.reg_lambda, [0.0, 0.25, 0.5, 1.0])                          
    .addGrid(xgb_classifier.gamma, [0.0, 0.25, 0.5, 0.75, 1.0])                                
    .addGrid(xgb_classifier.colsample_bylevel, [0.25, 0.5, 0.75, 1.0])                   
    .build()
)

# COMMAND ----------

def calculate_mcc(tn, fp, fn, tp):
    numerator = (tp * tn) - (fp * fn)
    denominator = np.sqrt(float((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn)))
    return numerator / denominator if denominator != 0.0 else 0.0
  
def calculate_mean_and_ci(metric_values):
    mean_value = np.mean(metric_values)
    ci_value = np.percentile(metric_values, [2.5, 97.5])
    return mean_value, ci_value

# Create an instance of the BinaryClassificationEvaluator
auc_evaluator = BinaryClassificationEvaluator(
    rawPredictionCol="rawPrediction",  # The column containing raw predictions (e.g., rawPrediction)
    labelCol="outcome",  # The column containing true labels (e.g., label)
    metricName="areaUnderROC"  # The metric to evaluate (e.g., area under the ROC curve)
)

# Create an instance of the MulticlassClassificationEvaluator
multi_evaluator = MulticlassClassificationEvaluator(
    labelCol="outcome",  # The column containing true labels (e.g., label)
    predictionCol="prediction",  # The column containing predicted labels (e.g., prediction)
    metricName="accuracy"  # The metric to evaluate (e.g., accuracy)
)

# COMMAND ----------

import mlflow

# Set an active experiment
experiment_name = "/Workspace/Users/spa240@ccwdata.org/Paper 4/06. Hyperparameters - all features/11. SDP - statin received/SPD - First Outcome - 24 depth - run 1"

mlflow.set_experiment(experiment_name)

# COMMAND ----------

for params in param_grid:
    xgb = SparkXGBClassifier(features_col ="features", label_col="outcome", missing=0.0, **{param.name: value for param, value in params.items()})
    model = xgb.fit(train_sub_df)
    predictions = model.transform(val_df)

    # Evaluate the performance of the best model
    #auc = auc_evaluator.evaluate(predictions)
    tp = predictions.filter((col('prediction') == 1.0) & (col('outcome') == 1.0)).count()
    tn = predictions.filter((col('prediction') == 0.0) & (col('outcome') == 0.0)).count()
    fp = predictions.filter((col('prediction') == 1.0) & (col('outcome') == 0.0)).count()
    fn = predictions.filter((col('prediction') == 0.0) & (col('outcome') == 1.0)).count()

    sensitivity = tp / (tp + fn) if (tp + fn) > 0.0 else 0.0
    specificity = tn / (tn + fp) if (tn + fp) > 0.0 else 0.0
    ppv = tp / (tp + fp) if (tp + fp) > 0.0 else 0.0
    npv = tn / (tn + fn) if (tn + fn) > 0.0 else 0.0
    accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0.0 else 0.0
    mcc = calculate_mcc(tn, fp, fn, tp)    
    
    precision = tp / (tp + fp) if (tp + fp) != 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) != 0 else 0.0
    f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) != 0 else 0.0    

    with mlflow.start_run():
        for param, value in params.items():
            mlflow.log_param(param, value)

        # Log metrics
        mlflow.log_metrics({
            "Accuracy": accuracy,
            "Sensitivity": sensitivity,
            "Specificity": specificity,
            "PPV": ppv,
            "NPV": npv,
            "MCC": mcc,
            "F1": f1_score,
            "AUC": auc_evaluator.evaluate(predictions)
        })