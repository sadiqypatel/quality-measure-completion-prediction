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

def calculate_mcc(tn, fp, fn, tp):
    numerator = (tp * tn) - (fp * fn)
    denominator = np.sqrt(float((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn)))
    return numerator / denominator if denominator != 0.0 else 0.0
  
def calculate_mean_and_ci(metric_values):
    mean_value = np.mean(metric_values)
    ci_value = np.percentile(metric_values, [2.5, 97.5])
    return mean_value, ci_value

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics
from pyspark.ml.linalg import Vectors
import numpy as np

def bootstrap_metrics(df, n_iterations, fraction):
    aucs = []
    mccs = []
    accuracies = []
    sensitivities = []
    specificities = []
    npvs = []
    ppvs = []
    f1s = []
    for _ in range(n_iterations):
        # Create a bootstrap sample with replacement
        bootstrap_sample = df.sample(withReplacement=False, fraction=fraction)
        
        auc_evaluator = BinaryClassificationEvaluator(rawPredictionCol='rawPrediction', labelCol='outcome', metricName='areaUnderROC')
        auc = auc_evaluator.evaluate(bootstrap_sample)
        
        # Calculate True Positives (TP), False Positives (FP), True Negatives (TN), and False Negatives (FN)
        
        tp = bootstrap_sample.filter((col('prediction') == 1.0) & (col('outcome') == 1.0)).count()
        tn = bootstrap_sample.filter((col('prediction') == 0.0) & (col('outcome') == 0.0)).count()
        fp = bootstrap_sample.filter((col('prediction') == 1.0) & (col('outcome') == 0.0)).count()
        fn = bootstrap_sample.filter((col('prediction') == 0.0) & (col('outcome') == 1.0)).count()

        sensitivity = tp / (tp + fn) if (tp + fn) > 0 else None
        specificity = tn / (tn + fp) if (tn + fp) > 0 else None
        ppv = tp / (tp + fp) if (tp + fp) > 0 else None
        npv = tn / (tn + fn) if (tn + fn) > 0 else None
        accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else None
        mcc = calculate_mcc(tn, fp, fn, tp)

        precision = tp / (tp + fp) if (tp + fp) != 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) != 0 else 0.0
        f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) != 0 else 0.0
                                
        sensitivities.append(sensitivity)
        specificities.append(specificity)
        npvs.append(npv)
        ppvs.append(ppv)
        mccs.append(mcc)
        accuracies.append(accuracy)
        aucs.append(auc)
        f1s.append(f1_score)   

        
    return aucs, mccs, accuracies, sensitivities, specificities, npvs, ppvs, f1s

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

test = spark.table("dua_058828_spa240.paper_4_amm_acute_test_xgboost_new1_no_sdoh")


# COMMAND ----------

test.show()

# COMMAND ----------

iteration_levels = [500]
fraction = 0.25

# COMMAND ----------

# Perform bootstrap analysis for each iteration level for the training set
train_aucs_list = []
train_mccs_list = []
train_accuracies_list = []
train_ppvs_list = []
train_npvs_list = []
train_sensitivities_list = []
train_specificities_list = []
train_f1_list = []

train_ci_aucs_list = []
train_ci_mccs_list = []
train_ci_accuracies_list = []
train_ci_ppvs_list = []
train_ci_npvs_list = []
train_ci_sensitivities_list = []
train_ci_specificities_list = []
train_ci_f1_list = []

for n_iterations in iteration_levels:
    aucs, mccs, accuracies, sensitivities, specificities, npvs, ppvs, f1s = bootstrap_metrics(test, n_iterations, fraction)
    train_aucs_list.append(np.mean(aucs))
    train_mccs_list.append(np.mean(mccs))
    train_accuracies_list.append(np.mean(accuracies))
    train_sensitivities_list.append(np.mean(sensitivities))
    train_specificities_list.append(np.mean(specificities))
    train_npvs_list.append(np.mean(npvs))
    train_ppvs_list.append(np.mean(ppvs))
    train_f1_list.append(np.mean(f1s))
    
    train_ci_aucs_list.append(np.percentile(aucs, [2.5, 97.5]))
    train_ci_mccs_list.append(np.percentile(mccs, [2.5, 97.5]))
    train_ci_accuracies_list.append(np.percentile(accuracies, [2.5, 97.5]))
    train_ci_sensitivities_list.append(np.percentile(sensitivities, [2.5, 97.5]))
    train_ci_specificities_list.append(np.percentile(specificities, [2.5, 97.5]))
    train_ci_npvs_list.append(np.percentile(npvs, [2.5, 97.5]))
    train_ci_ppvs_list.append(np.percentile(ppvs, [2.5, 97.5]))
    train_ci_f1_list.append(np.percentile(f1s, [2.5, 97.5]))

print(train_aucs_list)
print(train_mccs_list)
print(train_accuracies_list)    
print(train_sensitivities_list)
print(train_specificities_list)
print(train_npvs_list)
print(train_ppvs_list)
print(train_f1_list)

print(train_ci_aucs_list)
print(train_ci_mccs_list)
print(train_ci_accuracies_list)    
print(train_ci_sensitivities_list)
print(train_ci_specificities_list)
print(train_ci_npvs_list)
print(train_ci_ppvs_list)
print(train_ci_f1_list)

# COMMAND ----------

