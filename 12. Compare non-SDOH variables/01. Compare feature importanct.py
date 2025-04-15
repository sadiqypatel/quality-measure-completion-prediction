# Databricks notebook source
hedis1 = spark.table("dua_058828_spa240.paper_4_prenatal_non_sdoh_feature_imp")
hedis1 = hedis1.orderBy("raw_value", ascending=False)
hedis1 = hedis1.limit(10)
print(hedis1.count())
hedis1.show(truncate=False)

# COMMAND ----------

hedis2 = spark.table("dua_058828_spa240.paper_4_postpartum_non_sdoh_feature_imp")
hedis2 = hedis2.orderBy("raw_value", ascending=False)
hedis2 = hedis2.limit(10)
print(hedis2.count())
hedis2.show(truncate=False)

# COMMAND ----------

hedis3 = spark.table("dua_058828_spa240.paper_4_low_back_non_sdoh_feature_imp")
hedis3 = hedis3.orderBy("raw_value", ascending=False)
hedis3 = hedis3.limit(10)
print(hedis3.count())
hedis3.show(truncate=False)

# COMMAND ----------

hedis4 = spark.table("dua_058828_spa240.paper_4_pcr_non_sdoh_feature_imp")
hedis4 = hedis4.orderBy("raw_value", ascending=False)
hedis4 = hedis4.limit(10)
print(hedis4.count())
hedis4.show(truncate=False)

# COMMAND ----------

hedis5 = spark.table("dua_058828_spa240.paper_4_pbd_non_sdoh_feature_imp")
hedis5 = hedis5.orderBy("raw_value", ascending=False)
hedis5 = hedis5.limit(10)
print(hedis5.count())
hedis5.show(truncate=False)

# COMMAND ----------

hedis6 = spark.table("dua_058828_spa240.paper_4_fum30_non_sdoh_feature_imp")
hedis6 = hedis6.orderBy("raw_value", ascending=False)
hedis6 = hedis6.limit(10)
print(hedis6.count())
hedis6.show(truncate=False)

# COMMAND ----------

hedis7 = spark.table("dua_058828_spa240.paper_4_amm_acute_non_sdoh_feature_imp")
hedis7 = hedis7.orderBy("raw_value", ascending=False)
hedis7 = hedis7.limit(10)
print(hedis7.count())
hedis7.show(truncate=False)

# COMMAND ----------

hedis8 = spark.table("dua_058828_spa240.paper_4_amm_contin_non_sdoh_feature_imp")
hedis8 = hedis8.orderBy("raw_value", ascending=False)
hedis8 = hedis8.limit(10)
print(hedis8.count())
hedis8.show(truncate=False)

# COMMAND ----------

hedis9 = spark.table("dua_058828_spa240.paper_4_spc_outcome1_non_sdoh_feature_imp")
hedis9 = hedis9.orderBy("raw_value", ascending=False)
hedis9 = hedis9.limit(10)
print(hedis9.count())
hedis9.show(truncate=False)

# COMMAND ----------

hedis10 = spark.table("dua_058828_spa240.paper_4_spc_outcome2_non_sdoh_feature_imp")
hedis10 = hedis10.orderBy("raw_value", ascending=False)
hedis10 = hedis10.limit(10)
print(hedis10.count())
hedis10.show(truncate=False)

# COMMAND ----------

hedis11 = spark.table("dua_058828_spa240.paper_4_spd_outcome1_non_sdoh_feature_imp")
hedis11 = hedis11.orderBy("raw_value", ascending=False)
hedis11 = hedis11.limit(10)
print(hedis11.count())
hedis11.show(truncate=False)

# COMMAND ----------

hedis12 = spark.table("dua_058828_spa240.paper_4_spd_outcome2_non_sdoh_feature_imp")
hedis12 = hedis12.orderBy("raw_value", ascending=False)
hedis12 = hedis12.limit(10)
print(hedis12.count())
hedis12.show(truncate=False)

# COMMAND ----------

hedis13 = spark.table("dua_058828_spa240.paper_4_wcv_non_sdoh_feature_imp")
hedis13 = hedis13.orderBy("raw_value", ascending=False)
hedis13 = hedis13.limit(10)
print(hedis13.count())
hedis13.show(truncate=False)

# COMMAND ----------

from pyspark.sql import SparkSession

# assume you have 13 DataFrames named hedis1 to hedis13

# create a list of DataFrames
hedis_dfs = [hedis1, hedis2, hedis3, hedis4, hedis5, hedis6, hedis7, hedis8, hedis9, hedis10, hedis11, hedis12, hedis13]

# initialize the union with the first DataFrame
union_df = hedis_dfs[0].select("feature")

# iterate over the remaining DataFrames and union with the current union
for df in hedis_dfs[1:]:
    union_df = union_df.union(df.select("feature"))

# get the distinct values
unique_values = union_df.distinct()

# collect and print the unique values
values = unique_values.collect()
for value in values:
    print(value["feature"])

# COMMAND ----------

unique_count = unique_values.count()
print(f"Number of unique values: {unique_count}")

# COMMAND ----------

unique_values.show(n=unique_values.count(), truncate=False)

# COMMAND ----------

hedis1 = spark.table("dua_058828_spa240.paper_4_prenatal_non_sdoh_feature_imp")
hedis2 = spark.table("dua_058828_spa240.paper_4_postpartum_non_sdoh_feature_imp")
hedis3 = spark.table("dua_058828_spa240.paper_4_low_back_non_sdoh_feature_imp")
hedis4 = spark.table("dua_058828_spa240.paper_4_pcr_non_sdoh_feature_imp")
hedis5 = spark.table("dua_058828_spa240.paper_4_pbd_non_sdoh_feature_imp")
hedis6 = spark.table("dua_058828_spa240.paper_4_fum30_non_sdoh_feature_imp")
hedis7 = spark.table("dua_058828_spa240.paper_4_amm_acute_non_sdoh_feature_imp")
hedis8 = spark.table("dua_058828_spa240.paper_4_amm_contin_non_sdoh_feature_imp")
hedis9 = spark.table("dua_058828_spa240.paper_4_spc_outcome1_non_sdoh_feature_imp")
hedis10 = spark.table("dua_058828_spa240.paper_4_spc_outcome2_non_sdoh_feature_imp")
hedis11 = spark.table("dua_058828_spa240.paper_4_spd_outcome1_non_sdoh_feature_imp")
hedis12 = spark.table("dua_058828_spa240.paper_4_spd_outcome2_non_sdoh_feature_imp")
hedis13 = spark.table("dua_058828_spa240.paper_4_wcv_non_sdoh_feature_imp")

# COMMAND ----------

# filter the data sets to keep only the "feature" values in unique_values
for i in range(1, 14):
    globals()[f"hedis{i}"] = unique_values.join(globals()[f"hedis{i}"], on='feature', how='left')
    print(f"hedis{i} count: {globals()[f'hedis{i}'].count()}")

# COMMAND ----------

# Create a list of DataFrames
hedis_dfs = [hedis1, hedis2, hedis3, hedis4, hedis5, hedis6, hedis7, hedis8, hedis9, hedis10, hedis11, hedis12, hedis13]

# Create a new DataFrame with the desired structure
result_df = hedis_dfs[0].select("feature", "scaled_value").withColumnRenamed("scaled_value", "hedis1")

# Iterate over the remaining DataFrames and join them to the result_df
for i, df in enumerate(hedis_dfs[1:]):
    result_df = result_df.join(df.select("feature", "scaled_value").withColumnRenamed("scaled_value", f"hedis{i+2}"), on="feature", how="left")

# Rearrange the columns to have the feature column first
result_df = result_df.select("feature", *[f"hedis{i}" for i in range(1, 14)])
print(result_df.count())

# COMMAND ----------

result_df.show(n=result_df.count(), truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

# Assuming your DataFrame is named 'df'
result_df = result_df.withColumn(
    "feature_sum",
    sum([F.when(F.col(col).isNotNull(), 1).otherwise(0) for col in [
        'hedis1', 'hedis2', 'hedis3', 'hedis4', 'hedis5', 
        'hedis6', 'hedis7', 'hedis8', 'hedis9', 'hedis10', 
        'hedis11', 'hedis12', 'hedis13'
    ]])
)

# Show the result
result_df.show()

# COMMAND ----------

# Perform value count for the 'feature_sum' column
feature_sum_count_df = result_df.groupBy("feature_sum").count()

# Show the result
feature_sum_count_df.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Step 1: Count the number of rows with at least one non-null value in hedis1 through hedis13
rows_with_non_null = result_df.filter(
    F.expr("hedis1 is not null or hedis2 is not null or hedis3 is not null or hedis4 is not null or hedis5 is not null or hedis6 is not null or hedis7 is not null or hedis8 is not null or hedis9 is not null or hedis10 is not null or hedis11 is not null or hedis12 is not null or hedis13 is not null")
).count()

# Step 2: Calculate the total number of rows in the DataFrame
total_rows = result_df.count()

# Step 3: Compute the percentage of rows with at least one non-null value
percentage = (rows_with_non_null / total_rows) * 100

# Show the result
print(f"Percentage of rows with at least one non-null value in hedis1 through hedis13: {percentage:.2f}%")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


# Define the list of desired features
desired_features = [
    "categorical_features_ssi_Vec_yes",
    "categorical_features_fedPovLine_Vec_100To200",
    "numeric_features_goodAirDays",
    "categorical_features_ssi_Vec_missing",
    "categorical_features_ssi_Vec_no",
    "categorical_features_speakEnglish_Vec_missing",
    "categorical_features_ssdi_Vec_yes",
    "categorical_features_fedPovLine_Vec_200AndMore",
    "categorical_features_tanf_Vec_no",
    "categorical_features_speakEnglish_Vec_no",
    "categorical_features_fedPovLine_Vec_missing",
    "numeric_features_povRate",
    "categorical_features_fedPovLine_Vec_0To100",
    "categorical_features_tanf_Vec_yes",
    "numeric_features_urgentCareRate",
    "numeric_features_saServRate",
    "numeric_features_aprnRate"
]

result_df = result_df.filter(result_df.feature.isin(desired_features))
print(result_df.count())

# COMMAND ----------

result_df.show(n=result_df.count(), truncate=False)

# COMMAND ----------

result_df.show(truncate=False)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Create a list of DataFrames
hedis_dfs = [hedis1, hedis2, hedis3, hedis4, hedis5, hedis6, hedis7, hedis8, hedis9, hedis10, hedis11, hedis12, hedis13]

# Create a new DataFrame with the desired structure
result_df = hedis_dfs[0].select("feature", "scaled_value").withColumnRenamed("scaled_value", "hedis1")

# Iterate over the remaining DataFrames and join them to the result_df
for i, df in enumerate(hedis_dfs[1:]):
    result_df = result_df.join(df.select("feature", "scaled_value").withColumnRenamed("scaled_value", f"hedis{i+2}"), on="feature", how="inner")

# Rearrange the columns to have the feature column first
result_df = result_df.select("feature", *[f"hedis{i}" for i in range(1, 14)])

# COMMAND ----------

print(result_df.count())

# COMMAND ----------

result_df.show(truncate=False)

# COMMAND ----------

