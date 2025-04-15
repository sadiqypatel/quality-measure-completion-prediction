# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# List of words you want to filter out
words_to_filter_out = ['categorical_features_race_', 'categorical_features_disabled', 'categorical_features_houseSize', 'categorical_features_married', 'categorical_features_UsCitizen']

# Build the filter condition
filter_condition = ~(
    col('feature').startswith(words_to_filter_out[0]) |
    col('feature').startswith(words_to_filter_out[1]) |
    col('feature').startswith(words_to_filter_out[2]) |
    col('feature').startswith(words_to_filter_out[3]) |
    col('feature').startswith(words_to_filter_out[4]) 
)

# COMMAND ----------

hedis1 = spark.table("dua_058828_spa240.paper_4_prenatal_sdoh_feature_imp")
hedis2 = spark.table("dua_058828_spa240.paper_4_postpartum_sdoh_feature_imp")
hedis3 = spark.table("dua_058828_spa240.paper_4_low_back_sdoh_feature_imp")
hedis4 = spark.table("dua_058828_spa240.paper_4_postpartum_pcr_feature_imp")
hedis5 = spark.table("dua_058828_spa240.paper_4_pbd_sdoh_feature_imp")
hedis6 = spark.table("dua_058828_spa240.paper_4_fum30_sdoh_feature_imp")
hedis7 = spark.table("dua_058828_spa240.paper_4_amm_acute_sdoh_feature_imp")
hedis8 = spark.table("dua_058828_spa240.paper_4_amm_contin_sdoh_feature_imp")
hedis9 = spark.table("dua_058828_spa240.paper_4_spc_outcome1_sdoh_feature_imp")
hedis10 = spark.table("dua_058828_spa240.paper_4_spc_outcome2_sdoh_feature_imp")
hedis11 = spark.table("dua_058828_spa240.paper_4_spd_outcome1_sdoh_feature_imp")
hedis12 = spark.table("dua_058828_spa240.paper_4_spd_outcome2_sdoh_feature_imp")
hedis13 = spark.table("dua_058828_spa240.paper_4_wcv_sdoh_feature_imp")

# COMMAND ----------

hedis1.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

# List of DataFrame names
dataframe_names = [f"hedis{i}" for i in range(1, 14)]

# List of words to filter out
words_to_filter_out = ['categorical_features_race_', 
                       'categorical_features_disabled', 
                       'categorical_features_houseSize', 
                       'categorical_features_married', 
                       'categorical_features_UsCitizen']

# Loop through each DataFrame name
for df_name in dataframe_names:
    # Access the DataFrame by its name
    df = globals()[df_name]
    
    # Build the filter condition to exclude rows based on `words_to_filter_out`
    filter_condition = ~(
        F.col('feature').startswith(words_to_filter_out[0]) |
        F.col('feature').startswith(words_to_filter_out[1]) |
        F.col('feature').startswith(words_to_filter_out[2]) |
        F.col('feature').startswith(words_to_filter_out[3]) |
        F.col('feature').startswith(words_to_filter_out[4])
    )
    
    # Apply the filter condition
    df = df.filter(filter_condition)
    
    # Extract the common prefix for each feature up to the first "_" after the main identifier
    df = df.withColumn("feature", F.regexp_extract(F.col("feature"), r"^(categorical_features_[^_]+|numeric_features_[^_]+)", 0))


    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # Step 1: Apply the filter condition
    df = df.filter(filter_condition)

    # Step 3: Create a window to rank features by their `raw_value`
    window_spec = Window.partitionBy("feature").orderBy(F.desc("scaled_value"))

    # Add a rank column to identify the "top" feature within each group
    df = df.withColumn("rank", F.rank().over(window_spec))


    # Step 4: Compute the combined value
    df = df.withColumn(
        "combined_value",
        F.when(F.col("rank") == 1, F.col("scaled_value"))  # Full value of the top feature
        .otherwise(F.when(F.col("rank") > 1, 0.125 * F.col("scaled_value")))  # 0.5 of other features
    )


    # Step 5: Aggregate the combined values by feature_prefix
    result_df = df.groupBy("feature").agg(
        F.round(F.sum("combined_value"), 3).alias("scaled_value"),
        F.round(F.max("scaled_value"), 3).alias("max_scaled_value")  # Retaining max scaled_value for reference
    )

    # Step 6: Sort by the final value in descending order
    result_df = result_df.orderBy(F.desc("max_scaled_value"))

    # Step 7: Show the final result
    result_df.show(truncate=False)

    globals()[df_name] = result_df


# COMMAND ----------

hedis1.show(truncate=False)

# COMMAND ----------

# Define the list of values you want to keep
values_to_keep = ["categorical_features_speakEnglish","numeric_features_urgentCareRate","numeric_features_mhTreatRate","numeric_features_saServRate","numeric_features_povRate","numeric_features_saFacRate","numeric_features_popDensity","numeric_features_highSchoolGradRate","categorical_features_ssi","categorical_features_ssdi","categorical_features_tanf","numeric_features_goodAirDays","categorical_features_fedPovLine","numeric_features_drugdeathRate","numeric_features_aprnRate"]

# Filter rows for each DataFrame where the "feature" column has a value in the list
hedis1 = hedis1.filter(hedis1["feature"].isin(values_to_keep))
hedis2 = hedis2.filter(hedis2["feature"].isin(values_to_keep))
hedis3 = hedis3.filter(hedis3["feature"].isin(values_to_keep))
hedis4 = hedis4.filter(hedis4["feature"].isin(values_to_keep))
hedis5 = hedis5.filter(hedis5["feature"].isin(values_to_keep))
hedis6 = hedis6.filter(hedis6["feature"].isin(values_to_keep))
hedis7 = hedis7.filter(hedis7["feature"].isin(values_to_keep))
hedis8 = hedis8.filter(hedis8["feature"].isin(values_to_keep))
hedis9 = hedis9.filter(hedis9["feature"].isin(values_to_keep))
hedis10 = hedis10.filter(hedis10["feature"].isin(values_to_keep))
hedis11 = hedis11.filter(hedis11["feature"].isin(values_to_keep))
hedis12 = hedis12.filter(hedis12["feature"].isin(values_to_keep))
hedis13 = hedis13.filter(hedis13["feature"].isin(values_to_keep))

# COMMAND ----------

hedis1.show(truncate=False)

# COMMAND ----------

from pyspark.sql import SparkSession

# assume you have 13 DataFrames named hedis1 to hedis13

# create a list of DataFrames
hedis_dfs = [hedis1, hedis2, hedis3, hedis4, hedis5, hedis6, hedis7, hedis8, hedis9, hedis10, hedis11, hedis12, hedis13]

# initialize the union with the first DataFrame
union_df = hedis_dfs[0].select('feature')

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

hedis1.show(truncate=False)

# COMMAND ----------

unique_count = unique_values.count()
print(f"Number of unique values: {unique_count}")

# COMMAND ----------

unique_values.show(n=unique_values.count(), truncate=False)

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

result_df = result_df.withColumn("feature", F.regexp_replace(F.col("feature"), r"^(categorical_features_|numeric_features_)", ""))
result_df.show(n=result_df.count(), truncate=False)

# COMMAND ----------

# Import necessary functions
from pyspark.sql import functions as F

# Convert the feature column into rows (pivot-like structure)
transposed_df = result_df.selectExpr("feature", "stack(13, 'hedis1', hedis1, 'hedis2', hedis2, 'hedis3', hedis3, 'hedis4', hedis4, 'hedis5', hedis5, 'hedis6', hedis6, 'hedis7', hedis7, 'hedis8', hedis8, 'hedis9', hedis9, 'hedis10', hedis10, 'hedis11', hedis11, 'hedis12', hedis12, 'hedis13', hedis13) as (hedis, value)")\
    .groupBy("hedis")\
    .pivot("feature")\
    .agg(F.first("value"))

# Show the transposed DataFrame
transposed_df.show(truncate=False)

# COMMAND ----------

# Use `F.when` to add a new column with group labels
transposed_df = transposed_df.withColumn(
    "group",
    F.when(F.col("hedis") == "hedis1", "Preventive Care")
    .when(F.col("hedis") == "hedis2", "Preventive Care")
    .when(F.col("hedis") == "hedis3", "Unnecessary Care")
    .when(F.col("hedis") == "hedis4", "Care Coordination")
    .when(F.col("hedis") == "hedis5", "Chronic Disease Management")
    .when(F.col("hedis") == "hedis6", "Behavioral Health")
    .when(F.col("hedis") == "hedis7", "Chronic Disease Management")
    .when(F.col("hedis") == "hedis8", "Chronic Disease Management")
    .when(F.col("hedis") == "hedis9", "Chronic Disease Management")
    .when(F.col("hedis") == "hedis10", "Chronic Disease Management")
    .when(F.col("hedis") == "hedis11", "Chronic Disease Management")
    .when(F.col("hedis") == "hedis12", "Chronic Disease Management")
    .when(F.col("hedis") == "hedis13", "Preventive Care")
)


# Show the updated DataFrame
#transposed_df.show(truncate=False)

# COMMAND ----------

# Specify your desired column order
desired_column_order = ["hedis","group", "fedPovLine", "ssdi", "ssi", "tanf", "speakEnglish", "saServRate", "mhTreatRate", "urgentCareRate", "aprnRate", "povRate", "popDensity", "highSchoolGradRate", "goodAirDays"]  # Replace with your desired column names

# Reorder the DataFrame columns
transposed_df_new = transposed_df.select(*desired_column_order)

# Show the reordered DataFrame
print("DataFrame with columns ordered as specified:")
print(transposed_df_new.count())
#transposed_df_new.show(truncate=False, n=transposed_df_new.count())


# COMMAND ----------

# Specify your desired column order
desired_column_order = ["hedis","group", "fedPovLine", "ssdi", "ssi", "tanf", "speakEnglish", "saServRate", "mhTreatRate", "urgentCareRate", "aprnRate", "povRate", "popDensity", "highSchoolGradRate", "goodAirDays"]  # Replace with your desired column names

# Reorder the DataFrame columns
transposed_df_new = transposed_df_new.select(*desired_column_order)

# Show the reordered DataFrame
print("DataFrame with columns ordered as specified:")
print(transposed_df_new.count())
transposed_df_new.show(truncate=False)

# COMMAND ----------

df= transposed_df

# COMMAND ----------

from pyspark.sql import functions as F

# Step 1: Group by `group` and calculate average importance for each variable
importance_columns = [col for col in df.columns if col not in ['group', 'hedis']]
grouped_df = df.groupBy("group").agg(
    *[F.avg(F.col(col)).alias(col) for col in importance_columns]
)

# Step 2: Collect top 10 variables for each group
top_10_by_group = {}
for group in grouped_df.select("group").distinct().collect():
    group_name = group["group"]
    group_data = grouped_df.filter(F.col("group") == group_name)
    
    # Unpivot the importance columns to find the top 10
    melted_df = group_data.selectExpr(
        "group", "stack({}, {}) as (variable, importance)".format(
            len(importance_columns),
            ", ".join([f"'{col}', {col}" for col in importance_columns])
        )
    )
    
    # Sort by importance and collect top 10 variables
    top_10 = melted_df.orderBy(F.desc("importance")).limit(10).select("variable").rdd.flatMap(lambda x: x).collect()
    top_10_by_group[group_name] = set(top_10)

# Step 3: Find overlaps across groups
all_top_10 = list(top_10_by_group.values())
overlapping_variables = set.intersection(*all_top_10)

# Step 4: Identify remaining (non-overlapping) variables for each group
remaining_variables_by_group = {
    group: top_10 - overlapping_variables for group, top_10 in top_10_by_group.items()
}

# Print results
print("Overlapping Variables Across All Groups:")
print(overlapping_variables)

print("\nRemaining Non-Overlapping Variables for Each Group:")
for group, remaining_vars in remaining_variables_by_group.items():
    print(f"Group: {group}, Remaining Variables: {remaining_vars}")


# COMMAND ----------

from pyspark.sql import functions as F

# Step 1: Group by `group` and calculate average importance for each variable
importance_columns = [col for col in df.columns if col not in ['group', 'hedis']]
grouped_df = df.groupBy("group").agg(
    *[F.avg(F.col(col)).alias(col) for col in importance_columns]
)

# Step 2: Collect top 10 variables for each group
top_10_by_group = {}
for group in grouped_df.select("group").distinct().collect():
    group_name = group["group"]
    group_data = grouped_df.filter(F.col("group") == group_name)
    
    # Unpivot the importance columns to find the top 10
    melted_df = group_data.selectExpr(
        "group", "stack({}, {}) as (variable, importance)".format(
            len(importance_columns),
            ", ".join([f"'{col}', {col}" for col in importance_columns])
        )
    )
    
    # Sort by importance and collect top 10 variables
    top_10 = melted_df.orderBy(F.desc("importance")).limit(10).select("variable").rdd.flatMap(lambda x: x).collect()
    top_10_by_group[group_name] = set(top_10)

# Step 3: Find overlaps across groups
all_top_10 = list(top_10_by_group.values())
overlapping_variables = set.intersection(*all_top_10)

# Step 4: Calculate means for all variables by group
# Update the `overlapping_df` to include means of all variables grouped by 'group'

# Exclude null values when calculating mean for each variable by group
means_df = grouped_df.groupBy("group").agg(
    *[
        F.mean(F.col(var)).alias(var) 
        for var in grouped_df.columns if var != "group"  # Exclude the group column itself
    ]
)

# Show the calculated means
print("Means of all variables by group (excluding nulls):")
means_df.show(truncate=False)

# COMMAND ----------

# Specify your desired column order
desired_column_order = ["group", "fedPovLine", "ssdi", "ssi", "tanf", "speakEnglish", "saServRate", "mhTreatRate", "urgentCareRate", "aprnRate", "povRate", "popDensity", "highSchoolGradRate", "goodAirDays"]  # Replace with your desired column names

# Reorder the DataFrame columns
means_df_new = means_df.select(*desired_column_order)

# Show the reordered DataFrame
print("DataFrame with columns ordered as specified:")
print(means_df_new.count())
means_df_new.show(truncate=False)


# COMMAND ----------

