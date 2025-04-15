# Databricks notebook source
comparison = [
    'numeric_features_saServRate', 'numeric_features_saFacRate', 'numeric_features_mhTreatRate', 'numeric_features_popDensity', 'numeric_features_povRate', 'numeric_features_publicAssistRate', 'numeric_features_highSchoolGradRate', 'numeric_features_goodAirDays', 'numeric_features_injDeathRate', 'numeric_features_urgentCareRate', 'numeric_features_drugdeathRate', 'numeric_features_100HeatDays', 'numeric_features_aprnRate','categorical_features_race_Vec_asian', 'categorical_features_race_Vec_native', 'categorical_features_race_Vec_hawaiian', 'categorical_features_race_Vec_multiracial', 'categorical_features_houseSize_Vec_missing', 'categorical_features_houseSize_Vec_single', 'categorical_features_houseSize_Vec_twoToFive', 'categorical_features_houseSize_Vec_sixorMore', 'categorical_features_fedPovLine_Vec_missing', 'categorical_features_fedPovLine_Vec_0To100', 'categorical_features_fedPovLine_Vec_100To200', 'categorical_features_fedPovLine_Vec_200AndMore', 'categorical_features_speakEnglish_Vec_missing', 'categorical_features_speakEnglish_Vec_yes', 'categorical_features_speakEnglish_Vec_no', 'categorical_features_married_Vec_missing', 'categorical_features_married_Vec_no', 'categorical_features_married_Vec_yes', 'categorical_features_UsCitizen_Vec_yes', 'categorical_features_UsCitizen_Vec_missing', 'categorical_features_UsCitizen_Vec_no', 'categorical_features_ssi_Vec_no', 'categorical_features_ssi_Vec_missing', 'categorical_features_ssi_Vec_yes', 'categorical_features_ssdi_Vec_no', 'categorical_features_ssdi_Vec_missing', 'categorical_features_ssdi_Vec_yes', 'categorical_features_tanf_Vec_no', 'categorical_features_tanf_Vec_missing', 'categorical_features_tanf_Vec_yes', 'categorical_features_disabled_Vec_no', 'categorical_features_disabled_Vec_yes'
]

# COMMAND ----------

input_data = """


"""

#input_data = [f'"{x.strip()}"' for x in input_data.strip().split('\n') if x.strip()]
input_data = [x.strip() for x in input_data.strip().split('\n') if x.strip()]
print(input_data)

# COMMAND ----------

matches1 = [x for x in input_data if x.split(', ')[0] in comparison]

matches1.sort(key=lambda x: float(x.split(', ')[1]), reverse=True)

for match in matches1:
    feature, value = match.split(', ')
    print(f"{feature}, {float(value):.2f}")

# COMMAND ----------

# create a DataFrame from the matches1 list
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.functions import round

# define the schema
schema = StructType([
    StructField("feature", StringType(), True),
    StructField("raw_value", FloatType(), True)
])

df = spark.createDataFrame([(x.split(", ")[0], float(x.split(", ")[1])) for x in matches1], schema)
df.show()

# COMMAND ----------

def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

max_value = max(float(x.split(', ')[1]) for x in input_data if len(x.split(', ')) > 1 and is_float(x.split(', ')[1]))
print(max_value)

# COMMAND ----------

df = df.withColumn("scaled_value", df["raw_value"] / max_value)
df = df.withColumn("raw_value", round(df["raw_value"], 2)).withColumn("scaled_value", round(df["scaled_value"], 2))
df.show(truncate=False)

# COMMAND ----------

print(df.count())

# COMMAND ----------

df.write.saveAsTable("dua_058828_spa240.paper_4_postpartum_sdoh_feature_imp", mode="overwrite")


# COMMAND ----------

