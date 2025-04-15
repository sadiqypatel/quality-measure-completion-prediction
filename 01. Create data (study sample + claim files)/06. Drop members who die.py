# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf
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

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper_4_final_patient_sample_2017_2019_12_months")
df = df.select("beneID","state")
print((df.count(), len(df.columns)))

# COMMAND ----------

death2017 = spark.sql("select BENE_ID, STATE_CD, DEATH_IND from extracts.tafr17.demog_elig_base")
death2017.show()

death2018 = spark.sql("select BENE_ID, STATE_CD, DEATH_IND from extracts.tafr18.demog_elig_base")
death2018.show()

death2019 = spark.sql("select BENE_ID, STATE_CD, DEATH_IND from extracts.tafr19.demog_elig_base")
death2019.show()
death = death2018.union(death2019).union(death2017)

# COMMAND ----------

# Assuming death is your DataFrame
death_counts = death.groupBy("DEATH_IND").count()

# Show the results
death_counts.show()

# COMMAND ----------

# Assuming death is your DataFrame
filtered_death = death.filter(death.DEATH_IND == 1).distinct()

# Show the results
filtered_death.show()

# COMMAND ----------

filtered_death = filtered_death.withColumnRenamed("STATE_CD", "state")
filtered_death = filtered_death.withColumnRenamed("BENE_ID", "beneID")
filtered_death.show()

# COMMAND ----------

final = spark.table("dua_058828_spa240.paper_4_final_patient_sample_2017_2019_12_months")
print(final.count())

# COMMAND ----------

final = final.join(filtered_death, on=['beneID','state'], how='left')
print(final.count())
final.show()

# COMMAND ----------

# Value count for a specific column
value_counts = final.groupBy("DEATH_IND").count()

# Show the result
value_counts.show()

# COMMAND ----------

final.printSchema()

# COMMAND ----------

# Filter out rows where DEATH_IND is equal to 1
final = final.withColumn("DEATH_IND", col("DEATH_IND").cast("float"))
filtered_df = final.filter(col("DEATH_IND").isNull())

#print(final.count())
print(filtered_df.count())

# COMMAND ----------

filtered_df.show()

# COMMAND ----------

filtered_df = filtered_df.drop("DEATH_IND")
filtered_df.show()

# COMMAND ----------

filtered_df.write.saveAsTable("dua_058828_spa240.paper_4_final_patient_sample_2017_2019_12_months_no_dead", mode='overwrite')

# COMMAND ----------

