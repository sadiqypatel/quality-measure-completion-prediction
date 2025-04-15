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

df2019 = spark.table("dua_058828_spa240.paper_4_patient_sample_2019")
df2019 = df2019.select("beneID","state")
print((df2019.count(), len(df2019.columns)))

df2018 = spark.table("dua_058828_spa240.paper_4_patient_sample_2018")
print((df2018.count(), len(df2018.columns)))

# Count the number of distinct values in a column
distinct_count_2018 = df2018.select(col("state")).distinct().count()
distinct_count_2019 = df2019.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", distinct_count_2018)
print("Distinct count:", distinct_count_2019)

# COMMAND ----------

df2019.show()

# COMMAND ----------

df2018.show()

# COMMAND ----------

final = df2018.join(df2019, on=["beneID","state"], how="inner")
print((final.count(), len(final.columns)))

# Count the number of distinct values in a column
final_count = final.select(col("state")).distinct().count()

# Display the result
print("Distinct count:", final_count)

# COMMAND ----------

final.show()

# COMMAND ----------

# Group by the 'state' column and count the unique rows
state_counts = final.groupBy("state").count()

# Show the resulting DataFrame
state_counts.show(30)

# COMMAND ----------

final.write.saveAsTable("dua_058828_spa240.paper_4_final_patient_sample_both", mode='overwrite')