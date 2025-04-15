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

# COMMAND ----------

# MAGIC %md
# MAGIC PATIENT DEMOGRAPHICS

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_demo_file_01_12_months")
denom = denom.select("beneID","state", "sex", "age", "birthday")
print(denom.count())

# COMMAND ----------

denom.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Define the measurement year
measurement_year = 2018

# Calculate the age as of Dec 31 of the measurement year
denom = denom.withColumn("age", F.floor(F.datediff(F.lit(f"{measurement_year}-12-31"), F.col("birthday")) / 365.25))

# Filter the rows based on the criteria
eligible_population = denom.filter(
    (F.col("age").between(18, 100))
)

eligible_population.show()

# COMMAND ----------

print(eligible_population.count())

# COMMAND ----------

# MAGIC %md
# MAGIC EXCLUSIONS [ALREADY DONE]
# MAGIC
# MAGIC - DIED
# MAGIC - HOSPICE CARE

# COMMAND ----------

# MAGIC %md
# MAGIC IDENTIFY MEMBERS WITH ANY DEPRESSION MEDICATIONS

# COMMAND ----------

import pandas as pd
rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
#betos_df.head()
rx_df = spark.createDataFrame(rx_df)
rx_df = rx_df[rx_df['rxDcDrugName'].isin(['bupropion','bupropion [Aplenzin]','bupropion [Forfivo]','bupropion [Wellbutrin]','bupropion | naltrexone [Contrave]','Vilazodone','vortioxetine [Trintellix]','isocarboxazid [Marplan]','phenelzine','phenelzine [Nardil]','selegiline','selegiline [Emsam]','selegiline [Zelapar]','tranylcypromine','tranylcypromine [Parnate]','nefazodone','trazodone','amitriptyline | chlordiazepoxide','amitriptyline | perphenazine','fluoxetine | olanzapine','fluoxetine | olanzapine [Symbyax]','desvenlafaxine','desvenlafaxine [Pristiq]','duloxetine','duloxetine [Cymbalta]','duloxetine [Drizalma]','duloxetine [Irenka]','levomilnacipran','levomilnacipran [Fetzima]','venlafaxine','venlafaxine [Effexor]','citalopram','citalopram [Celexa]','escitalopram','escitalopram [Lexapro]','fluoxetine','fluoxetine [Prozac]','fluvoxamine','paroxetine','paroxetine [Brisdelle]','paroxetine [Paxil]','paroxetine [Pexeva]','sertraline','sertraline [Zoloft]','mirtazapine','mirtazapine [Remeron]','amitriptyline','amitriptyline [Elavil]','amoxapine','clomipramine','clomipramine [Anafranil]','desipramine','desipramine [Norpramin]','doxepin','doxepin [Prudoxin]','doxepin [Silenor]','doxepin [Zonalon]','Imipramine','nortriptyline','nortriptyline [Pamelor]','protriptyline','trimipramine'])]
rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
rx_df = rx_df.select("ndc", "rxDcDrugName").distinct()
print(rx_df.count())
rx_df.show(1000)

# COMMAND ----------

pharm2018 = spark.table("dua_058828_spa240.paper_4_pharm2018_12_months")
pharm = pharm2018
print(pharm.count())
pharm = pharm.select("beneID", "state", "RX_FILL_DT", "NDC")
pharm.show()

# COMMAND ----------

pharm.registerTempTable("connections")

pharm = spark.sql('''

SELECT DISTINCT beneID, state, RX_FILL_DT, NDC

FROM connections;
''')

pharm = pharm.filter(col("NDC").isNotNull())
pharm.show(200)

# COMMAND ----------

from pyspark.sql.functions import col, substring

dep_meds = pharm.join(
    rx_df,
    on='ndc',
    how='inner'
)

print(pharm.count())
print(dep_meds.count())
dep_meds.show()

# COMMAND ----------

dep_meds = dep_meds.select("beneID", "state", "RX_FILL_DT").distinct()
print(dep_meds.count())

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number


# Define the window specification
window_spec = Window.partitionBy("beneID", "state").orderBy("RX_FILL_DT")

# Add a row number column to identify the earliest date
df_with_row_num = dep_meds.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the earliest date for each unique beneID, state combination
final_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")

# Show the final DataFrame
print(dep_meds.count())
print(final_df.count())
final_df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, date_sub

index_df = final_df.withColumn("RX_FILL_DT", col("RX_FILL_DT").cast("date"))

# Add start_date and end_date columns
index_df = index_df.withColumn('start_date', date_sub(col('RX_FILL_DT'), 60)) \
       .withColumn('end_date', date_add(col('RX_FILL_DT'), 60))

# Show the results to verify correct application
index_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC REMOVE PEOPLE WITH NO MAJOR DEP DISCORDER DIAGNOSIS

# COMMAND ----------

outpat2017 = spark.table("dua_058828_spa240.paper_4_otherservices2017_12_months")
outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")
inpat2017 = spark.table("dua_058828_spa240.paper_4_inpatient2017_12_months_new")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

# COMMAND ----------

outpat2017 = outpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD", "POS_CD")
outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD", "POS_CD")
outpat2019 = outpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD", "POS_CD")

inpat2017 = inpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1","PRCDR_CD_1")
inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1","PRCDR_CD_1")
inpat2019 = inpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1","PRCDR_CD_1")

inpat2017 = inpat2017.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2019 = inpat2019.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

inpat2017 = inpat2017.withColumn('POS_CD', lit(None))
inpat2018 = inpat2018.withColumn('POS_CD', lit(None))
inpat2019 = inpat2019.withColumn('POS_CD', lit(None))

# Use reduce to apply union to all DataFrames in the list
all_claims = outpat2018.union(outpat2019).union(inpat2018).union(inpat2019).union(outpat2017).union(inpat2017)
all_claims.show()

# COMMAND ----------

# Define lists for the ICD-10-CM, CPT, HCPCS, and POS codes
icd10_codes = [
    'F320', 'F321', 'F322', 'F323', 'F324', 'F329', 'F330', 'F331', 'F332', 'F333', 'F3341', 'F3342', 'F339', 'F341'
]

bh_outpatient_cpt_codes = [
    '98960', '98961', '98962', '99078', '99202', '99203', '99204', '99205', '99211', '99212', '99213', '99214', '99215',
    '99242', '99243', '99244', '99245', '99341', '99342', '99343', '99344', '99345', '99347', '99348', '99349', '99350',
    '99381', '99382', '99383', '99384', '99385', '99386', '99387', '99391', '99392', '99393', '99394', '99395', '99396', '99397',
    '99401', '99402', '99403', '99404', '99411', '99412', '99483', '99492', '99493', '99494', '99510'
]

bh_outpatient_hcpcs_codes = [
    'G0155', 'G0176', 'G0177', 'G0409', 'G0463', 'G0512', 'H0002', 'H0004', 'H0031', 'H0034', 'H0036', 'H0037', 'H0039', 
    'H0040', 'H2000', 'H2010', 'H2011', 'H2013', 'H2014', 'H2015', 'H2016', 'H2017', 'H2018', 'H2019', 'H2020', 'T1015'
]

pos_codes = ['02', '10', '52', '53']
telehealth_pos_codes = ['10']
telehealth_cpt_codes = ['98966', '98967', '98968', '99441', '99442', '99443']
ed_visit_cpt_codes = ['99281', '99282', '99283', '99284', '99285']
online_assessment_cpt_codes = [
    '98970', '98971', '98972', '98980', '98981', '99421', '99422', '99423', '99457', '99458'
]
online_assessment_hcpcs_codes = ['G0071', 'G2010', 'G2012', 'G2250', 'G2251', 'G2252']

# Filter the DataFrame based on the specified conditions
filtered_df = all_claims.filter(
    (col('DGNS_CD_1').isin(icd10_codes)) &
    (
        col('LINE_PRCDR_CD').isin(bh_outpatient_cpt_codes + bh_outpatient_hcpcs_codes) & col('POS_CD').isin(pos_codes) |
        col('LINE_PRCDR_CD').isin(telehealth_cpt_codes) & col('POS_CD').isin(telehealth_pos_codes) |
        col('LINE_PRCDR_CD').isin(ed_visit_cpt_codes) |
        col('LINE_PRCDR_CD').isin(online_assessment_cpt_codes + online_assessment_hcpcs_codes)
    )
)

# Show the results to verify correct application
filtered_df.show()

# COMMAND ----------

# Select distinct rows based on beneID, state, and date
distinct_df = filtered_df.select('beneID', 'state', 'SRVC_BGN_DT').distinct()

# Add a column with value 1 called 'dep_visit'
final_df = distinct_df.withColumn('dep_visit', lit(1))

# Show the results to verify correct application
final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC IDENTIFY VISITS in 120 day period of index medication

# COMMAND ----------

combined = final_df.join(index_df, on=["beneID","state"], how="inner")
print(final_df.count())
print(combined.count())
combined.show()

# COMMAND ----------

# Add mdd_visit column based on whether rx_date is between start_date and end_date
combined = combined.withColumn(
    'mdd_visit',
    when((col('SRVC_BGN_DT') >= col('start_date')) & (col('SRVC_BGN_DT') <= col('end_date')), 1).otherwise(0)
)

# Show the results to verify correct application
combined.show()

# COMMAND ----------

from pyspark.sql.functions import col, date_add, date_sub, when, max as spark_max

# Group by beneID, state, RX_DATE and calculate max(mdd_visit)
combined = combined.groupBy('beneID', 'state', 'RX_FILL_DT') \
              .agg(spark_max('mdd_visit').alias('mdd_yes'))

# Show the results to verify correct application
combined.show()

# COMMAND ----------

values = combined.groupBy("mdd_yes").count()
values.show()

# COMMAND ----------

print(combined.count())

# COMMAND ----------

# Drop rows where mdd_visit is not 1
filtered_df = combined.filter(col('mdd_yes') == 1)
print(filtered_df.count())

# COMMAND ----------

filtered_df.write.saveAsTable("dua_058828_spa240.paper_4_amm_denom_12_months_new", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC Capture medication andherence 

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_amm_denom_12_months_new")
denom = denom.withColumnRenamed('RX_FILL_DT', 'index_date')
print(denom.count())

# COMMAND ----------

denom.show()

# COMMAND ----------

import pandas as pd
rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
#betos_df.head()
rx_df = spark.createDataFrame(rx_df)
rx_df = rx_df[rx_df['rxDcDrugName'].isin(['bupropion','bupropion [Aplenzin]','bupropion [Forfivo]','bupropion [Wellbutrin]','bupropion | naltrexone [Contrave]','Vilazodone','vortioxetine [Trintellix]','isocarboxazid [Marplan]','phenelzine','phenelzine [Nardil]','selegiline','selegiline [Emsam]','selegiline [Zelapar]','tranylcypromine','tranylcypromine [Parnate]','nefazodone','trazodone','amitriptyline | chlordiazepoxide','amitriptyline | perphenazine','fluoxetine | olanzapine','fluoxetine | olanzapine [Symbyax]','desvenlafaxine','desvenlafaxine [Pristiq]','duloxetine','duloxetine [Cymbalta]','duloxetine [Drizalma]','duloxetine [Irenka]','levomilnacipran','levomilnacipran [Fetzima]','venlafaxine','venlafaxine [Effexor]','citalopram','citalopram [Celexa]','escitalopram','escitalopram [Lexapro]','fluoxetine','fluoxetine [Prozac]','fluvoxamine','paroxetine','paroxetine [Brisdelle]','paroxetine [Paxil]','paroxetine [Pexeva]','sertraline','sertraline [Zoloft]','mirtazapine','mirtazapine [Remeron]','amitriptyline','amitriptyline [Elavil]','amoxapine','clomipramine','clomipramine [Anafranil]','desipramine','desipramine [Norpramin]','doxepin','doxepin [Prudoxin]','doxepin [Silenor]','doxepin [Zonalon]','Imipramine','nortriptyline','nortriptyline [Pamelor]','protriptyline','trimipramine'])]
rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
rx_df = rx_df.select("ndc", "rxDcDrugName").distinct()
print(rx_df.count())
rx_df.show(1000)

# COMMAND ----------

pharm2018 = spark.table("dua_058828_spa240.paper_4_pharm2018_12_months")
pharm2019 = spark.table("dua_058828_spa240.paper_4_pharm2019_12_months")
pharm = pharm2018.union(pharm2019)
#print(pharm.count())
pharm = pharm.select("beneID", "state", "RX_FILL_DT", "DAYS_SUPPLY", "NDC")
pharm.show()

# COMMAND ----------

pharm.registerTempTable("connections")

pharm = spark.sql('''

SELECT DISTINCT beneID, state, RX_FILL_DT, NDC, DAYS_SUPPLY

FROM connections;
''')

pharm = pharm.filter(col("NDC").isNotNull())
#pharm.show(200)

# COMMAND ----------

from pyspark.sql.functions import col, substring

dep_meds = pharm.join(
    rx_df,
    on='ndc',
    how='inner'
)

#print(pharm.count())
#print(dep_meds.count())
#dep_meds.show()

# COMMAND ----------

dep_meds = dep_meds.join(denom, on=["beneID","state"], how="inner")
#dep_meds.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 84 days

# COMMAND ----------

# Add a column 'index_date_plus_84' which is 'index_date' plus 84 days
dep_meds = dep_meds.withColumn('index_plus_84', date_add(col('index_date'), 84))

# Show the results to verify correct application
dep_meds =dep_meds.drop("mdd_yes","ndc")
#dep_meds.show()

# COMMAND ----------

# Filter the DataFrame where rx_fill_dt is less than index_date_plus_84
dep_meds = dep_meds.filter((col('RX_FILL_DT') < col('index_plus_84')) & (col('index_date') <= col('RX_FILL_DT')))
dep_meds = dep_meds.orderBy(['beneID', 'state', 'RX_FILL_DT'], ascending=[True, True, True])

# Show the results to verify correct application
dep_meds.show(150)

# COMMAND ----------

distinct = dep_meds.select("beneID","state", "RX_FILL_DT", "DAYS_SUPPLY").distinct()

# COMMAND ----------

# Sum days_supply by beneID and state
from pyspark.sql.functions import col, date_add, sum as spark_sum
distinct_agg = distinct.groupBy('beneID', 'state').agg(spark_sum('DAYS_SUPPLY').alias('outcome1'))

# COMMAND ----------

# Add outcome1_yes column based on the condition
distinct_agg = distinct_agg.withColumn('outcome1_yes', when(col('outcome1') >= 84, 1).otherwise(0))
distinct_agg.show()

# COMMAND ----------

denom = denom.join(distinct_agg, on=["beneID","state"], how="left").fillna(0)
print(denom.count())

# COMMAND ----------

value_outcome1 = denom.groupBy("outcome1_yes").count()
value_outcome1.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("outcome1_yes") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

denom.show()

# COMMAND ----------

denom.write.saveAsTable("dua_058828_spa240.paper_4_amm_outcome1_months_new", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC 180 days

# COMMAND ----------

# Add a column 'index_date_plus_84' which is 'index_date' plus 84 days
from pyspark.sql.functions import col, date_add, sum as spark_sum

dep_meds = dep_meds.withColumn('index_plus_180', date_add(col('index_date'), 180))

# Show the results to verify correct application
dep_meds =dep_meds.drop("mdd_yes","ndc")
#dep_meds.show()

# COMMAND ----------

# Filter the DataFrame where rx_fill_dt is less than index_date_plus_84
dep_meds = dep_meds.filter((col('RX_FILL_DT') < col('index_plus_180')) & (col('index_date') <= col('RX_FILL_DT')))
dep_meds = dep_meds.orderBy(['beneID', 'state', 'RX_FILL_DT'], ascending=[True, True, True])

# Show the results to verify correct application
dep_meds.show(150)

# COMMAND ----------

distinct = dep_meds.select("beneID","state", "RX_FILL_DT", "DAYS_SUPPLY").distinct()

# COMMAND ----------

# Sum days_supply by beneID and state
from pyspark.sql.functions import col, date_add, sum as spark_sum
distinct_agg = distinct.groupBy('beneID', 'state').agg(spark_sum('DAYS_SUPPLY').alias('outcome2'))

# COMMAND ----------

# Add outcome1_yes column based on the condition
distinct_agg = distinct_agg.withColumn('outcome2_yes', when(col('outcome2') >= 180, 1).otherwise(0))
distinct_agg.show()

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_amm_outcome1_months_new")
denom = denom.join(distinct_agg, on=["beneID","state"], how="left").fillna(0)
print(denom.count())

# COMMAND ----------

value_outcome1 = denom.groupBy("outcome2_yes").count()
value_outcome1.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("outcome2_yes") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

denom.show()

# COMMAND ----------

# MAGIC %md
# MAGIC SESNTIVITY ANALYSIS

# COMMAND ----------

df  = spark.table("dua_058828_spa240.paper_4_amm_both_outcomes_months_new")
df.show()

# COMMAND ----------

filtered_visits = df.filter(F.col("outcome1_yes") == 1)


# Assuming your DataFrame is named df
filtered_visits = filtered_visits.withColumn(
    "semester", 
    F.when(F.month(F.col("index_date")).between(1, 6), 1).otherwise(2)
)

from pyspark.sql import functions as F

# Perform value count for the 'semester' column
semester_count = filtered_visits.groupBy("semester").count()

# Count the total number of rows in the dataset
total_count = filtered_visits.count()

# Calculate the percentage for each semester value
semester_percentage = semester_count.withColumn(
    "percentage", (F.col("count") / total_count) * 100
)

# Show the result with count and percentage
semester_percentage.show()

# COMMAND ----------

# MAGIC %md
# MAGIC END SENSITIVITY ANALYSIS

# COMMAND ----------

denom.write.saveAsTable("dua_058828_spa240.paper_4_amm_both_outcomes_months_new", mode='overwrite')