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

from pyspark.ml.classification import LogisticRegression

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
    (F.col("age").between(6, 65))
)

eligible_population.show(200)

# COMMAND ----------

print(eligible_population.count())

# COMMAND ----------

# MAGIC %md
# MAGIC MEMBERS WHO DIED DURING MEASUREMENT YEAR 
# MAGIC - This is already removed

# COMMAND ----------

# MAGIC %md
# MAGIC STORE INITIAL DENOMINATOR

# COMMAND ----------

#eligible_population.write.saveAsTable("dua_058828_spa240.paper_4_fum30_denom_12_months", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC IDENTIFY HOSPITALIZATIONS

# COMMAND ----------

sample = eligible_population
#sample = spark.table("dua_058828_spa240.paper_4_fum30_denom_12_months")
sample = sample.select("beneID","state")
print(sample.count())
sample.show(10)

# COMMAND ----------

outpat = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
outpat_selected = outpat.select("beneID", "state", "CLM_ID" ,"REV_CNTR_CD" ,"LINE_PRCDR_CD" ,"SRVC_BGN_DT" ,"SRVC_END_DT", "DGNS_CD_1", "POS_CD")
print(outpat.count())
outpat_selected = outpat_selected.join(sample, how="inner", on=["beneID","state"])
print(outpat_selected.count())
outpat_selected = outpat_selected.withColumn("inpatientVisit", lit(0))
outpat_selected.show(10)

# COMMAND ----------

inpat = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat_selected = inpat.select("beneID", "state", "CLM_ID" ,"REV_CNTR_CD" ,"SRVC_BGN_DT", "SRVC_END_DT", "DGNS_CD_1")
print(inpat.count())
inpat_selected = inpat_selected.join(sample, how="inner", on=["beneID","state"])
print(inpat_selected.count())
inpat_selected = inpat_selected.withColumn("inpatientVisit", lit(1))
inpat_selected = inpat_selected.withColumn("EDvisit", lit(0))
#print(inpat_selected.printSchema())
#inpat_selected.show()

# COMMAND ----------

# Get unique states and their counts
state_counts = inpat_selected.groupBy("state").count()

# Print the number of unique states
num_unique_states = state_counts.count()
print(f"Number of unique states: {num_unique_states}")

# Show the result
state_counts.show(n=state_counts.count(), truncate=False)

# COMMAND ----------

# Get unique states and their counts
state_counts = outpat_selected.groupBy("state").count()

# Print the number of unique states
num_unique_states = state_counts.count()
print(f"Number of unique states: {num_unique_states}")

# Show the result
state_counts.show(n=state_counts.count(), truncate=False)

# COMMAND ----------

# Define the conditions for the "EDvisit" binary indicator
edvisit_conditions = (
    outpat_selected["REV_CNTR_CD"].isin(['0450', '0451', '0452', '0453', '0454', '0456', '0457', '0458', '0459', '0981']) |
    outpat_selected["POS_CD"].isin([23]) |
    outpat_selected["LINE_PRCDR_CD"].isin(['99281', '99282', '99283', '99284', '99285'])
)

# Create the "EDvisit" binary indicator based on the conditions
outpat_selected = outpat_selected.withColumn("EDvisit", when(edvisit_conditions, 1).otherwise(0))

# Create the "inpatientVisit" binary indicator and set it equal to 0
outpat_selected = outpat_selected.withColumn("inpatientVisit", lit(0))

# Filter out rows where "EDvisit" is not equal to 1
outpat_selected = outpat_selected.filter(outpat_selected["EDvisit"] == 1)

# Show the result
print(outpat_selected.count())
outpat_selected.show(1000)

# COMMAND ----------

inpatFinal = inpat_selected.select("beneID", "state", "CLM_ID","SRVC_BGN_DT" ,"SRVC_END_DT", "DGNS_CD_1", "EDvisit", "inpatientVisit")
outpatFinal = outpat_selected.select("beneID", "state", "CLM_ID","SRVC_BGN_DT" ,"SRVC_END_DT", "DGNS_CD_1", "EDvisit", "inpatientVisit")

# Show the result
inpatFinal.show(1)
outpatFinal.show(1)

# COMMAND ----------

df =  inpatFinal.union(outpatFinal)
df = df.withColumnRenamed("SRVC_BGN_DT", "StartDate").withColumnRenamed("SRVC_END_DT", "EndDate")
print(df.printSchema())

# COMMAND ----------

print(df.count())
df = df.join(sample, on=["beneID","state"], how="inner")
print(df.count())

# COMMAND ----------

df = df.withColumn("StartDate", col("StartDate").cast("date"))
df = df.withColumn("EndDate", col("EndDate").cast("date"))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, sum as cumsum, when, row_number
from pyspark.sql.window import Window

def episodesOfCare(df):
    # Define window specifications for calculating lag values, cumulative sum, and row number
    beneID_state_window = Window.partitionBy("beneID", "state").orderBy("StartDate", "EndDate")
    beneID_state_window_cumsum = Window.partitionBy("beneID", "state").orderBy("StartDate", "EndDate").rowsBetween(Window.unboundedPreceding, 0)

    # Calculate lag values for StartDate and EndDate columns
    df = df.withColumn("prev_StartDate", lag("StartDate").over(beneID_state_window))
    df = df.withColumn("prev_EndDate", lag("EndDate").over(beneID_state_window))

    # Calculate row number within each group
    df = df.withColumn("row_num", row_number().over(beneID_state_window))

    # Define conditions for new episode and overlap types
    new_episode_condition = (col("StartDate") > col("prev_EndDate") + 1) | col("prev_EndDate").isNull()
    regular_overlap_condition = (col("StartDate") <= col("prev_EndDate") + 1) & (col("EndDate") > col("prev_EndDate"))
    same_start_date_condition = (col("StartDate") == col("prev_StartDate")) & (col("EndDate") < col("prev_EndDate"))
    embedded_condition = (col("StartDate") > col("prev_StartDate")) & (col("EndDate") < col("prev_EndDate"))
    perfect_overlap_condition = (col("StartDate") == col("prev_StartDate")) & (col("EndDate") == col("prev_EndDate"))

    # Assign new episode flag based on condition
    df = df.withColumn("new_episode_flag", new_episode_condition.cast("int"))

    # Calculate episode numbers using cumulative sum
    df = df.withColumn("episode", cumsum("new_episode_flag").over(beneID_state_window_cumsum))

    df = df.withColumn("ovlp", 
                   when(col("row_num") == 1, "1.First")
                   .when(new_episode_condition, "2.New Episode")
                   .when(regular_overlap_condition, "3.Regular Overlap")
                   .when(same_start_date_condition, "5.Same Start Date (SRO)")
                   .when(embedded_condition, "6.Embedded")
                   .when(perfect_overlap_condition, "7.Perfect Overlap"))

    # Drop unnecessary columns
    df = df.drop("prev_StartDate", "prev_EndDate", "new_episode_flag", "row_num")

    return df
  
# Convert 'StartDate' and 'EndDate' columns to date type
df = df.withColumn("StartDate", col("StartDate").cast("date"))
df = df.withColumn("EndDate", col("EndDate").cast("date"))

# Apply the episodesOfCare function
result_df = episodesOfCare(df)

# Sort the DataFrame by beneID, state, StartDate, and EndDate
#result_df = result_df.orderBy("beneID", "state", "StartDate", "EndDate")

# Show the result
result_df.show(200)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, substring

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Combined list of mental health and suicide/self-harm ICD-10 codes without periods
# dx_codes = [
#     'F0390', 'F03911', 'F03918', 'F0392', 'F0393', 'F0394', 'F03A0', 'F03A11', 'F03A18', 'F03A2',
#     'F03A3', 'F03A4', 'F03B0', 'F03B11', 'F03B18', 'F03B2', 'F03B3', 'F03B4', 'F03C0', 'F03C11',
#     'F03C18', 'F03C2', 'F03C3', 'F03C4', 'F200', 'F201', 'F202', 'F203', 'F205', 'F2081', 'F2089',
#     'F209', 'F21', 'F22', 'F23', 'F24', 'F250', 'F251', 'F258', 'F259', 'F28', 'F29', 'F3010', 'F3011',
#     'F3012', 'F3013', 'F302', 'F303', 'F304', 'F308', 'F309', 'F310', 'F3110', 'F3111', 'F3112',
#     'F3113', 'F312', 'F3130', 'F3131', 'F3132', 'F314', 'F315', 'F3160', 'F3161', 'F3162', 'F3163',
#     'F3164', 'F3170', 'F3171', 'F3172', 'F3173', 'F3174', 'F3175', 'F3176', 'F3177', 'F3178', 'F3181',
#     'F3189', 'F319', 'F320', 'F321', 'F322', 'F323', 'F324', 'F325', 'F3281', 'F3289', 'F329', 'F32A',
#     'F330', 'F331', 'F332', 'F333', 'F3340', 'F3341', 'F3342', 'F338', 'F339', 'F340', 'F341', 'F3481',
#     'F3489', 'F349', 'F39', 'F4000', 'F4001', 'F4002', 'F4010', 'F4011', 'F40210', 'F40218', 'F40220',
#     'F40228', 'F40230', 'F40231', 'F40232', 'F40233', 'F40240', 'F40241', 'F40242', 'F40243', 'F40248',
#     'F40290', 'F40291', 'F40298', 'F408', 'F409', 'F410', 'F411', 'F413', 'F418', 'F419', 'F422',
#     'F423', 'F424', 'F428', 'F429', 'F430', 'F4310', 'F4311', 'F4312', 'F4320', 'F4321', 'F4322',
#     'F4323', 'F4324', 'F4325', 'F4329', 'F4381', 'F4389', 'F439', 'F440', 'F441', 'F442', 'F444',
#     'F445', 'F446', 'F447', 'F4481', 'F4489', 'F449', 'F450', 'F451', 'F4520', 'F4521', 'F4522',
#     'F4529', 'F4541', 'F4542', 'F458', 'F459', 'F481', 'F482', 'F488', 'F489', 'F5000', 'F5001',
#     'F5002', 'F502', 'F5081', 'F5082', 'F5089', 'F509', 'F5101', 'F5102', 'F5103', 'F5104', 'F5105',
#     'F5109', 'F5111', 'F5112', 'F5113', 'F5119', 'F513', 'F514', 'F515', 'F518', 'F519', 'F520',
#     'F521', 'F5221', 'F5222', 'F5231', 'F5232', 'F524', 'F525', 'F526', 'F528', 'F529', 'F530',
#     'F531', 'F59', 'F600', 'F601', 'F602', 'F603', 'F604', 'F605', 'F606', 'F607', 'F6081',
#     'F6089', 'F609', 'F630', 'F631', 'F632', 'F633', 'F6381', 'F6389', 'F639', 'F640', 'F641',
#     'F642', 'F648', 'F649', 'F650', 'F651', 'F652', 'F653', 'F654', 'F6550', 'F6551', 'F6552',
#     'F6581', 'F6589', 'F659', 'F66', 'F6810', 'F6811', 'F6812', 'F6813', 'F688', 'F68A', 'F69',
#     'F800', 'F801', 'F802', 'F804', 'F8081', 'F8082', 'F8089', 'F809', 'F810', 'F812', 'F8181',
#     'F8189', 'F819', 'F82', 'F840', 'F842', 'F843', 'F845', 'F848', 'F849', 'F88', 'F89',
#     'F900', 'F901', 'F902', 'F908', 'F909', 'F910', 'F911', 'F912', 'F913', 'F918', 'F919',
#     'F930', 'F938', 'F939', 'F940', 'F941', 'F942', 'F948', 'F949', 'F950', 'F951', 'F952',
#     'F958', 'F959', 'F980', 'F981', 'F9821', 'F9829', 'F983', 'F984', 'F985', 'F988', 'F989', 'F99',
#     'T1491XA', 'T1491XD', 'T1491XS', 'T360X2A', 'T360X2D', 'T360X2S', 'T361X2A', 'T361X2D', 'T361X2S',
#     'T362X2A', 'T362X2D', 'T362X2S', 'T363X2A', 'T363X2D', 'T363X2S', 'T364X2A', 'T364X2D', 'T364X2S',
#     'T365X2A', 'T365X2D', 'T365X2S', 'T366X2A', 'T366X2D', 'T366X2S', 'T367X2A', 'T367X2D', 'T367X2S',
#     'T368X2A', 'T368X2D', 'T368X2S', 'T3692XA', 'T3692XD', 'T3692XS', 'T370X2A', 'T370X2D', 'T370X2S',
#     'T371X2A', 'T371X2D', 'T371X2S', 'T372X2A', 'T372X2D', 'T372X2S', 'T373X2A', 'T373X2D', 'T373X2S',
#     'T374X2A', 'T374X2D', 'T374X2S', 'T375X2A', 'T375X2D', 'T375X2S', 'T378X2A', 'T378X2D', 'T378X2S',
#     'T3792XA', 'T3792XD', 'T3792XS', 'T380X2A', 'T380X2D', 'T380X2S', 'T381X2A', 'T381X2D', 'T381X2S',
#     'T382X2A', 'T382X2D', 'T382X2S', 'T383X2A', 'T383X2D', 'T383X2S', 'T384X2A', 'T384X2D', 'T384X2S',
#     'T385X2A', 'T385X2D', 'T385X2S', 'T386X2A', 'T386X2D', 'T386X2S', 'T387X2A', 'T387X2D', 'T387X2S',
#     'T38802A', 'T38802D', 'T38802S', 'T38812A', 'T38812D', 'T38812S', 'T38892A', 'T38892D', 'T38892S',
#     'T38902A', 'T38902D', 'T38902S', 'T38992A', 'T38992D', 'T38992S', 'T39012A', 'T39012D', 'T39012S',
#     'T39092A', 'T39092D', 'T39092S', 'T391X2A', 'T391X2D', 'T391X2S', 'T392X2A', 'T392X2D', 'T392X2S',
#     'T39312A', 'T39312D', 'T39312S', 'T39392A', 'T39392D', 'T39392S', 'T394X2A', 'T394X2D', 'T394X2S',
#     'T398X2A', 'T398X2D', 'T398X2S', 'T3992XA', 'T3992XD', 'T3992XS', 'T400X2A', 'T400X2D', 'T400X2S',
#     'T401X2A', 'T401X2D', 'T401X2S', 'T402X2A', 'T402X2D', 'T402X2S', 'T403X2A', 'T403X2D', 'T403X2S',
#     'T40412A', 'T40412D', 'T40412S', 'T40422A', 'T40422D', 'T40422S', 'T40492A', 'T40492D', 'T40492S',
#     'T405X2A', 'T405X2D', 'T405X2S', 'T40602A', 'T40602D', 'T40602S', 'T40692A', 'T40692D', 'T40692S',
#     'T40712A', 'T40712D', 'T40712S', 'T40722A', 'T40722D', 'T40722S', 'T408X2A', 'T408X2D', 'T408X2S',
#     'T40902A', 'T40902D', 'T40902S', 'T40992A', 'T40992D', 'T40992S', 'T410X2A', 'T410X2D', 'T410X2S',
#     'T411X2A', 'T411X2D', 'T411X2S', 'T41202A', 'T41202D', 'T41202S', 'T41292A', 'T41292D', 'T41292S',
#     'T413X2A', 'T413X2D', 'T413X2S', 'T4142XA', 'T4142XD', 'T4142XS', 'T415X2A', 'T415X2D', 'T415X2S',
#     'T420X2A', 'T420X2D', 'T420X2S', 'T421X2A', 'T421X2D', 'T421X2S', 'T422X2A', 'T422X2D', 'T422X2S',
#     'T423X2A'
# ]

dx_codes = [
    'F0390', 'F03911', 'F03918', 'F0392', 'F0393', 'F0394', 'F03A0', 'F03A11', 'F03A18', 'F03A2',
    'F03A3', 'F03A4', 'F03B0', 'F03B11', 'F03B18', 'F03B2', 'F03B3', 'F03B4', 'F03C0', 'F03C11',
    'F03C18', 'F03C2', 'F03C3', 'F03C4', 'F200', 'F201', 'F202', 'F203', 'F205', 'F2081', 'F2089',
    'F209', 'F21', 'F22', 'F23', 'F24', 'F250', 'F251', 'F258', 'F259', 'F28', 'F29', 'F3010', 'F3011',
    'F3012', 'F3013', 'F302', 'F303', 'F304', 'F308', 'F309', 'F310', 'F3110', 'F3111', 'F3112',
    'F3113', 'F312', 'F3130', 'F3131', 'F3132', 'F314', 'F315', 'F3160', 'F3161', 'F3162', 'F3163',
    'F3164', 'F3170', 'F3171', 'F3172', 'F3173', 'F3174', 'F3175', 'F3176', 'F3177', 'F3178', 'F3181',
    'F3189', 'F319', 'F320', 'F321', 'F322', 'F323', 'F324', 'F325', 'F3281', 'F3289', 'F329', 'F32A',
    'F330', 'F331', 'F332', 'F333', 'F3340', 'F3341', 'F3342', 'F338', 'F339', 'F340', 'F341', 'F3481',
    'F3489', 'F349', 'F39', 'F4000', 'F4001', 'F4002', 'F4010', 'F4011', 'F40210', 'F40218', 'F40220',
    'F40228', 'F40230', 'F40231', 'F40232', 'F40233', 'F40240', 'F40241', 'F40242', 'F40243', 'F40248',
    'F40290', 'F40291', 'F40298', 'F408', 'F409', 'F410', 'F411', 'F413', 'F418', 'F419', 'F422',
    'F423', 'F424', 'F428', 'F429', 'F430', 'F4310', 'F4311', 'F4312', 'F4320', 'F4321', 'F4322',
    'F4323', 'F4324', 'F4325', 'F4329', 'F4381', 'F4389', 'F439', 'F440', 'F441', 'F442', 'F444',
    'F445', 'F446', 'F447', 'F4481', 'F4489', 'F449', 'F450', 'F451', 'F4520', 'F4521', 'F4522',
    'F4529', 'F4541', 'F4542', 'F458', 'F459', 'F481', 'F482', 'F488', 'F489', 'F5000', 'F5001',
    'F5002', 'F502', 'F5081', 'F5082', 'F5089', 'F509', 'F5101', 'F5102', 'F5103', 'F5104', 'F5105',
    'F5109', 'F5111', 'F5112', 'F5113', 'F5119', 'F513', 'F514', 'F515', 'F518', 'F519', 'F520',
    'F521', 'F5221', 'F5222', 'F5231', 'F5232', 'F524', 'F525', 'F526', 'F528', 'F529', 'F530',
    'F531', 'F59', 'F600', 'F601', 'F602', 'F603', 'F604', 'F605', 'F606', 'F607', 'F6081',
    'F6089', 'F609', 'F630', 'F631', 'F632', 'F633', 'F6381', 'F6389', 'F639', 'F640', 'F641',
    'F642', 'F648', 'F649', 'F650', 'F651', 'F652', 'F653', 'F654', 'F6550', 'F6551', 'F6552',
    'F6581', 'F6589', 'F659', 'F66', 'F6810', 'F6811', 'F6812', 'F6813', 'F688', 'F68A', 'F69',
    'F800', 'F801', 'F802', 'F804', 'F8081', 'F8082', 'F8089', 'F809', 'F810', 'F812', 'F8181',
    'F8189', 'F819', 'F82', 'F840', 'F842', 'F843', 'F845', 'F848', 'F849', 'F88', 'F89',
    'F900', 'F901', 'F902', 'F908', 'F909', 'F910', 'F911', 'F912', 'F913', 'F918', 'F919',
    'F930', 'F938', 'F939', 'F940', 'F941', 'F942', 'F948', 'F949', 'F950', 'F951', 'F952',
    'F958', 'F959', 'F980', 'F981', 'F9821', 'F9829', 'F983', 'F984', 'F985', 'F988', 'F989', 'F99'
]


# Create a new column 'bh' based on whether the first 3 characters of 'DGNS_CD_1' are in the list of ICD codes
   
result_df = result_df.withColumn(
    "bh",
    F.when(F.col("DGNS_CD_1").isin(dx_codes), 1 
    ).otherwise(0)
)

# Show the result
result_df.show(300)

# COMMAND ----------

bh_visits = result_df.groupBy("bh").count()
bh_visits.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Aggregating by beneID, state, episode
aggregated_df = result_df.groupBy("beneID", "state", "episode").agg(
    F.min("StartDate").alias("min_StartDate"),
    F.max("EndDate").alias("max_EndDate"),
    F.max("bh").alias("bh_yes"),
    F.max("inpatientVisit").alias("max_inpatientVisit"),
    F.max("EDvisit").alias("max_EDvisit")
)

aggregated_df.show()

# COMMAND ----------

# Dropping rows where max_inpatientVisit is 0
# Filter the DataFrame
aggregated_df = aggregated_df.filter((col('max_EdVisit') == 1) & (col('max_inpatientVisit') == 0))
aggregated_df = aggregated_df.filter((col('bh_yes') == 1))

# Show the filtered results
aggregated_df.show(50)

# COMMAND ----------

# Create index_visits where max_EndDate is any date before 2018-12-01
index_visits = aggregated_df.filter(col('max_EndDate') < '2018-12-01')
print(index_visits.count())

# COMMAND ----------

index_visits.write.saveAsTable("dua_058828_spa240.paper_4_fum30_denominator_ed_visits_12_months_new3", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC IDENTIFY ALL MH OUTPATIENT VISITS

# COMMAND ----------

outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
print(outpat2018.count())
outpat2018.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Identify BH VISITS [defined by primary MH or SUICIDE ICD-10-CM]

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, substring

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

dx_codes = [
    'F0390', 'F03911', 'F03918', 'F0392', 'F0393', 'F0394', 'F03A0', 'F03A11', 'F03A18', 'F03A2',
    'F03A3', 'F03A4', 'F03B0', 'F03B11', 'F03B18', 'F03B2', 'F03B3', 'F03B4', 'F03C0', 'F03C11',
    'F03C18', 'F03C2', 'F03C3', 'F03C4', 'F200', 'F201', 'F202', 'F203', 'F205', 'F2081', 'F2089',
    'F209', 'F21', 'F22', 'F23', 'F24', 'F250', 'F251', 'F258', 'F259', 'F28', 'F29', 'F3010', 'F3011',
    'F3012', 'F3013', 'F302', 'F303', 'F304', 'F308', 'F309', 'F310', 'F3110', 'F3111', 'F3112',
    'F3113', 'F312', 'F3130', 'F3131', 'F3132', 'F314', 'F315', 'F3160', 'F3161', 'F3162', 'F3163',
    'F3164', 'F3170', 'F3171', 'F3172', 'F3173', 'F3174', 'F3175', 'F3176', 'F3177', 'F3178', 'F3181',
    'F3189', 'F319', 'F320', 'F321', 'F322', 'F323', 'F324', 'F325', 'F3281', 'F3289', 'F329', 'F32A',
    'F330', 'F331', 'F332', 'F333', 'F3340', 'F3341', 'F3342', 'F338', 'F339', 'F340', 'F341', 'F3481',
    'F3489', 'F349', 'F39', 'F4000', 'F4001', 'F4002', 'F4010', 'F4011', 'F40210', 'F40218', 'F40220',
    'F40228', 'F40230', 'F40231', 'F40232', 'F40233', 'F40240', 'F40241', 'F40242', 'F40243', 'F40248',
    'F40290', 'F40291', 'F40298', 'F408', 'F409', 'F410', 'F411', 'F413', 'F418', 'F419', 'F422',
    'F423', 'F424', 'F428', 'F429', 'F430', 'F4310', 'F4311', 'F4312', 'F4320', 'F4321', 'F4322',
    'F4323', 'F4324', 'F4325', 'F4329', 'F4381', 'F4389', 'F439', 'F440', 'F441', 'F442', 'F444',
    'F445', 'F446', 'F447', 'F4481', 'F4489', 'F449', 'F450', 'F451', 'F4520', 'F4521', 'F4522',
    'F4529', 'F4541', 'F4542', 'F458', 'F459', 'F481', 'F482', 'F488', 'F489', 'F5000', 'F5001',
    'F5002', 'F502', 'F5081', 'F5082', 'F5089', 'F509', 'F5101', 'F5102', 'F5103', 'F5104', 'F5105',
    'F5109', 'F5111', 'F5112', 'F5113', 'F5119', 'F513', 'F514', 'F515', 'F518', 'F519', 'F520',
    'F521', 'F5221', 'F5222', 'F5231', 'F5232', 'F524', 'F525', 'F526', 'F528', 'F529', 'F530',
    'F531', 'F59', 'F600', 'F601', 'F602', 'F603', 'F604', 'F605', 'F606', 'F607', 'F6081',
    'F6089', 'F609', 'F630', 'F631', 'F632', 'F633', 'F6381', 'F6389', 'F639', 'F640', 'F641',
    'F642', 'F648', 'F649', 'F650', 'F651', 'F652', 'F653', 'F654', 'F6550', 'F6551', 'F6552',
    'F6581', 'F6589', 'F659', 'F66', 'F6810', 'F6811', 'F6812', 'F6813', 'F688', 'F68A', 'F69',
    'F800', 'F801', 'F802', 'F804', 'F8081', 'F8082', 'F8089', 'F809', 'F810', 'F812', 'F8181',
    'F8189', 'F819', 'F82', 'F840', 'F842', 'F843', 'F845', 'F848', 'F849', 'F88', 'F89',
    'F900', 'F901', 'F902', 'F908', 'F909', 'F910', 'F911', 'F912', 'F913', 'F918', 'F919',
    'F930', 'F938', 'F939', 'F940', 'F941', 'F942', 'F948', 'F949', 'F950', 'F951', 'F952',
    'F958', 'F959', 'F980', 'F981', 'F9821', 'F9829', 'F983', 'F984', 'F985', 'F988', 'F989', 'F99',
    'T1491XA', 'T1491XD', 'T1491XS', 'T360X2A', 'T360X2D', 'T360X2S', 'T361X2A', 'T361X2D', 'T361X2S',
    'T362X2A', 'T362X2D', 'T362X2S', 'T363X2A', 'T363X2D', 'T363X2S', 'T364X2A', 'T364X2D', 'T364X2S',
    'T365X2A', 'T365X2D', 'T365X2S', 'T366X2A', 'T366X2D', 'T366X2S', 'T367X2A', 'T367X2D', 'T367X2S',
    'T368X2A', 'T368X2D', 'T368X2S', 'T3692XA', 'T3692XD', 'T3692XS', 'T370X2A', 'T370X2D', 'T370X2S',
    'T371X2A', 'T371X2D', 'T371X2S', 'T372X2A', 'T372X2D', 'T372X2S', 'T373X2A', 'T373X2D', 'T373X2S',
    'T374X2A', 'T374X2D', 'T374X2S', 'T375X2A', 'T375X2D', 'T375X2S', 'T378X2A', 'T378X2D', 'T378X2S',
    'T3792XA', 'T3792XD', 'T3792XS', 'T380X2A', 'T380X2D', 'T380X2S', 'T381X2A', 'T381X2D', 'T381X2S',
    'T382X2A', 'T382X2D', 'T382X2S', 'T383X2A', 'T383X2D', 'T383X2S', 'T384X2A', 'T384X2D', 'T384X2S',
    'T385X2A', 'T385X2D', 'T385X2S', 'T386X2A', 'T386X2D', 'T386X2S', 'T387X2A', 'T387X2D', 'T387X2S',
    'T38802A', 'T38802D', 'T38802S', 'T38812A', 'T38812D', 'T38812S', 'T38892A', 'T38892D', 'T38892S',
    'T38902A', 'T38902D', 'T38902S', 'T38992A', 'T38992D', 'T38992S', 'T39012A', 'T39012D', 'T39012S',
    'T39092A', 'T39092D', 'T39092S', 'T391X2A', 'T391X2D', 'T391X2S', 'T392X2A', 'T392X2D', 'T392X2S',
    'T39312A', 'T39312D', 'T39312S', 'T39392A', 'T39392D', 'T39392S', 'T394X2A', 'T394X2D', 'T394X2S',
    'T398X2A', 'T398X2D', 'T398X2S', 'T3992XA', 'T3992XD', 'T3992XS', 'T400X2A', 'T400X2D', 'T400X2S',
    'T401X2A', 'T401X2D', 'T401X2S', 'T402X2A', 'T402X2D', 'T402X2S', 'T403X2A', 'T403X2D', 'T403X2S',
    'T40412A', 'T40412D', 'T40412S', 'T40422A', 'T40422D', 'T40422S', 'T40492A', 'T40492D', 'T40492S',
    'T405X2A', 'T405X2D', 'T405X2S', 'T40602A', 'T40602D', 'T40602S', 'T40692A', 'T40692D', 'T40692S',
    'T40712A', 'T40712D', 'T40712S', 'T40722A', 'T40722D', 'T40722S', 'T408X2A', 'T408X2D', 'T408X2S',
    'T40902A', 'T40902D', 'T40902S', 'T40992A', 'T40992D', 'T40992S', 'T410X2A', 'T410X2D', 'T410X2S',
    'T411X2A', 'T411X2D', 'T411X2S', 'T41202A', 'T41202D', 'T41202S', 'T41292A', 'T41292D', 'T41292S',
    'T413X2A', 'T413X2D', 'T413X2S', 'T4142XA', 'T4142XD', 'T4142XS', 'T415X2A', 'T415X2D', 'T415X2S',
    'T420X2A', 'T420X2D', 'T420X2S', 'T421X2A', 'T421X2D', 'T421X2S', 'T422X2A', 'T422X2D', 'T422X2S',
    'T423X2A'
]

# dx_codes = [
#     'F0390', 'F03911', 'F03918', 'F0392', 'F0393', 'F0394', 'F03A0', 'F03A11', 'F03A18', 'F03A2',
#     'F03A3', 'F03A4', 'F03B0', 'F03B11', 'F03B18', 'F03B2', 'F03B3', 'F03B4', 'F03C0', 'F03C11',
#     'F03C18', 'F03C2', 'F03C3', 'F03C4', 'F200', 'F201', 'F202', 'F203', 'F205', 'F2081', 'F2089',
#     'F209', 'F21', 'F22', 'F23', 'F24', 'F250', 'F251', 'F258', 'F259', 'F28', 'F29', 'F3010', 'F3011',
#     'F3012', 'F3013', 'F302', 'F303', 'F304', 'F308', 'F309', 'F310', 'F3110', 'F3111', 'F3112',
#     'F3113', 'F312', 'F3130', 'F3131', 'F3132', 'F314', 'F315', 'F3160', 'F3161', 'F3162', 'F3163',
#     'F3164', 'F3170', 'F3171', 'F3172', 'F3173', 'F3174', 'F3175', 'F3176', 'F3177', 'F3178', 'F3181',
#     'F3189', 'F319', 'F320', 'F321', 'F322', 'F323', 'F324', 'F325', 'F3281', 'F3289', 'F329', 'F32A',
#     'F330', 'F331', 'F332', 'F333', 'F3340', 'F3341', 'F3342', 'F338', 'F339', 'F340', 'F341', 'F3481',
#     'F3489', 'F349', 'F39', 'F4000', 'F4001', 'F4002', 'F4010', 'F4011', 'F40210', 'F40218', 'F40220',
#     'F40228', 'F40230', 'F40231', 'F40232', 'F40233', 'F40240', 'F40241', 'F40242', 'F40243', 'F40248',
#     'F40290', 'F40291', 'F40298', 'F408', 'F409', 'F410', 'F411', 'F413', 'F418', 'F419', 'F422',
#     'F423', 'F424', 'F428', 'F429', 'F430', 'F4310', 'F4311', 'F4312', 'F4320', 'F4321', 'F4322',
#     'F4323', 'F4324', 'F4325', 'F4329', 'F4381', 'F4389', 'F439', 'F440', 'F441', 'F442', 'F444',
#     'F445', 'F446', 'F447', 'F4481', 'F4489', 'F449', 'F450', 'F451', 'F4520', 'F4521', 'F4522',
#     'F4529', 'F4541', 'F4542', 'F458', 'F459', 'F481', 'F482', 'F488', 'F489', 'F5000', 'F5001',
#     'F5002', 'F502', 'F5081', 'F5082', 'F5089', 'F509', 'F5101', 'F5102', 'F5103', 'F5104', 'F5105',
#     'F5109', 'F5111', 'F5112', 'F5113', 'F5119', 'F513', 'F514', 'F515', 'F518', 'F519', 'F520',
#     'F521', 'F5221', 'F5222', 'F5231', 'F5232', 'F524', 'F525', 'F526', 'F528', 'F529', 'F530',
#     'F531', 'F59', 'F600', 'F601', 'F602', 'F603', 'F604', 'F605', 'F606', 'F607', 'F6081',
#     'F6089', 'F609', 'F630', 'F631', 'F632', 'F633', 'F6381', 'F6389', 'F639', 'F640', 'F641',
#     'F642', 'F648', 'F649', 'F650', 'F651', 'F652', 'F653', 'F654', 'F6550', 'F6551', 'F6552',
#     'F6581', 'F6589', 'F659', 'F66', 'F6810', 'F6811', 'F6812', 'F6813', 'F688', 'F68A', 'F69',
#     'F800', 'F801', 'F802', 'F804', 'F8081', 'F8082', 'F8089', 'F809', 'F810', 'F812', 'F8181',
#     'F8189', 'F819', 'F82', 'F840', 'F842', 'F843', 'F845', 'F848', 'F849', 'F88', 'F89',
#     'F900', 'F901', 'F902', 'F908', 'F909', 'F910', 'F911', 'F912', 'F913', 'F918', 'F919',
#     'F930', 'F938', 'F939', 'F940', 'F941', 'F942', 'F948', 'F949', 'F950', 'F951', 'F952',
#     'F958', 'F959', 'F980', 'F981', 'F9821', 'F9829', 'F983', 'F984', 'F985', 'F988', 'F989', 'F99'
# ]

# Create a new column 'bh' based on whether the first 3 characters of 'DGNS_CD_1' are in the list of ICD codes
outpat2018 = outpat2018.withColumn(
    'bh_dx',
    when(
        (col('DGNS_CD_1')).isin(dx_codes),
        1
    ).otherwise(0)
)

# Show the result
outpat2018.show(300)

# COMMAND ----------

# Perform a value count on the 'bh_provider' field
value_counts = outpat2018.groupBy('bh_dx').count()

# Show the results of the value count
value_counts.show()

# COMMAND ----------

# Filter the DataFrame to keep only rows where bh equals 1
#outpat2018 = outpat2018.filter(outpat2018.bh_dx == 1)

# Show the results to verify correct application
outpat2018.show()

# COMMAND ----------

print(outpat2018.count())

# COMMAND ----------

# MAGIC %md
# MAGIC VISITS

# COMMAND ----------

from pyspark.sql.functions import col

# Cast columns to string if necessary
outpat2018 = outpat2018.withColumn("LINE_PRCDR_CD", col("LINE_PRCDR_CD").cast("string"))
outpat2018 = outpat2018.withColumn("POS_CD", col("POS_CD").cast("string"))

# COMMAND ----------

# Define lists for Electroconvulsive Therapy CPT, ICD-10-PCS, and POS codes
electroconvulsive_cpt_codes = ['90870']
electroconvulsive_icd10pcs_codes = ['GZB0ZZZ', 'GZB1ZZZ', 'GZB2ZZZ', 'GZB3ZZZ', 'GZB4ZZZ']
electroconvulsive_pos_codes = [
    '24', '53', '52', '03', '05', '07', '09', '11', '12', '13', '14', '15', '16', '17', '18', '19', 
    '20', '22', '33', '49', '50', '71', '72'
]

# Create the 'bh_visit1' indicator based on the specified conditions
outpat2018 = outpat2018.withColumn(
    'bh_visit1',
    when(
        (col('LINE_PRCDR_CD').isin(electroconvulsive_cpt_codes + electroconvulsive_icd10pcs_codes)) & 
        (col('POS_CD').isin(electroconvulsive_pos_codes)),
        1
    ).otherwise(0)
)

# Show the results to verify correct application
#outpat2018.show()

# COMMAND ----------

# Perform a value count on the 'bh_provider' field
value_counts = outpat2018.groupBy('bh_visit1').count()

# Show the results of the value count
value_counts.show()

# COMMAND ----------

# Define lists for BH Outpatient Visit CPT and HCPCS codes
bh_outpatient_cpt_codes = [
    '98960', '98961', '98962', '99078', '99202', '99203', '99204', '99205', '99211', '99212', '99213', '99214', '99215',
    '99242', '99243', '99244', '99245', '99341', '99342', '99344', '99345', '99347', '99348', '99349', '99350',
    '99381', '99382', '99383', '99384', '99385', '99386', '99387', '99391', '99392', '99393', '99394', '99395', '99396', '99397',
    '99401', '99402', '99403', '99404', '99411', '99412', '99483', '99492', '99493', '99494', '99510'
]
bh_outpatient_hcpcs_codes = [
    'G0155', 'G0176', 'G0177', 'G0409', 'G0463', 'G0512', 'H0002', 'H0004', 'H0031', 'H0034', 'H0036', 'H0037', 'H0039', 
    'H0040', 'H2000', 'H2010', 'H2011', 'H2013', 'H2014', 'H2015', 'H2016', 'H2017', 'H2018', 'H2019', 'H2020', 'T1015'
]

bh_dx = [1]

# Create the 'bh_visit2' indicator based on the specified conditions
outpat2018 = outpat2018.withColumn(
    'bh_visit2',
    when(
        col('bh_dx').isin(bh_dx) &
        col('LINE_PRCDR_CD').isin(bh_outpatient_cpt_codes + bh_outpatient_hcpcs_codes),
        1
    ).otherwise(0)
)

# Show the results to verify correct application
outpat2018.show()

# COMMAND ----------

# Perform a value count on the 'bh_provider' field
value_counts = outpat2018.groupBy('bh_visit2').count()

# Show the results of the value count
value_counts.show()

# COMMAND ----------

# Define lists for Outpatient Visit CPT codes and POS codes
outpatient_visit_cpt_codes = [
    '90791', '90792', '90832', '90833', '90834', '90836', '90837', '90838', '90839', '90840', 
    '90845', '90847', '90849', '90853', '90875', '90876', '99221', '99222', '99223', 
    '99231', '99232', '99233', '99238', '99239', '99252', '99253', '99254', '99255'
]
pos_codes_community_mental_health = ['53']
pos_codes_partial_hospitalization = ['52']
pos_codes_telehealth = ['02', '10']
pos_codes_outpatient = [
    '03', '05', '07', '09', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '22', '33', '49', '50', '71', '72'
]

# Create the 'bh_visit3' indicator based on the specified conditions
outpat2018 = outpat2018.withColumn(
    'bh_visit3',
    when(
        (col('LINE_PRCDR_CD').isin(outpatient_visit_cpt_codes)) & 
        (col('POS_CD').isin(pos_codes_community_mental_health + pos_codes_partial_hospitalization + pos_codes_telehealth + pos_codes_outpatient)),
        1
    ).otherwise(0)
)

# Show the results to verify correct application
outpat2018.show()

# COMMAND ----------

# Perform a value count on the 'bh_provider' field
value_counts = outpat2018.groupBy('bh_visit3').count()

# Show the results of the value count
value_counts.show()

# COMMAND ----------

# Define lists for Partial Hospitalization or Intensive Outpatient, Psychiatric Collaborative Care Management,
# Telephone Visits, Transitional Care Management, and POS codes
partial_hospitalization_hcpcs_codes = [
    'G0410', 'G0411', 'H0035', 'H2001', 'H2012', 'S0201', 'S9480', 'S9484', 'S9485'
]
psychiatric_collaborative_care_management_cpt_codes = ['99492', '99493', '99494']
psychiatric_collaborative_care_management_hcpcs_codes = ['G0512']
telephone_visit_cpt_codes = ['98966', '98967', '98968', '99441', '99442', '99443']
transitional_care_management_cpt_codes = ['99495', '99496']
community_mental_health_pos_codes = ['53']

# Combined list of HCPCS, CPT, and ICD-10-PCS codes
combined_codes = [
    # Electroconvulsive Therapy CPT and ICD-10-PCS codes
    '90870', 'GZB0ZZZ', 'GZB1ZZZ', 'GZB2ZZZ', 'GZB3ZZZ', 'GZB4ZZZ',
    
    # BH Outpatient Visit CPT and HCPCS codes
    '98960', '98961', '98962', '99078', '99202', '99203', '99204', '99205', '99211', '99212', '99213', '99214', '99215',
    '99242', '99243', '99244', '99245', '99341', '99342', '99344', '99345', '99347', '99348', '99349', '99350',
    '99381', '99382', '99383', '99384', '99385', '99386', '99387', '99391', '99392', '99393', '99394', '99395', '99396', '99397',
    '99401', '99402', '99403', '99404', '99411', '99412', '99483', '99492', '99493', '99494', '99510',
    'G0155', 'G0176', 'G0177', 'G0409', 'G0463', 'G0512', 'H0002', 'H0004', 'H0031', 'H0034', 'H0036', 'H0037', 'H0039', 
    'H0040', 'H2000', 'H2010', 'H2011', 'H2013', 'H2014', 'H2015', 'H2016', 'H2017', 'H2018', 'H2019', 'H2020', 'T1015',
    
    # Outpatient Visit CPT codes
    '90791', '90792', '90832', '90833', '90834', '90836', '90837', '90838', '90839', '90840', 
    '90845', '90847', '90849', '90853', '90875', '90876', '99221', '99222', '99223', 
    '99231', '99232', '99233', '99238', '99239', '99252', '99253', '99254', '99255',

    # Partial Hospitalization or Intensive Outpatient HCPCS codes
    'G0410', 'G0411', 'H0035', 'H2001', 'H2012', 'S0201', 'S9480', 'S9484', 'S9485',

    # Psychiatric Collaborative Care Management CPT and HCPCS codes
    '99492', '99493', '99494', 'G0512',

    # Telephone Visits CPT codes
    '98966', '98967', '98968', '99441', '99442', '99443',

    # Transitional Care Management CPT codes
    '99495', '99496'
]

# Example of usage in PySpark
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CombinedCodes").getOrCreate()

# Display the combined codes
print(combined_codes)


bh_dx = [1]

# Combine all relevant codes into one list for checking with POS 53
all_relevant_codes = (
    partial_hospitalization_hcpcs_codes + 
    psychiatric_collaborative_care_management_cpt_codes + 
    psychiatric_collaborative_care_management_hcpcs_codes + 
    telephone_visit_cpt_codes + 
    transitional_care_management_cpt_codes
)

# Create the 'bh_visit4' indicator based on the specified conditions
outpat2018 = outpat2018.withColumn(
    'bh_visit4',
    when(
        (col('LINE_PRCDR_CD').isin(partial_hospitalization_hcpcs_codes)) |
        (col('LINE_PRCDR_CD').isin(psychiatric_collaborative_care_management_cpt_codes + psychiatric_collaborative_care_management_hcpcs_codes)) |
        (col('LINE_PRCDR_CD').isin(telephone_visit_cpt_codes)) |
        (col('LINE_PRCDR_CD').isin(transitional_care_management_cpt_codes)) |
        ((col('POS_CD').isin(community_mental_health_pos_codes)  & (col('LINE_PRCDR_CD').isin(combined_codes) | col('bh_dx').isin(bh_dx)))),
        1
    ).otherwise(0)
)

#& col('bh_dx').isin(bh_dx)

# Show the results to verify correct application
outpat2018.show()

# COMMAND ----------

# Perform a value count on the 'bh_provider' field
value_counts = outpat2018.groupBy('bh_visit4').count()

# Show the results of the value count
value_counts.show()

#7,176,689

# COMMAND ----------

outpat2018 = outpat2018.withColumn(
    'bh_visit',
    when(
        (col('bh_visit1') == 1) | 
        (col('bh_visit2') == 1) | 
        (col('bh_visit3') == 1) | 
        (col('bh_visit4') == 1) ,
        1
    ).otherwise(0)
)

# Show the results to verify correct application
outpat2018.show()

# COMMAND ----------

# Perform a value count on the 'bh_provider' field
value_counts = outpat2018.groupBy('bh_visit').count()

# Show the results of the value count
value_counts.show()

#35950132

# COMMAND ----------

# MAGIC %md
# MAGIC clean up

# COMMAND ----------

# Filter out rows where 'bh_visit' is 0
df = outpat2018.filter(col('bh_visit') != 0)

# Select distinct beneID, state, SRVC_BGN_DDT, bh_visit
result_df = df.select('beneID', 'state', 'SRVC_BGN_DT', 'bh_visit').distinct()

# Show the result
print(result_df.count())
result_df.show()

# COMMAND ----------

#result_df.write.saveAsTable("dua_058828_spa240.paper_4_fum30_bh_outpatient_visits_12_months", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC FINAL VISITS in 30 DAY WINDOW

# COMMAND ----------

index_visits = spark.table("dua_058828_spa240.paper_4_fum30_denominator_ed_visits_12_months_new3")
print(index_visits.count())
index_visits.show()

# COMMAND ----------

#outcome_visits = result_df
outcome_visits = spark.table("dua_058828_spa240.paper_4_fum30_bh_outpatient_visits_12_months")
outcome_visits.show()

# COMMAND ----------

# Convert dates to appropriate format if necessary
index_visits = index_visits.withColumn("max_EndDate", col("max_EndDate").cast("date"))
outcome_visits = outcome_visits.withColumn("SRVC_BGN_DT", col("SRVC_BGN_DT").cast("date"))

# COMMAND ----------

# Perform a self-join on beneID and state
joined_df = index_visits.alias("iv").join(
    outcome_visits.alias("ov"),
    (col("iv.beneID") == col("ov.beneID")) & (col("iv.state") == col("ov.state")),
    "inner"
)

print(joined_df.count())
#joined_df.show(200)

# COMMAND ----------

from pyspark.sql.functions import col, datediff, expr

# Filter rows where min_StartDate is 30 days or less after max_EndDate and dates are not equal
filtered_df = joined_df.filter(
    (datediff(col("ov.SRVC_BGN_DT"), col("iv.max_EndDate")) <= 30) &
    (col("iv.max_EndDate") != col("ov.SRVC_BGN_DT"))
)

# COMMAND ----------

filtered_df.show()

# COMMAND ----------

from pyspark.sql.functions import lit, datediff, col

# Calculate the time difference between min_StartDate and max_EndDate
result_df = filtered_df.select(
    col("iv.beneID"),
    col("iv.state"),
    # Assuming 'episode' and 'days' are columns from index_visits
    col("iv.episode"),   
    col("iv.max_EndDate"),
    col("ov.SRVC_BGN_DT"),
    # Calculate time_to_readmit
    datediff(col("ov.SRVC_BGN_DT"), col("iv.max_EndDate")).alias("time_to_visit")
).withColumn("bh_visit", lit(1))

# Filter rows where time_to_readmit is 30 days or less and not 0
result_df = result_df.filter(
    (col("time_to_visit") > 0) & (col("time_to_visit") <= 30)
)

# # Select the final set of columns
# result_df = result_df.select(
#     col("beneID"),
#     col("state"),
#     col("episode"),
#     col("time_to_visit"),
#     col("bh_visit"),
# )

result_df.show(100)

# COMMAND ----------

# MAGIC %md
# MAGIC END SENSITIVITY ANALYSIS

# COMMAND ----------

print(result_df.count())
result_df = result_df.select('beneID', 'state', 'episode', 'bh_visit').distinct()
print(result_df.count())

# COMMAND ----------

# Perform a left join between index_visits and result_df based on beneID, state, and episode
final_df = index_visits.join(result_df, on=["beneID","state","episode"], how="left").fillna(0)

print(index_visits.count())
print(final_df.count())
final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC START SENSITIVITY

# COMMAND ----------

filtered_visits = final_df.filter(F.col("bh_visit") == 1)


# Assuming your DataFrame is named df
filtered_visits = final_df.withColumn(
    "semester", 
    F.when(F.month(F.col("max_EndDate")).between(1, 6), 1).otherwise(2)
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
# MAGIC END SENSITIVITY

# COMMAND ----------



# COMMAND ----------

full = final_df

# COMMAND ----------

outcome1_value = full.groupBy("bh_visit").count()
outcome1_value.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = full.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = full.groupBy("bh_visit") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

print(full.count())

# COMMAND ----------

full.show()

# COMMAND ----------

full.write.saveAsTable("dua_058828_spa240.paper_4_fum30_outcome_12_months_new2", mode='overwrite')

# COMMAND ----------

