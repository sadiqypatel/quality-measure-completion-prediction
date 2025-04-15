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
# MAGIC Denominator

# COMMAND ----------

outpat2017 = spark.table("dua_058828_spa240.paper_4_otherservices2017_12_months")
outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")

inpat2017 = spark.table("dua_058828_spa240.paper_4_inpatient2017_12_months_new")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

# COMMAND ----------

# inpat2019.show()

# COMMAND ----------

outpat2019 = outpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2017 = outpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")

inpat2019 = inpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2017 = inpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")

inpat2019 = inpat2019.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2017 = inpat2017.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

# COMMAND ----------

test = inpat2018.union(outpat2018)
test.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Identify index event

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# List of ICD-10 codes for lower back pain
lower_back_pain_codes = [
    "M4726", "M4727", "M4728", "M47816", "M47817", "M47818", "M47896", "M47897", "M47898",
    "M48061", "M4807", "M4808", "M5116", "M5117", "M5126", "M5127", "M5136", "M5137",
    "M5186", "M5187", "M532X6", "M532X7", "M532X8", "M533", "M5386", "M5387", "M5388",
    "M5416", "M5417", "M5418", "M5430", "M5431", "M5432", "M5440", "M5441", "M5442", "M545",
    "M5450", "M5451", "M5459", "M5489", "M549", "M9903", "M9904", "M9923", "M9933", "M9943",
    "M9953", "M9963", "M9973", "M9983", "M9984", "S33100A", "S33100D", "S33100S", "S33110A",
    "S33110D", "S33110S", "S33120A", "S33120D", "S33120S", "S33130A", "S33130D", "S33130S",
    "S33140A", "S33140D", "S33140S", "S335XXA", "S336XXA", "S338XXA", "S339XXA", "S39002A",
    "S39002D", "S39002S", "S39012A", "S39012D", "S39012S", "S39092A", "S39092D", "S39092S",
    "S3982XA", "S3982XD", "S3982XS", "S3992XA", "S3992XD", "S3992XS"
]

# Convert the list of codes to a set
lower_back_pain_codes_set = set(lower_back_pain_codes)

# Filter the DataFrame to keep only rows where DGNS_CD_1 is in the list of lower back pain codes
all_filtered = test.filter(col("DGNS_CD_1").isin(lower_back_pain_codes_set))

# Show the result
print(all_filtered.count())
all_filtered.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_date
from pyspark.sql.window import Window

# Convert SRVC_BGN_DT to date format
all_filtered = all_filtered.withColumn("SRVC_BGN_DT", to_date(col("SRVC_BGN_DT"), "yyyy-MM-dd"))

# Filter for the date range
filtered_df = all_filtered.filter(
    (col("SRVC_BGN_DT") >= "2018-01-01") & (col("SRVC_BGN_DT") <= "2018-12-03")
)

# Define window specification
window_spec = Window.partitionBy("beneID", "state").orderBy("SRVC_BGN_DT")

# Apply window function to get row numbers
filtered_df = filtered_df.withColumn("row_number", row_number().over(window_spec))

# Filter to keep only the first row for each beneID, state combination
result_df = filtered_df.filter(col("row_number") == 1).drop("row_number")

# Show the result
print(result_df.count())
result_df.show()

# COMMAND ----------

print(result_df.count())
print(all_filtered.count())

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_demo_file_01_12_months")
denom = denom.select("beneID","state","birthday")
print(denom.count())

# COMMAND ----------

from pyspark.sql.functions import col, year, lit

# Define the year of the measurement period, e.g., 2024
measurement_year = 2018

# Calculate the age in the measurement year
denom = denom.withColumn('age', measurement_year - year(col('birthday')))

# Filter rows to keep only those who are between 18 and 74 years old
denom = denom.filter((col('age') >= 18) & (col('age') < 75))

# Show the result
print(denom.count())
denom.show()

# COMMAND ----------

denom = denom.join(result_df, how="inner", on=["beneID","state"])
print(denom.count())

# COMMAND ----------

# Count the number of unique rows based on beneID and state combination
unique_rows_count = denom.select("beneID", "state").distinct().count()

# Print the result
print(f"Number of unique rows based on beneID and state combination: {unique_rows_count}")

# COMMAND ----------

denom.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Exclusion 1

# COMMAND ----------

from pyspark.sql.functions import col, expr, lit

# Add start_date and end_date columns
denom1 = denom.withColumn("start_date", lit('2017-01-01'))
denom1 = denom1.withColumn("end_date", expr("date_add(SRVC_BGN_DT, 28)"))

# Show the result
denom1.show()

# COMMAND ----------

exclude1 = inpat2018.union(outpat2018).union(inpat2017).union(outpat2017)
print(exclude1.count())
print(test.count())

# COMMAND ----------

cancer_codes = [
    "C000", "C001", "C002", "C003", "C004", "C005", "C006", "C008", "C009", 
    "C010", "C020", "C021", "C022", "C023", "C024", "C028", "C029", "C030", 
    "C031", "C039", "C040", "C041", "C048", "C049", "C050", "C051", "C052", 
    "C058", "C059", "C060", "C061", "C062", "C0680", "C0689", "C069", "C070", 
    "C080", "C081", "C089", "C090", "C091", "C098", "C099", "C100", "C101", 
    "C102", "C103", "C104", "C108", "C109", "C110", "C111", "C112", "C113", 
    "C118", "C119", "C120", "C130", "C131", "C132", "C138", "C139", "C140", 
    "C142", "C148", "C153", "C154", "C155", "C158", "C159", "C160", "C161", 
    "C162", "C163", "C164", "C165", "C166", "C168", "C169", "C170", "C171", 
    "C172", "C178", "C179", "C180", "C181", "C182", "C183", "C184", "C185", 
    "C186", "C187", "C188", "C189", "C190", "C200", "C210", "C920", "C9290", 
    "C9392", "C93Z0", "C93Z1", "C93Z2", "C9400", "C9401", "C9402", "C9420", 
    "C9421", "C9422", "C9430", "C9431", "C9432", "C9440", "C9441", "C9442", 
    "C9460", "C9480", "C9481", "C9482", "C9500", "C9501", "C9502", "C9510", 
    "C9511", "C9512", "C9590", "C9591", "C9592", "C960", "C962", "C9620", 
    "C9621", "C9622", "C9629", "C964", "C965", "C966", "C969", "C96A", "C96Z"
]

other_neoplasm_codes = [
    "D0000", "D0001", "D0008", "D001", "D002", "D010", "D011", "D012", 
    "D013", "D0140", "D0149", "D015", "D017", "D019", "D020", "D021", 
    "D0220", "D0221", "D0222", "D023", "D024", "D030", "D0310", "D0311", 
    "D03111", "D03112", "D03121", "D03122", "D0320", "D0321", "D0322", 
    "D0330", "D0339", "D034", "D0351", "D0352", "D0359", "D0360", "D0361", 
    "D0362", "D0370", "D0371", "D0372", "D038", "D039", "D040", "D0410", 
    "D0411", "D04111", "D04112", "D0412", "D04121", "D04122", "D0420", 
    "D0421", "D0422", "D0430", "D0439", "D044", "D045", "D0460", "D0461", 
    "D0462", "D0470", "D0471", "D0472", "D048", "D049", "D0500", "D0501", 
    "D0502", "D0510", "D0511", "D0512", "D0580", "D0581", "D0582", "D0590", 
    "D0591", "D0592", "D060", "D061", "D067", "D069", "D070", "D071", 
    "D072", "D0730", "D0739", "D074", "D075", "D0760", "D0761", "D0769", 
    "D090", "D0910", "D0919", "D0920", "D0921", "D0922", "D093", "D098", 
    "D099", "D3701", "D3702", "D37030", "D37031", "D37032", "D37039", 
    "D3704", "D3705", "D3709", "D371", "D376", "D378", "D379", "D380", 
    "D386", "D390", "D3910", "D3911", "D3912", "D392", "D398", "D399", 
    "D400", "D4010", "D4011", "D4012", "D408", "D409", "D4100", "D4101", 
    "D4102", "D4110", "D4111", "D4112", "D413", "D414", "D418", "D419", 
    "D420", "D421", "D429", "D430", "D434", "D438", "D439", "D440", 
    "D4410", "D4411", "D4412", "D442", "D443", "D447", "D449", "D450", 
    "D461", "D4620", "D4621", "D4622", "D464", "D469", "D46A", "D46B", 
    "D46C", "D46Z", "D470", "D4701", "D4702", "D4709", "D471", "D472", 
    "D474", "D479", "D47Z1", "D47Z2", "D47Z9", "D480", "D481", "D482", 
    "D483", "D484", "D485", "D4860", "D4861", "D4862", "D487", "D489", 
    "D490", "D491", "D492", "D493", "D494", "D495", "D49511", "D49512", 
    "D49519", "D4959", "D496", "D497", "D4981", "D4989", "D499"
]

history_of_malignant_neoplasm_codes = [
    "Z8500", "Z8501", "Z85020"
]

other_malignant_neoplasm_of_skin_codes = [
    "C4400", "C4401", "C4402"
]

hiv_codes = ["B20", "Z21"]

kidney_transplant_history_codes = ["Z940"]

kidney_transplant_cpt_codes = ["50360", "50365", "50380"]

kidney_transplant_hcpcs_codes = ["S2065"]

kidney_transplant_icd10pcs_codes = [
    "0TY00Z0", "0TY00Z1", "0TY00Z2", "0TY10Z0", "0TY10Z1", "0TY10Z2"
]

other_organ_transplant_cpt_codes = ["32850", "32851", "32852", "32853", "32854", "32855", "32856"]

osteoporosis_hcpcs_codes = ["J0897", "J1740", "J3110", "J3111", "J3489"]

lumbar_surgery_cpt_codes = [
    "22114", "22207", "22214", "22224", "22511", "22512", "22514", "22515",
    "22533", "22534", "22558", "22612", "22630", "22632", "22633", "22634",
    "22857", "22860", "22862", "22865", "22867", "22868", "22869", "22870",
    "62287", "62380", "63005", "63012", "63017", "63030", "63035", "63042",
    "63044", "63047", "63048", "63052", "63053", "63056", "63057", "63087",
    "63088", "63090", "63091", "63102", "63103", "63170", "63200", "63252",
    "63267", "63272", "63277", "63282", "63287"
]

lumbar_surgery_hcpcs_codes = ["S2348", "S2350"]

lumbar_surgery_icd10pcs_codes = [
    "005Y0ZZ", "008Y0ZZ", "009Y00Z", "00BY0ZX", "00CY0ZZ", "00NY0ZZ",
    "00QY0ZZ", "00SY0ZZ", "0Q500ZZ", "0Q800ZZ", "0QH004Z", "0QR03KZ",
    "0QU007Z", "0SG037J", "0SW4XKZ"
]

spondylopathy_codes = [
    "M450", "M453", "M454", "M455", "M456", "M457", "M458", "M459",
    "M4810", "M4813", "M4814", "M4815", "M4816", "M4817", "M4818",
    "M4819"
]

# COMMAND ----------

exclude1_codes = (cancer_codes + other_neoplasm_codes + history_of_malignant_neoplasm_codes + other_malignant_neoplasm_of_skin_codes + hiv_codes + kidney_transplant_history_codes + kidney_transplant_cpt_codes + kidney_transplant_hcpcs_codes + kidney_transplant_icd10pcs_codes + other_organ_transplant_cpt_codes + osteoporosis_hcpcs_codes + lumbar_surgery_cpt_codes + lumbar_surgery_hcpcs_codes + lumbar_surgery_icd10pcs_codes + spondylopathy_codes)

print(exclude1_codes)

# COMMAND ----------

print(exclude1.count())

exclude1 = exclude1.filter(
    col("DGNS_CD_1").isin(exclude1_codes) |
    col("LINE_PRCDR_CD").isin(exclude1_codes)
)

print(exclude1.count())
exclude1.show()

# COMMAND ----------

denom_exclude1 = denom1.select("beneID","state","start_date", "end_date")
# Count the number of unique rows based on beneID and state combination
unique_rows_count = denom_exclude1.select("beneID", "state").distinct().count()

# Print the result
print(f"Number of unique rows based on beneID and state combination: {unique_rows_count}")
print(denom_exclude1.count())
denom_exclude1.show()

# COMMAND ----------

print(exclude1.count())
exclude1 = exclude1.join(denom_exclude1, on=["beneID","state"], how="inner")
print(exclude1.count())
exclude1.show()

# COMMAND ----------

print(exclude1.count())
filtered_exclude1 = exclude1.filter(
    (col("SRVC_BGN_DT") >= col("start_date")) & (col("SRVC_BGN_DT") <= col("end_date"))
)
print(filtered_exclude1.count())
filtered_exclude1.show()

# COMMAND ----------

# Select the required columns and add the new column with a value of 1
filtered_exclude1 = filtered_exclude1.select(
    col("beneID"),
    col("state"),
    lit(1).alias("exclude1")
)

# Show the result
print(filtered_exclude1.count())
filtered_exclude1 = filtered_exclude1.distinct()
print(filtered_exclude1.count())

filtered_exclude1.show()

# COMMAND ----------

# Perform the left join
print(denom.count())
denom = denom.join(filtered_exclude1, on=["beneID", "state"], how="left")
print(denom.count())

# Fill missing values in the 'exclude1' column with 0
denom = denom.fillna({"exclude1": 0})

# Show the result
denom.show()

# COMMAND ----------

# Perform value counts for 'exclude1'
value_counts = denom.groupBy("exclude1").count()

# Show the result
value_counts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC EXCLUSION 2

# COMMAND ----------

from pyspark.sql.functions import col, expr, lit, date_sub, date_add

# Add start_date and end_date columns
denom2 = denom.withColumn("start_date", expr("date_sub(SRVC_BGN_DT, 365)"))
denom2 = denom2.withColumn("end_date", expr("date_add(SRVC_BGN_DT, 28)"))

# Show the result
denom2.show()

# COMMAND ----------

exclude2 = inpat2018.union(outpat2018).union(inpat2017).union(outpat2017)
print(exclude2.count())

# COMMAND ----------

neurologic_impairment_codes = ["G834", "K592", "M48062", "R262", "R292"]

spinal_infection_codes = ["A1781", "G061", "M4625", "M4626", "M4627", "M4628", "M4635", "M4636", "M4637", "M4638", "M4646", "M4647", "M4648"]

intravenous_drug_abuse_codes = ["F1110", "F1111", "F11120", "F11121", "F11122", "F11129", "F1113", "F1114", "F11150", "F11151", "F11159", "F11181", "F11182", "F11188", "F1119", "F1120", "F1121", "F11220", "F11221", "F11222", "F11229", "F1123", "F1124", "F11250", "F11251", "F11259", "F11281", "F11282", "F11288", "F1129", "F1310", "F1311", "F13120", "F13121", "F13129", "F13130", "F13131", "F13132", "F13139", "F1314", "F13150", "F13151", "F13159", "F13180", "F13181", "F13182", "F13188", "F1319", "F1320", "F1321", "F13220", "F13221", "F13229", "F13230", "F13231", "F13232", "F13239", "F1324", "F13250", "F13251", "F13259", "F13260", "F13261", "F13262", "F13269", "F13280", "F13281", "F13282", "F13288", "F1329", "F1410", "F1411", "F14120", "F14121", "F14122", "F14129", "F1413", "F1414", "F14150", "F14151", "F14159", "F14180", "F14181", "F14182", "F14188", "F1419", "F1420", "F1421", "F14220", "F14221", "F14222", "F14229", "F1423", "F1424", "F14250", "F14251", "F14259", "F14280", "F14281", "F14282", "F14288", "F1429", "F1510", "F1511", "F15120", "F15121", "F15122", "F15129", "F1513", "F1514", "F15150", "F15151", "F15159", "F15180", "F15181", "F15182", "F15188", "F1519", "F1520", "F1521", "F15220", "F15221", "F15222", "F15229", "F1523", "F1524", "F15250", "F15251", "F15259", "F15280", "F15281", "F15282", "F15288", "F1529"]

# COMMAND ----------

exclude2_codes = (neurologic_impairment_codes + spinal_infection_codes + intravenous_drug_abuse_codes)

# COMMAND ----------

print(exclude2.count())

exclude2 = exclude2.filter(
    col("DGNS_CD_1").isin(exclude2_codes) |
    col("LINE_PRCDR_CD").isin(exclude2_codes)
)

print(exclude2.count())
exclude2.show()

# COMMAND ----------

denom_exclude2 = denom2.select("beneID","state","start_date", "end_date")
# Count the number of unique rows based on beneID and state combination
unique_rows_count = denom_exclude2.select("beneID", "state").distinct().count()

# Print the result
print(f"Number of unique rows based on beneID and state combination: {unique_rows_count}")
print(denom_exclude2.count())
denom_exclude2.show()

# COMMAND ----------

print(exclude2.count())
exclude2 = exclude2.join(denom_exclude2, on=["beneID","state"], how="inner")
print(exclude2.count())
exclude2.show()

# COMMAND ----------

print(exclude2.count())
filtered_exclude2 = exclude2.filter(
    (col("SRVC_BGN_DT") >= col("start_date")) & (col("SRVC_BGN_DT") <= col("end_date"))
)
print(filtered_exclude2.count())
filtered_exclude2.show()

# COMMAND ----------

# Select the required columns and add the new column with a value of 1
filtered_exclude2 = filtered_exclude2.select(
    col("beneID"),
    col("state"),
    lit(1).alias("exclude2")
)

# Show the result
print(filtered_exclude2.count())
filtered_exclude2 = filtered_exclude2.distinct()
print(filtered_exclude2.count())

filtered_exclude2.show()

# COMMAND ----------

# Perform the left join
print(denom.count())
denom = denom.join(filtered_exclude2, on=["beneID", "state"], how="left")
print(denom.count())

# Fill missing values in the 'exclude1' column with 0
denom = denom.fillna({"exclude2": 0})

# Show the result
denom.show()

# COMMAND ----------

# Perform value counts for 'exclude1'
value_counts1 = denom.groupBy("exclude1").count()
value_counts2 = denom.groupBy("exclude2").count()

# Show the result
value_counts1.show()
value_counts2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC EXCLUSION 3

# COMMAND ----------

from pyspark.sql.functions import col, expr, lit, date_sub, date_add

# Add start_date and end_date columns
denom3 = denom.withColumn("start_date", expr("date_sub(SRVC_BGN_DT, 90)"))
denom3 = denom3.withColumn("end_date", expr("date_add(SRVC_BGN_DT, 28)"))

# Show the result
denom3.show()

# COMMAND ----------

exclude3 = inpat2018.union(outpat2018).union(inpat2017).union(outpat2017)
print(exclude3.count())

# COMMAND ----------

recent_trauma_codes = ["G8911"]

fragility_fracture_codes = ["M4840XA", "M4840XD", "M4840XG", "M4840XS", "M4841XA", "M4841XD", "M4841XG", "M4841XS", "M4842XA", "M4842XD", "M4842XG", "M4842XS", "M4843XA", "M4843XD", "M4843XG", "M4843XS", "M4844XA", "M4844XD", "M4844XG", "M4844XS", "M4845XA", "M4845XD", "M4845XG", "M4845XS", "M4846XA", "M4846XD", "M4846XG", "M4846XS", "M4847XA", "M4847XD", "M4847XG", "M4847XS", "M4848XA", "M4848XD", "M4848XG", "M4848XS", "M8008XA", "M8008XD", "M8008XG", "M8008XK", "M8008XP", "M8008XS", "M8088XA", "M8088XD", "M8088XG", "M8088XK", "M8088XP", "M8088XS", "M84359A", "M84359D", "M84359G", "M84359K", "M84359P", "M84359S", "M9701XA", "M9701XD", "M9701XS", "M9702XA", "M9702XD", "M9702XS"]

# COMMAND ----------

exclude3_codes = (recent_trauma_codes + fragility_fracture_codes)

# COMMAND ----------

print(exclude3.count())

exclude3 = exclude3.filter(
    col("DGNS_CD_1").isin(exclude3_codes) |
    col("LINE_PRCDR_CD").isin(exclude3_codes) |
    col("DGNS_CD_1").rlike("^S")
)

print(exclude3.count())
exclude3.show()

# COMMAND ----------

denom_exclude3 = denom3.select("beneID","state","start_date", "end_date")
# Count the number of unique rows based on beneID and state combination
unique_rows_count = denom_exclude3.select("beneID", "state").distinct().count()

# Print the result
print(f"Number of unique rows based on beneID and state combination: {unique_rows_count}")
print(denom_exclude3.count())
denom_exclude3.show()

# COMMAND ----------

print(exclude3.count())
exclude3 = exclude3.join(denom_exclude3, on=["beneID","state"], how="inner")
print(exclude3.count())
exclude3.show()

# COMMAND ----------

print(exclude3.count())
filtered_exclude3 = exclude3.filter(
    (col("SRVC_BGN_DT") >= col("start_date")) & (col("SRVC_BGN_DT") <= col("end_date"))
)
print(filtered_exclude3.count())
filtered_exclude3.show()

# COMMAND ----------

# Select the required columns and add the new column with a value of 1
filtered_exclude3 = filtered_exclude3.select(
    col("beneID"),
    col("state"),
    lit(1).alias("exclude3")
)

# Show the result
print(filtered_exclude3.count())
filtered_exclude3 = filtered_exclude3.distinct()
print(filtered_exclude3.count())

filtered_exclude3.show()

# COMMAND ----------

# Perform the left join
print(denom.count())
denom = denom.join(filtered_exclude3, on=["beneID", "state"], how="left")
print(denom.count())

# Fill missing values in the 'exclude1' column with 0
denom = denom.fillna({"exclude3": 0})

# Show the result
denom.show()

# COMMAND ----------

# Perform value counts for 'exclude1'
value_counts1 = denom.groupBy("exclude1").count()
value_counts2 = denom.groupBy("exclude2").count()
value_counts3 = denom.groupBy("exclude3").count()

# Show the result
value_counts1.show()
value_counts2.show()
value_counts3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Exclusion 4 - medication

# COMMAND ----------

pharm2018 = spark.table("dua_058828_spa240.paper_4_pharm2017_12_months")
print(pharm2018.count())
pharm2018 = pharm2018.select("beneID", "state", "RX_FILL_DT", "NDC")

pharm2019 = spark.table("dua_058828_spa240.paper_4_pharm2018_12_months")
print(pharm2019.count())
pharm2019 = pharm2019.select("beneID", "state", "RX_FILL_DT", "NDC")

pharm = pharm2019.union(pharm2018)
pharm.show()

# COMMAND ----------

pharm.registerTempTable("connections")

pharm = spark.sql('''

SELECT DISTINCT beneID, state, NDC

FROM connections;
''')

pharm = pharm.filter(col("NDC").isNotNull())
pharm.show(200)

# COMMAND ----------

import pandas as pd
rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
#betos_df.head()
rx_df = spark.createDataFrame(rx_df)
rx_df = rx_df[rx_df['rxDcDrugName'].isin(["clomiphene"])]
rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
rx_df = rx_df.select("ndc", "rxDcDrugName").distinct()
print(rx_df.count())
rx_df.show(1000)

# COMMAND ----------

from pyspark.sql.functions import col, substring

exclude2 = pharm.join(
    rx_df,
    on='ndc',
    how='inner'
)

print(exclude2.count())

# COMMAND ----------

exclude2.show()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, expr, lit

# Add start_date and end_date columns
denom4 = denom.withColumn("start_date", lit('2017-01-01'))
denom4 = denom4.withColumn("end_date", expr("date_add(SRVC_BGN_DT, 28)"))

# Show the result
denom4.show()

# COMMAND ----------

denom_exclude4 = denom4.select("beneID","state","start_date", "end_date")
# Count the number of unique rows based on beneID and state combination
unique_rows_count = denom_exclude4.select("beneID", "state").distinct().count()

# Print the result
print(f"Number of unique rows based on beneID and state combination: {unique_rows_count}")
print(denom_exclude4.count())
denom_exclude4.show()

# COMMAND ----------

print(exclude4.count())
exclude4 = exclude4.join(denom_exclude4, on=["beneID","state"], how="inner")
print(exclude4.count())
exclude4.show()

# COMMAND ----------

print(exclude4.count())
filtered_exclude4 = exclude4.filter(
    (col("RX_FILL_DT") >= col("start_date")) & (col("RX_FILL_DT") <= col("end_date"))
)
print(filtered_exclude4.count())
filtered_exclude4.show()

# COMMAND ----------

# Select the required columns and add the new column with a value of 1
filtered_exclude4 = filtered_exclude4.select(
    col("beneID"),
    col("state"),
    lit(1).alias("exclude4")
)

# Show the result
print(filtered_exclude4.count())
filtered_exclude4 = filtered_exclude4.distinct()
print(filtered_exclude4.count())

filtered_exclude4.show()

# COMMAND ----------

# Perform the left join
print(denom.count())
denom = denom.join(filtered_exclude4, on=["beneID", "state"], how="left")
print(denom.count())

# Fill missing values in the 'exclude1' column with 0
denom = denom.fillna({"exclude4": 0})

# Show the result
denom.show()

# COMMAND ----------

# Perform value counts for 'exclude1'
# value_counts1 = denom.groupBy("exclude1").count()
# value_counts2 = denom.groupBy("exclude2").count()
# value_counts3 = denom.groupBy("exclude3").count()
value_counts4 = denom.groupBy("exclude4").count()

# Show the result
# value_counts1.show()
# value_counts2.show()
# value_counts3.show()
value_counts4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Exclusion 5 - also meds

# COMMAND ----------

pharm2018 = spark.table("dua_058828_spa240.paper_4_pharm2017_12_months")
print(pharm2018.count())
pharm2018 = pharm2018.select("beneID", "state", "RX_FILL_DT", "NDC")

pharm2019 = spark.table("dua_058828_spa240.paper_4_pharm2018_12_months")
print(pharm2019.count())
pharm2019 = pharm2019.select("beneID", "state", "RX_FILL_DT", "NDC")

pharm = pharm2019.union(pharm2018)
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

import pandas as pd
rx_df = pd.read_csv("/Volumes/analytics/dua_058828/files/rxDcCrossWalk.csv")
#betos_df.head()
rx_df = spark.createDataFrame(rx_df)
rx_df = rx_df[rx_df['rxDcDrugCode'].isin(["R01152640101001", "R00460410101002", "R00460410101000", "R00776550101001", "R00730560101000", "R00460410101001", "R00107590101002", "R00069020101004", "R00054920101015", "R00054920101005", "R00069020101003", "R00069020101001", "R00032640101007", "R00054920203000", "R00054920101007", "R00086400101000", "R00032640101000", "R00015140101002", "R00015140101001", "R00015140101000", "R00086380101000", "R00107590101000", "R00086380101005", "R00776550101000", "R00730560101001", "R00730560101002", "R00776550101000", "R01152640101000", "R00054920201000", "R00069020101002", "R00107590101001", "R00086380101002", "R00054920201002", "R00032640101005", "R00107590101005", "R00015140101004", "R00054920203002", "R00054920203001", "R00032640101004", "R00032640101002", "R00054920101004", "R00054920101013", "R00054920101011", "R00054920101009", "R00054920101010", "R00032640101006", "R00054920203003", "R00054920101003", "R00054920201008", "R00107590101003", "R00086400101001"])]
rx_df = rx_df.withColumnRenamed("ndcNum", "ndc")
rx_df = rx_df.select("ndc", "rxDcDrugCode").distinct()
rx_df.show(100)

# COMMAND ----------

from pyspark.sql.functions import col, substring

exclude5 = pharm.join(
    rx_df,
    on='ndc',
    how='inner'
)

print(exclude5.count())

# COMMAND ----------

from pyspark.sql.functions import col, expr, lit

# Add start_date and end_date columns
denom5 = denom.withColumn("start_date", expr("date_sub(SRVC_BGN_DT, 365)"))
denom5 = denom5.withColumn("end_date", expr("date_add(SRVC_BGN_DT, 28)"))

# Show the result
denom5.show()

# COMMAND ----------

denom_exclude5 = denom5.select("beneID","state","start_date", "end_date")
# Count the number of unique rows based on beneID and state combination
unique_rows_count = denom_exclude5.select("beneID", "state").distinct().count()

# Print the result
print(f"Number of unique rows based on beneID and state combination: {unique_rows_count}")
print(denom_exclude5.count())
denom_exclude5.show()

# COMMAND ----------

print(exclude5.count())
exclude5 = exclude5.join(denom_exclude5, on=["beneID","state"], how="inner")
print(exclude5.count())
exclude5.show()

# COMMAND ----------

print(exclude5.count())
filtered_exclude5 = exclude5.filter(
    (col("RX_FILL_DT") >= col("start_date")) & (col("RX_FILL_DT") <= col("end_date"))
)
print(denom_exclude5.count())
denom_exclude5.show()

# COMMAND ----------

# Select the required columns and add the new column with a value of 1
filtered_exclude5 = filtered_exclude5.select(
    col("beneID"),
    col("state"),
    lit(1).alias("exclude5")
)

# Show the result
print(filtered_exclude5.count())
filtered_exclude5 = filtered_exclude5.distinct()
print(filtered_exclude5.count())

filtered_exclude5.show()

# COMMAND ----------

# Perform the left join
print(denom.count())
denom = denom.join(filtered_exclude5, on=["beneID", "state"], how="left")
print(denom.count())

# Fill missing values in the 'exclude1' column with 0
denom = denom.fillna({"exclude5": 0})

# Show the result
denom.show()

# COMMAND ----------

# Perform value counts for 'exclude1'
value_counts5 = denom.groupBy("exclude5").count()


# Show the result
value_counts5.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Exclusion 6 - death
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Exclude 7 - prior lower back pain 

# COMMAND ----------

from pyspark.sql.functions import col, expr, lit

# Add start_date and end_date columns
denom7 = denom.withColumn("start_date", expr("date_sub(SRVC_BGN_DT, 180)"))
denom7 = denom7.withColumn("end_date", expr("date_sub(SRVC_BGN_DT, 1)"))

# Show the result
denom7.show()

# COMMAND ----------

exclude7 = inpat2018.union(outpat2018).union(inpat2017).union(outpat2017)
#print(exclude7.count())

# COMMAND ----------

exclude7_codes = [
    "M4726", "M4727", "M4728", "M47816", "M47817", "M47818", "M47896", "M47897", "M47898",
    "M48061", "M4807", "M4808", "M5116", "M5117", "M5126", "M5127", "M5136", "M5137",
    "M5186", "M5187", "M532X6", "M532X7", "M532X8", "M533", "M5386", "M5387", "M5388",
    "M5416", "M5417", "M5418", "M5430", "M5431", "M5432", "M5440", "M5441", "M5442", "M545",
    "M5450", "M5451", "M5459", "M5489", "M549", "M9903", "M9904", "M9923", "M9933", "M9943",
    "M9953", "M9963", "M9973", "M9983", "M9984", "S33100A", "S33100D", "S33100S", "S33110A",
    "S33110D", "S33110S", "S33120A", "S33120D", "S33120S", "S33130A", "S33130D", "S33130S",
    "S33140A", "S33140D", "S33140S", "S335XXA", "S336XXA", "S338XXA", "S339XXA", "S39002A",
    "S39002D", "S39002S", "S39012A", "S39012D", "S39012S", "S39092A", "S39092D", "S39092S",
    "S3982XA", "S3982XD", "S3982XS", "S3992XA", "S3992XD", "S3992XS"
]

# COMMAND ----------

print(exclude7.count())

exclude7 = exclude7.filter(
    col("DGNS_CD_1").isin(exclude7_codes) |
    col("LINE_PRCDR_CD").isin(exclude7_codes)
)

print(exclude7.count())
exclude7.show()

# COMMAND ----------

denom_exclude7 = denom7.select("beneID","state","start_date", "end_date")
# Count the number of unique rows based on beneID and state combination
unique_rows_count = denom_exclude7.select("beneID", "state").distinct().count()

# Print the result
print(f"Number of unique rows based on beneID and state combination: {unique_rows_count}")
print(denom_exclude7.count())
denom_exclude7.show()

# COMMAND ----------

print(exclude7.count())
exclude7 = exclude7.join(denom_exclude7, on=["beneID","state"], how="inner")
print(exclude7.count())
exclude7.show()

# COMMAND ----------

print(exclude7.count())
filtered_exclude7 = exclude7.filter(
    (col("SRVC_BGN_DT") >= col("start_date")) & (col("SRVC_BGN_DT") <= col("end_date"))
)
print(filtered_exclude7.count())
filtered_exclude7.show()

# COMMAND ----------

# Select the required columns and add the new column with a value of 1
filtered_exclude7 = filtered_exclude7.select(
    col("beneID"),
    col("state"),
    lit(1).alias("exclude7")
)

# Show the result
#print(filtered_exclude7.count())
filtered_exclude7 = filtered_exclude7.distinct()
#print(filtered_exclude7.count())

#filtered_exclude7.show()

# COMMAND ----------

# Perform the left join
print(denom.count())
denom = denom.join(filtered_exclude7, on=["beneID", "state"], how="left")
print(denom.count())

# Fill missing values in the 'exclude1' column with 0
denom = denom.fillna({"exclude7": 0})

# Show the result
#denom.show()

# COMMAND ----------

value_counts7 = denom.groupBy("exclude7").count()
value_counts7.show()

# COMMAND ----------

denom.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Final denominator

# COMMAND ----------

# Filter out rows where any of the specified columns have a value of 1
filtered_denom = denom.filter(
    (col("exclude1") != 1) &
    (col("exclude2") != 1) &
    (col("exclude3") != 1) &
    (col("exclude4") != 1) &
    (col("exclude5") != 1) &
    (col("exclude7") != 1)
)

# Show the filtered DataFrame
#print(denom.count())
print(filtered_denom.count())
#filtered_denom.show()

#728272
#

# COMMAND ----------

filtered_denom.write.saveAsTable("dua_058828_spa240.paper_4_lower_back_pain_denom_12_months", mode='overwrite')

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_lower_back_pain_denom_12_months")
print(denom.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Numerator

# COMMAND ----------

outpat2017 = spark.table("dua_058828_spa240.paper_4_otherservices2017_12_months")
outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")

inpat2017 = spark.table("dua_058828_spa240.paper_4_inpatient2017_12_months_new")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

# COMMAND ----------

outpat2019 = outpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2017 = outpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")

inpat2019 = inpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2017 = inpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")

inpat2019 = inpat2019.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2017 = inpat2017.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_lower_back_pain_denom_12_months")
print(denom.count())

# COMMAND ----------

numerator = spark.table("dua_058828_spa240.paper_4_lower_back_pain_denom_12_months")
print(numerator.count())
numerator.show()

# COMMAND ----------

from pyspark.sql.functions import col, expr, lit

# Add start_date and end_date columns
numerator = numerator.withColumn("start_date", expr("date_sub(SRVC_BGN_DT, 0)"))
numerator = numerator.withColumn("end_date", expr("date_add(SRVC_BGN_DT, 28)"))

# Show the result
numerator.show()

# COMMAND ----------

all_files = inpat2018.union(outpat2018).union(inpat2017).union(outpat2017)

# COMMAND ----------

scan_codes = [
    "72020", "72040", "72050", "72052", "72070", "72072", "72074", "72080", 
    "72081", "72082", "72083", "72084", "72100", "72110", "72114", "72120", 
    "72125", "72126", "72127", "72128", "72129", "72130", "72131", "72132", 
    "72133", "72141", "72142", "72146", "72147", "72148", "72149", "72156", 
    "72157", "72158", "72200", "72202", "72220"
]

lower_back_pain_codes = [
    "M4726", "M4727", "M4728", "M47816", "M47817", "M47818", "M47896", "M47897", "M47898",
    "M48061", "M4807", "M4808", "M5116", "M5117", "M5126", "M5127", "M5136", "M5137",
    "M5186", "M5187", "M532X6", "M532X7", "M532X8", "M533", "M5386", "M5387", "M5388",
    "M5416", "M5417", "M5418", "M5430", "M5431", "M5432", "M5440", "M5441", "M5442", "M545",
    "M5450", "M5451", "M5459", "M5489", "M549", "M9903", "M9904", "M9923", "M9933", "M9943",
    "M9953", "M9963", "M9973", "M9983", "M9984", "S33100A", "S33100D", "S33100S", "S33110A",
    "S33110D", "S33110S", "S33120A", "S33120D", "S33120S", "S33130A", "S33130D", "S33130S",
    "S33140A", "S33140D", "S33140S", "S335XXA", "S336XXA", "S338XXA", "S339XXA", "S39002A",
    "S39002D", "S39002S", "S39012A", "S39012D", "S39012S", "S39092A", "S39092D", "S39092S",
    "S3982XA", "S3982XD", "S3982XS", "S3992XA", "S3992XD", "S3992XS"
]

# COMMAND ----------

# Add the 'yes_scan' column
numerator = numerator.withColumn(
    "yes_scan_1",
    when((col("DGNS_CD_1").isin(lower_back_pain_codes)) & (col("LINE_PRCDR_CD").isin(scan_codes)), 1).otherwise(0)
)

# Show the result
numerator.show()

# COMMAND ----------

print(all_files.count())

# Filter the DataFrame to keep rows where conditions are met
all_files = all_files.filter(
    (col("DGNS_CD_1").isin(lower_back_pain_codes)) &
    (col("LINE_PRCDR_CD").isin(scan_codes))
)

# Show the result
print(all_files.count())
all_files.show()

# COMMAND ----------

numerator_exclude = numerator.select("beneID","state","start_date", "end_date")
# Count the number of unique rows based on beneID and state combination
unique_rows_count = numerator_exclude.select("beneID", "state").distinct().count()

# Print the result
print(f"Number of unique rows based on beneID and state combination: {unique_rows_count}")
print(numerator_exclude.count())
numerator_exclude.show()

# COMMAND ----------

print(all_files.count())
all_files = all_files.join(numerator_exclude, on=["beneID","state"], how="inner")
print(all_files.count())
all_files.show()

# COMMAND ----------

print(all_files.count())
filtered_all_files = all_files.filter(
    (col("SRVC_BGN_DT") >= col("start_date")) & (col("SRVC_BGN_DT") <= col("end_date"))
)
print(filtered_all_files.count())
filtered_all_files.show()

# COMMAND ----------

# Select the required columns and add the new column with a value of 1
filtered_all_files = filtered_all_files.select(
    col("beneID"),
    col("state"),
    lit(1).alias("yes_scan_2")
)

# Show the result
print(filtered_all_files.count())
filtered_all_files = filtered_all_files.distinct()
print(filtered_all_files.count())

filtered_all_files.show()

# COMMAND ----------

# Perform the left join
print(numerator.count())
numerator = numerator.join(filtered_all_files, on=["beneID", "state"], how="left")
print(numerator.count())

# Fill missing values in the 'exclude1' column with 0
numerator = numerator.fillna({"yes_scan_2": 0})

# Add the new column `yes_scan` based on the conditions
numerator = numerator.withColumn(
    "yes_scan",
    when((col("yes_scan_1") == 1) | (col("yes_scan_2") == 1), 1).otherwise(0)
)

# Show the result
numerator.show()

# COMMAND ----------

# Add the new column `low_back_pass` based on the value of yes_scan
numerator = numerator.withColumn(
    "low_back_pass",
    when(col("yes_scan") == 1, 0).otherwise(1)
)

# Show the result
numerator.show()

# COMMAND ----------

yes_scan = numerator.groupBy("yes_scan").count()
yes_scan.show()

low_back_pass = numerator.groupBy("low_back_pass").count()
low_back_pass.show()

# COMMAND ----------

numerator.show()

# COMMAND ----------

# MAGIC %md
# MAGIC START SENSITIVITY ANALYSIS

# COMMAND ----------

from pyspark.sql import functions as F

# Assuming your DataFrame is named df
numerator = numerator.withColumn(
    "semester", 
    F.when(F.month(F.col("SRVC_BGN_DT")).between(1, 6), 1).otherwise(2)
)

from pyspark.sql import functions as F

# Perform value count for the 'semester' column
semester_count = numerator.groupBy("semester").count()

# Count the total number of rows in the dataset
total_count = numerator.count()

# Calculate the percentage for each semester value
semester_percentage = semester_count.withColumn(
    "percentage", (F.col("count") / total_count) * 100
)

# Show the result with count and percentage
semester_percentage.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Filter rows where low_back_pass = 1
filtered_df = numerator.filter(F.col("low_back_pass") == 1)

# Perform value count for the 'semester' column on the filtered data
semester_count = filtered_df.groupBy("semester").count()

# Count the total number of rows in the filtered dataset
total_filtered_count = filtered_df.count()

# Calculate the percentage for each semester value
semester_percentage = semester_count.withColumn(
    "percentage", (F.col("count") / total_filtered_count) * 100
)

# Show the result with count and percentage
semester_percentage.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC END SENSITIVITY ANALYSIS

# COMMAND ----------

# MAGIC %md
# MAGIC Final Data Set

# COMMAND ----------

final = numerator.select("beneID","state","yes_scan","low_back_pass")
final.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = final.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = final.groupBy("low_back_pass") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

final.write.saveAsTable("dua_058828_spa240.paper_4_lower_back_pain_02_final_12_months", mode='overwrite')