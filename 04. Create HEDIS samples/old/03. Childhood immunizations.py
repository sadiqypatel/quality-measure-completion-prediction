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

outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2018")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2019")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019")

# COMMAND ----------

outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2019 = outpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")

inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2019 = inpat2019.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")

inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2019 = inpat2019.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

# COMMAND ----------

birth = spark.sql("select BENE_ID, STATE_CD, BIRTH_DT, AGE from extracts.tafr19.demog_elig_base")
birth = birth.withColumnRenamed("BENE_ID", "beneID")
birth = birth.withColumnRenamed("STATE_CD", "state")
birth = birth.withColumnRenamed("AGE", "age_2019")
birth.show()

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_final_data_all_predictors")
denom = denom.select("beneID","state")

denom = denom.join(birth, on=["beneID","state"], how="left")
denom.show()

# COMMAND ----------

# Filter out rows where "age_2019" is greater than 2
denom = denom.filter(col("age_2019") <= 2)

# Show the result
denom.show()

# COMMAND ----------

# Column to check for null values
column_to_check = "BIRTH_DT"

# Filter rows where the specified column is null
null_values_df = denom.filter(col(column_to_check).isNull())

# Show rows where the specified column is null
#null_values_df.show()

# Count the number of null values in the specified column
null_count = null_values_df.count()
print(f"Number of null values in column {column_to_check}: {null_count}")

# COMMAND ----------

denom.show(300)

# COMMAND ----------

# Convert BIRTH_DT to date type
denom = denom.withColumn("BIRTH_DT", col("BIRTH_DT").cast("date"))

# Calculate the date in 2019 when someone turns 2
denom = denom.withColumn(
    "TURN_2_DATE_2019",
    when(
        expr("DATE_ADD(BIRTH_DT, 2 * 365) < '2020-01-01'"),
        expr("DATE_ADD(BIRTH_DT, 2 * 365)")
    ).otherwise(lit("2019-12-31"))
)

# Show the result
denom.show()

# COMMAND ----------

exclusion_codes = [
    "D810", "D811", "D812", "D819",  # Severe Combined Immunodeficiency
    "D800", "D801", "D802", "D803", "D804", "D805", "D806", "D807", "D808", "D809", "D810", "D811", "D812", "D814", "D816", "D817", "D8189", "D819", "D820", "D821", "D822", "D823", "D824", "D828", "D829", "D830", "D831", "D832", "D838", "D839", "D840", "D841", "D848", "D8481", "D84821", "D84822", "D8489", "D849", "D893", "D89810", "D89811", "D89812", "D89813", "D8982", "D89831", "D89832", "D89833", "D89834", "D89835", "D89839", "D8989", "D899",  # Disorders of the Immune System (Immunodeficiency)
    "B20", "Z21",  # HIV
    "B9735",  # HIV Type 2
    "C8100", "C8101", "C8102", "C8103", "C8104", "C8105", "C8106", "C8107", "C8108", "C8109", "C8110", "C8111", "C8112", "C8113", "C8114", "C8115", "C8116", "C8117", "C8118", "C8119", "C8120", "C8121", "C8122", "C8123", "C8124", "C8125", "C8126", "C8127", "C8128", "C8129", "C8130", "C8131", "C8132", "C8133", "C8134", "C8135", "C8136", "C8137", "C8138", "C8139", "C8140", "C8141", "C8142", "C8143", "C8144", "C8145", "C8146", "C8147", "C8148", "C8149", "C8150", "C8151", "C8152", "C8153", "C8154", "C8155", "C8156", "C8157", "C8158", "C8159", "C8160", "C8161", "C8162", "C8163", "C8164", "C8165", "C8166", "C8167", "C8168", "C8169", "C8170", "C8171", "C8172", "C8173", "C8174", "C8175", "C8176", "C8177", "C8178", "C8179", "C8180", "C8181", "C8182", "C8183", "C8184", "C8185", "C8186", "C8187", "C8188", "C8189", "C8190", "C8191", "C8192", "C8193", "C8194", "C8195", "C8196", "C8197", "C8198", "C8199", "C8200", "C8201", "C8202", "C8203", "C8204", "C8205", "C8206", "C8207", "C8208", "C8209", "C8210", "C8211", "C8212", "C8213", "C8214", "C8215", "C8216", "C8217", "C8218", "C8219", "C8220", "C8221", "C8222", "C8223", "C8224", "C8225", "C8226", "C8227", "C8228", "C8229", "C8230", "C8231", "C8232", "C8233", "C8234", "C8235", "C8236", "C8237", "C8238", "C8239", "C8240", "C8241", "C8242", "C8243", "C8244", "C8245", "C8246", "C8247", "C8248", "C8249", "C8250", "C8251", "C8252", "C8253", "C8254", "C8255", "C8256", "C8257", "C8258", "C8259", "C8260", "C8261", "C8262", "C8263", "C8264", "C8265", "C8266", "C8267", "C8268", "C8269", "C8270", "C8271", "C8272", "C8273", "C8274", "C8275", "C8276", "C8277", "C8278", "C8279", "C8280", "C8281", "C8282", "C8283", "C8284", "C8285", "C8286", "C8287", "C8288", "C8289", "C8290", "C8291", "C8292", "C8293", "C8294", "C8295", "C8296", "C8297", "C8298", "C8299", "C8300", "C8301", "C8302", "C8303", "C8304", "C8305", "C8306", "C8307", "C8308", "C8309", "C8310", "C8311", "C8312", "C8313", "C8314", "C8315", "C8316", "C8317", "C8318", "C8319", "C8320", "C8321", "C8322", "C8323", "C8324", "C8325", "C8326", "C8327", "C8328", "C8329", "C8330", "C8331", "C8332", "C8333", "C8334", "C8335", "C8336", "C8337", "C8338", "C8339", "C8340", "C8341", "C8342", "C8343", "C8344", "C8345", "C8346", "C8347", "C8348", "C8349", "C8350", "C8351", "C8352", "C8353", "C8354", "C8355", "C8356", "C8357", "C8358", "C8359", "C8360", "C8361", "C8362", "C8363", "C8364", "C8365", "C8366", "C8367", "C8368", "C8369", "C8370", "C8371", "C8372", "C8373", "C8374", "C8375", "C8376", "C8377", "C8378", "C8379", "C8380", "C8381", "C8382", "C8383", "C8384", "C8385", "C8386", "C8387", "C8388", "C8389", "C8390", "C8391", "C8392", "C8393", "C8394", "C8395", "C8396", "C8397", "C8398", "C8399", "C8440", "C8441", "C8442", "C8443", "C8444", "C8445", "C8446", "C8447", "C8448", "C8449", "C8460", "C8461", "C8462", "C8463", "C8464", "C8465", "C8466", "C8467", "C8468", "C8469", "C8470", "C8471", "C8472", "C8473", "C8474", "C8475", "C8476", "C8477", "C8478", "C8479", "C8480", "C8481", "C8482", "C8483", "C8484", "C8485", "C8486", "C8487", "C8488", "C8489", "C8490", "C8491", "C8492", "C8493", "C8494", "C8495", "C8496","C8497", "C8498", "C8499",  # C84.90-C84.99
    "C84A0", "C84A1", "C84A2", "C84A3", "C84A4", "C84A5", "C84A6", "C84A7", "C84A8", "C84A9",  # C84.A0-C84.A9
    "C84Z0", "C84Z1", "C84Z2", "C84Z3", "C84Z4", "C84Z5", "C84Z6", "C84Z7", "C84Z8", "C84Z9",  # C84.Z0-C84.Z9
    "C8510", "C8511", "C8512", "C8513", "C8514", "C8515", "C8516", "C8517", "C8518", "C8519",  # C85.10-C85.19
    "C8520", "C8521", "C8522", "C8523", "C8524", "C8525", "C8526", "C8527", "C8528", "C8529",  # C85.20-C85.29
    "C8580", "C8581", "C8582", "C8583", "C8584", "C8585", "C8586", "C8587", "C8588", "C8589",  # C85.80-C85.89
    "C8590", "C8591", "C8592", "C8593", "C8594", "C8595", "C8596", "C8597", "C8598", "C8599",  # C85.90-C85.99
    "C860", "C861", "C862", "C863", "C864", "C865",  # C86.0-C86.5
    "C884", "C969", "C96Z",  # C88.4, C96.9, C96.Z

    # Multiple Myeloma
    "C9000", "C9001", "C9002",

    # Leukemia
    "C9010", "C9011", "C9012",  # C90.10-C90.12
    "C9100", "C9101", "C9102",  # C91.00-C91.02
    "C9110", "C9111", "C9112",  # C91.10-C91.12
    "C9130", "C9131", "C9132",  # C91.30-C91.32
    "C9140", "C9141", "C9142",  # C91.40-C91.42
    "C9150", "C9151", "C9152",  # C91.50-C91.52
    "C9160", "C9161", "C9162",  # C91.60-C91.62
    "C9190", "C9191", "C9192",  # C91.90-C91.92
    "C91A0", "C91A1", "C91A2",  # C91.A0-C91.A2
    "C91Z0", "C91Z1", "C91Z2",  # C91.Z0-C91.Z2
    "C9200", "C9201", "C9202",  # C92.00-C92.02
    "C9210", "C9211", "C9212",  # C92.10-C92.12
    "C9220", "C9221", "C9222",  # C92.20-C92.22
    "C9240", "C9241", "C9242",  # C92.40-C92.42
    "C9250", "C9251", "C9252",  # C92.50-C92.52
    "C9260", "C9261", "C9262",  # C92.60-C92.62
    "C9290", "C9291", "C9292",  # C92.90-C92.92
    "C92A0", "C92A1", "C92A2",  # C92.A0-C92.A2
    "C92Z0", "C92Z1", "C92Z2",  # C92.Z0-C92.Z2
    "C9300", "C9301", "C9302",  # C93.00-C93.02
    "C9310", "C9311", "C9312",  # C93.10-C93.12
    "C9330", "C9331", "C9332",  # C93.30-C93.32
    "C9390", "C9391", "C9392",  # C93.90-C93.92
    "C93Z0", "C93Z1", "C93Z2",  # C93.Z0-C93.Z2
    "C9400", "C9401", "C9402",  # C94.00-C94.02
    "C9420", "C9421", "C9422",  # C94.20-C94.22
    "C9430", "C9431", "C9432",  # C94.30-C94.32
    "C9480", "C9481", "C9482",  # C94.80-C94.82
    "C9500", "C9501", "C9502",  # C95.00-C95.02
    "C9510", "C9511", "C9512",  # C95.10-C95.12
    "C9590", "C9591", "C9592",  # C95.90-C95.92

    # Intussusception
    "K561"
]

# COMMAND ----------

exclude = inpat2019.union(outpat2019).union(inpat2018).union(outpat2018)
print(exclude.count())

# COMMAND ----------

# Convert the list of codes to a set
exclusion_codes_set = set(exclusion_codes)

# Filter the DataFrame to keep only rows where DGNS_CD_1 is in the list of codes
exclude = exclude.filter(col("DGNS_CD_1").isin(exclusion_codes_set))

# Show the result
exclude.show()

# COMMAND ----------

exclude = exclude.join(denom, on=["beneID","state"], how="inner")
exclude.show()

# COMMAND ----------

filtered_exclude = exclude.filter(col("SRVC_BGN_DT") <= col("TURN_2_DATE_2019"))
filtered_exclude.show()

# COMMAND ----------

# Select distinct beneID and state
distinct_beneID_state = filtered_exclude.select("beneID", "state").distinct().withColumn("exclude", lit(1))

# Show the result
distinct_beneID_state.show()

# COMMAND ----------

denom = denom.join(distinct_beneID_state, on=["beneID","state"], how="left").fillna(0)
denom.show()

# COMMAND ----------

# Perform a value count for the 'exclude' column in the 'denom' DataFrame
exclude_counts = denom.groupBy("exclude").count()

# Show the result
exclude_counts.show()

# COMMAND ----------

# Drop rows where 'exclude' column is equal to 1
denom = denom.filter(denom.exclude != 1)

# Show the result
denom.show()

# COMMAND ----------

dtap_codes = ["90697", "90698", "90700", "90723"]
ipv_codes = ["90697", "90698", "90713", "90723"]
mmr_codes = ["90707", "90710","B050", "B051", "B052", "B053", "B054", "B0581", "B0589", "B059", "B050", "B051", "B052", "B053", "B054", "B0581", "B0589", "B059","B260", "B261", "B262", "B263", "B2681", "B2682", "B2683", "B2684", "B2685", "B2689", "B269","B0600", "B0601", "B0602", "B0609", "B0681", "B0682", "B0689", "B069"]
hib_codes = ["90644", "90647", "90648", "90697", "90698", "90748"]
hep_b_codes = ["90697", "90723", "90740", "90744", "90747", "90748", "B160", "B161", "B162", "B169", "B170", "B180", "B181", "B1910", "B1911","G0010"]
varicella_codes = ["90710", "90716", "B010", "B0111", "B0112", "B012", "B0181", "B0189", "B019", "B020", "B021", "B0221", "B0222", "B0223", "B0224", "B0229", "B0231", "B0232", "B0233", "B0234", "B0239", "B027", "B028", "B029"]
pcv_codes = ["90670", "90671","G0009"]
hep_a_codes = ["90633","B150", "B159"]
rotavirus_2dose_codes = ["90681"]
rotavirus_3dose_codes = ["90680"]
influenza_codes = ["90655", "90657", "90661", "90673", "90674", "90685", "90686", "90687", "90688", "90689", "90756","G0008"]
influenza_laiv_codes = ["90660", "90672"]

# COMMAND ----------

exclude_codes = (dtap_codes + ipv_codes + mmr_codes + hib_codes + hep_b_codes + varicella_codes + pcv_codes + hep_a_codes + rotavirus_2dose_codes + rotavirus_3dose_codes + influenza_codes + influenza_laiv_codes)

print(exclude_codes)

# COMMAND ----------

exclude = inpat2019.union(outpat2019).union(inpat2018).union(outpat2018)
print(exclude.count())

# COMMAND ----------

print(exclude.count())

exclude = exclude.filter(
    col("DGNS_CD_1").isin(exclude_codes) |
    col("LINE_PRCDR_CD").isin(exclude_codes)
)

print(exclude.count())
exclude.show()

# COMMAND ----------

exclude = exclude.join(denom, on=["beneID","state"], how="inner")
print(exclude.count())
exclude.show()

# COMMAND ----------

filtered_exclude = exclude.filter(col("SRVC_BGN_DT") <= col("TURN_2_DATE_2019"))
print(exclude.count())
print(filtered_exclude.count())
filtered_exclude.show()

# COMMAND ----------

# Select distinct rows based on five fields
print(filtered_exclude.count())
filtered_exclude = filtered_exclude.dropDuplicates(["beneID", "state", "SRVC_BGN_DT", "DGNS_CD_1", "LINE_PRCDR_CD"])
print(filtered_exclude.count())

# Show the result
filtered_exclude.show()

# COMMAND ----------

dtap_codes = ["90697", "90698", "90700", "90723"]
ipv_codes = ["90697", "90698", "90713", "90723"]
mmr_codes = ["90707", "90710","B050", "B051", "B052", "B053", "B054", "B0581", "B0589", "B059", "B050", "B051", "B052", "B053", "B054", "B0581", "B0589", "B059","B260", "B261", "B262", "B263", "B2681", "B2682", "B2683", "B2684", "B2685", "B2689", "B269","B0600", "B0601", "B0602", "B0609", "B0681", "B0682", "B0689", "B069"]
hib_codes = ["90644", "90647", "90648", "90697", "90698", "90748"]
hep_b_codes = ["90697", "90723", "90740", "90744", "90747", "90748", "B160", "B161", "B162", "B169", "B170", "B180", "B181", "B1910", "B1911","G0010"]
varicella_codes = ["90710", "90716", "B010", "B0111", "B0112", "B012", "B0181", "B0189", "B019", "B020", "B021", "B0221", "B0222", "B0223", "B0224", "B0229", "B0231", "B0232", "B0233", "B0234", "B0239", "B027", "B028", "B029"]
pcv_codes = ["90670", "90671","G0009"]
hep_a_codes = ["90633","B150", "B159"]
rotavirus_2dose_codes = ["90681"]
rotavirus_3dose_codes = ["90680"]
influenza_codes = ["90655", "90657", "90661", "90673", "90674", "90685", "90686", "90687", "90688", "90689", "90756","G0008"]
influenza_laiv_codes = ["90660", "90672"]

# COMMAND ----------

# Add hib column based on the condition
filtered_exclude = filtered_exclude.select("beneID","state","SRVC_BGN_DT","DGNS_CD_1","LINE_PRCDR_CD")
filtered_exclude = filtered_exclude.withColumn(
    "dtap",
    when(col("DGNS_CD_1").isin(dtap_codes) | col("LINE_PRCDR_CD").isin(dtap_codes), 1).otherwise(0)
)

# Show the result
filtered_exclude.show()

# COMMAND ----------

filtered_exclude = filtered_exclude.withColumn(
    "ipv",
    when(col("DGNS_CD_1").isin(ipv_codes) | col("LINE_PRCDR_CD").isin(ipv_codes), 1).otherwise(0)
)

filtered_exclude = filtered_exclude.withColumn(
    "mmr",
    when(col("DGNS_CD_1").isin(mmr_codes) | col("LINE_PRCDR_CD").isin(mmr_codes), 1).otherwise(0)
)

filtered_exclude = filtered_exclude.withColumn(
    "hib",
    when(col("DGNS_CD_1").isin(hib_codes) | col("LINE_PRCDR_CD").isin(hib_codes), 1).otherwise(0)
)

filtered_exclude = filtered_exclude.withColumn(
    "hep_b",
    when(col("DGNS_CD_1").isin(hep_b_codes) | col("LINE_PRCDR_CD").isin(hep_b_codes), 1).otherwise(0)
)

filtered_exclude = filtered_exclude.withColumn(
    "varicella",
    when(col("DGNS_CD_1").isin(varicella_codes) | col("LINE_PRCDR_CD").isin(varicella_codes), 1).otherwise(0)
)

filtered_exclude.show()

# COMMAND ----------

pcv_codes = ["90670", "90671","G0009"]
hep_a_codes = ["90633","B150", "B159"]
rotavirus_2dose_codes = ["90681"]
rotavirus_3dose_codes = ["90680"]
influenza_codes = ["90655", "90657", "90661", "90673", "90674", "90685", "90686", "90687", "90688", "90689", "90756","G0008"]
influenza_laiv_codes = ["90660", "90672"]

filtered_exclude = filtered_exclude.withColumn(
    "pcv",
    when(col("DGNS_CD_1").isin(pcv_codes) | col("LINE_PRCDR_CD").isin(pcv_codes), 1).otherwise(0)
)

filtered_exclude = filtered_exclude.withColumn(
    "hep_a",
    when(col("DGNS_CD_1").isin(hep_a_codes) | col("LINE_PRCDR_CD").isin(hep_a_codes), 1).otherwise(0)
)

filtered_exclude = filtered_exclude.withColumn(
    "rota_2dose",
    when(col("DGNS_CD_1").isin(rotavirus_2dose_codes) | col("LINE_PRCDR_CD").isin(rotavirus_2dose_codes), 1).otherwise(0)
)

filtered_exclude = filtered_exclude.withColumn(
    "rota_3dose",
    when(col("DGNS_CD_1").isin(rotavirus_3dose_codes) | col("LINE_PRCDR_CD").isin(rotavirus_3dose_codes), 1).otherwise(0)
)

filtered_exclude = filtered_exclude.withColumn(
    "influenza",
    when(col("DGNS_CD_1").isin(influenza_codes) | col("LINE_PRCDR_CD").isin(influenza_codes), 1).otherwise(0)
)

filtered_exclude = filtered_exclude.withColumn(
    "influenza_laiv",
    when(col("DGNS_CD_1").isin(influenza_laiv_codes) | col("LINE_PRCDR_CD").isin(influenza_laiv_codes), 1).otherwise(0)
)

filtered_exclude.show()

# COMMAND ----------

from pyspark.sql.functions import sum as _sum

# Assuming SparkSession is already created
# spark = SparkSession.builder.appName("SumColumns").getOrCreate()

# Perform the sum aggregation by beneID and state
summed_df = filtered_exclude.groupBy("beneID", "state").agg(
    _sum("dtap").alias("dtap"),
    _sum("ipv").alias("ipv"),
    _sum("mmr").alias("mmr"),
    _sum("hib").alias("hib"),
    _sum("hep_b").alias("hep_b"),
    _sum("varicella").alias("varicella"),
    _sum("pcv").alias("pcv"),
    _sum("hep_a").alias("hep_a"),
    _sum("rota_2dose").alias("rota_2dose"),
    _sum("rota_3dose").alias("rota_3dose"),
    _sum("influenza").alias("influenza"),
    _sum("influenza_laiv").alias("influenza_laiv")
)

# Show the result
summed_df.show()

# COMMAND ----------

# Define the condition for combo3
combo3_condition = (
    (col("dtap") == 4) &
    (col("pcv") == 4) &
    (col("hib") == 3) &
    (col("ipv") == 3) &
    (col("hep_b") == 3) &
    (col("mmr") == 1) &
    (col("varicella") == 1)
)

# Add the combo3 column based on the condition
summed_df = summed_df.withColumn("combo3", when(combo3_condition, 1).otherwise(0))

# Show the result
summed_df.show()

# COMMAND ----------

# Define the condition for combo1-=
combo10_condition = (
    (col("hep_a") == 1) &
    ((col("rota_2dose") == 2) | (col("rota_3dose") == 3)) &
    (col("influenza") == 2) &
    (col("combo3") == 1)
)

# Add the combo3 column based on the condition
summed_df = summed_df.withColumn("combo10", when(combo10_condition, 1).otherwise(0))

# Show the result
summed_df.show()

# COMMAND ----------

# Perform a value count for the 'exclude' column in the 'denom' DataFrame
exclude_counts = summed_df.groupBy("combo3").count()

# Show the result
exclude_counts.show()

# COMMAND ----------

# Perform a value count for the 'exclude' column in the 'denom' DataFrame
exclude_counts = summed_df.groupBy("combo10").count()

# Show the result
exclude_counts.show()

# COMMAND ----------

summed_df.write.saveAsTable("dua_058828_spa240.paper_4_CIS_03_new1", mode='overwrite')

# COMMAND ----------

cis = spark.table("dua_058828_spa240.paper_4_CIS_03_new1")
cis.show()

# COMMAND ----------

cis = cis.select("beneID","state","combo3","combo10")
cis.show()

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_final_data_all_predictors")
denom = denom.join(cis, on=["beneID","state"], how="inner")
denom.show()

# COMMAND ----------

denom.write.saveAsTable("dua_058828_spa240.paper_4_CIS_03_new2", mode='overwrite')

# COMMAND ----------

denom = spark.table("dua_058828_spa240.paper_4_CIS_03_new2")
print(denom.count())

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("combo3") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = denom.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = denom.groupBy("combo10") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

3