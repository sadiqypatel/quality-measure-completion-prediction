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
    (F.col("age").between(18, 65))
)

eligible_population.show()

# COMMAND ----------

print(eligible_population.count())

# COMMAND ----------

# MAGIC %md
# MAGIC TWO YEAR EXCLUSION PER DIAGNOSIS [EXCLUDE1]
# MAGIC
# MAGIC Any Time During the Measurement Year or the Prior Year:
# MAGIC - Members with a diagnosis of pregnancy

# COMMAND ----------

outpat2017 = spark.table("dua_058828_spa240.paper_4_otherservices2017_12_months")
outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")

inpat2017 = spark.table("dua_058828_spa240.paper_4_inpatient2017_12_months_new")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

# COMMAND ----------

outpat2017 = outpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")

inpat2017 = inpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")

inpat2017 = inpat2017.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

# COMMAND ----------

from functools import reduce
# Use reduce to apply union to all DataFrames in the list
all_claims = outpat2017.union(outpat2018).union(inpat2017).union(inpat2018)

# Show the result
all_claims.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Define the ICD-10 code lists without dots

pregnancy_codes = [
    "O0000", "O0001", "O0010", "O00101", "O00102", "O00109", "O0011", "O00111", "O00112", "O00119", 
    "O0020", "O00201", "O00202", "O00209", "O0021", "O00211", "O00212", "O00219", "O0080", "O0081", 
    "O0090", "O0091", "O010", "O011", "O019", "O020", "O021", "O0281", "O0289", "O029", "O030", "O031", 
    "O032", "O0330", "O0331", "O0332", "O0333", "O0334", "O0335", "O0336", "O0337", "O0338", "O0339", 
    "O034", "O035", "O036", "O037", "O0380", "O0381", "O0382", "O0383", "O0384", "O0385", "O0386", 
    "O0387", "O0388", "O0389", "O039", "O045", "O046", "O047", "O0480", "O0481", "O0482", "O0483", 
    "O0484", "O0485", "O0486", "O0487", "O0488", "O0489", "O070", "O071", "O072", "O0730", "O0731", 
    "O0732", "O0733", "O0734", "O0735", "O0736", "O0737", "O0738", "O0739", "O074", "O080", "O081", 
    "O082", "O083", "O084", "O085", "O086", "O087", "O0881", "O0882", "O0883", "O0889", "O089", 
    "O0900", "O0901", "O0902", "O0903", "O0910", "O0911", "O0912", "O0913", "O09211", "O09212", 
    "O09213", "O09219", "O09291", "O09292", "O09293", "O09299", "O0930", "O0931", "O0932", "O0933", 
    "O0940", "O0941", "O0942", "O0943", "O09511", "O09512", "O09513", "O09519", "O09521", "O09522", 
    "O09523", "O09529", "O09611", "O09612", "O09613", "O09619", "O09621", "O09622", "O09623", "O09629", 
    "O0970", "O0971", "O0972", "O0973", "O09811", "O09812", "O09813", "O09819", "O09821", "O09822", 
    "O09823", "O09829", "O09891", "O09892", "O09893", "O09899", "O0990", "O0991", "O0992", "O0993", 
    "O09A0", "O09A1", "O09A2", "O09A3", "O10011", "O10012", "O10013", "O10019", "O1002", "O1003", 
    "O10111", "O10112", "O10113", "O10119", "O1012", "O1013", "O10211", "O10212", "O10213", "O10219", 
    "O1022", "O1023", "O10311", "O10312", "O10313", "O10319", "O1032", "O1033", "O10411", "O10412", 
    "O10413", "O10419", "O1042", "O1043", "O10911", "O10912", "O10913", "O10919", "O1092", "O1093", 
    "O111", "O112", "O113", "O114", "O115", "O119", "O1200", "O1201", "O1202", "O1203", "O1204", 
    "O1205", "O1210", "O1211", "O1212", "O1213", "O1214", "O1215", "O1220", "O1221", "O1222", "O1223", 
    "O1224", "O1225", "O131", "O132", "O133", "O134", "O135", "O139", "O1400", "O1402", "O1403", 
    "O1404", "O1405", "O1410", "O1412", "O1413", "O1414", "O1415", "O1420", "O1422", "O1423", "O1424", 
    "O1425", "O1490", "O1492", "O1493", "O1494", "O1495", "O1500", "O1502", "O1503", "O151", "O152", 
    "O159", "O161", "O162", "O163", "O164", "O165", "O169", "O200", "O208", "O209", "O210", "O211", 
    "O212", "O218", "O219", "O2200", "O2201", "O2202", "O2203", "O2210", "O2211", "O2212", "O2213", 
    "O2220", "O2221", "O2222", "O2223", "O2230", "O2231", "O2232", "O2233", "O2240", "O2241", "O2242", 
    "O2243", "O2250", "O2251", "O2252", "O2253", "O228X1", "O228X2", "O228X3", "O228X9", "O2290", 
    "O2291", "O2292", "O2293", "O2300", "O2301", "O2302", "O2303", "O2310", "O2311", "O2312", "O2313", 
    "O2320", "O2321", "O2322", "O2323", "O2330", "O2331", "O2332", "O2333", "O2340", "O2341", "O2342", 
    "O2343", "O23511", "O23512", "O23513", "O23519", "O23521", "O23522", "O23523", "O23529", "O23591", 
    "O23592", "O23593", "O23599", "O2390", "O2391", "O2392", "O2393", "O24011", "O24012", "O24013", 
    "O24019", "O2402", "O2403", "O24111", "O24112", "O24113", "O24119", "O2412", "O2413", "O24311", 
    "O24312", "O24313", "O24319", "O2432", "O2433", "O24410", "O24414", "O24415", "O24419", "O24420", 
    "O24424", "O24425", "O24429", "O24430", "O24434", "O24435", "O24439", "O24811", "O24812", "O24813", 
    "O24819", "O2482", "O2483", "O24911", "O24912", "O24913", "O24919", "O2492", "O2493", "O2510", 
    "O2511", "O2512", "O2513", "O252", "O253", "O2600", "O2601", "O2602", "O2603", "O2610", "O2611", 
    "O2612", "O2613", "O2620", "O2621", "O2622", "O2623", "O2630", "O2631", "O2632", "O2633", "O2640", 
    "O2641", "O2642", "O2643", "O2650", "O2651", "O2652", "O2653", "O26611", "O26612", "O26613", 
    "O26619", "O2662", "O2663", "O26711", "O26712", "O26713", "O26719", "O2672", "O2673", "O26811", 
    "O26812", "O26813", "O26819", "O26821", "O26822", "O26823", "O26829", "O26831", "O26832", "O26833", 
    "O26839", "O26841", "O26842", "O26843", "O26849", "O26851", "O26852", "O26853", "O26859", "O2686", 
    "O26872", "O26873", "O26879", "O26891", "O26892", "O26893", "O26899", "O2690", "O2691", "O2692", 
    "O2693", "O280", "O281", "O282", "O283", "O284", "O285", "O288", "O289", "O29011", "O29012", 
    "O29013", "O29019", "O29021", "O29022", "O29023", "O29029", "O29091", "O29092", "O29093", "O29099", 
    "O29111", "O29112", "O29113", "O29119", "O29121", "O29122", "O29123", "O29129", "O29191", "O29192", 
    "O29193", "O29199", "O29211", "O29212", "O29213", "O29219", "O29291", "O29292", "O29293", "O29299", 
    "O293X1", "O293X2", "O293X3", "O293X9", "O2940", "O2941", "O2942", "O2943", "O295X1", "O295X2", 
    "O295X3", "O295X9", "O2960", "O2961", "O2962", "O2963", "O298X1", "O298X2", "O298X3", "O298X9", 
    "O2990", "O2991", "O2992", "O2993", "O30001", "O30002", "O30003", "O30009", "O30011", "O30012", 
    "O30013", "O30019", "O30021", "O30022", "O30023", "O30029", "O30031", "O30032", "O30033", "O30039", 
    "O30041", "O30042", "O30043", "O30049", "O30091", "O30092", "O30093", "O30099", "O30101", "O30102", 
    "O30103", "O30109", "O30111", "O30112", "O30113", "O30119", "O30121", "O30122", "O30123", "O30129", 
    "O30191", "O30192", "O30193", "O30199", "O30201", "O30202", "O30203", "O30209", "O30211", "O30212", 
    "O30213", "O30219", "O30221", "O30222", "O30223", "O30229", "O30291", "O30292", "O30293", "O30299", 
    "O30801", "O30802", "O30803", "O30809", "O30811", "O30812", "O30813", "O30819", "O30821", "O30822", 
    "O30823", "O30829", "O30831", "O30832", "O30833", "O30839", "O30891", "O30892", "O30893", "O30899", 
    "O3090", "O3091", "O3092", "O3093", "O3100X0", "O3100X1", "O3100X2", "O3100X3", "O3100X4", 
    "O3100X5", "O3100X9", "O3101X0", "O3101X1", "O3101X2", "O3101X3", "O3101X4", "O3101X5", "O3101X9", 
    "O3102X0", "O3102X1", "O3102X2", "O3102X3", "O3102X4", "O3102X5", "O3102X9", "O3103X0", "O3103X1", 
    "O3103X2", "O3103X3", "O3103X4", "O3103X5", "O3103X9", "O3110X0", "O3110X1", "O3110X2", "O3110X3", 
    "O3110X4", "O3110X5", "O3110X9", "O3111X0", "O3111X1", "O3111X2", "O3111X3", "O3111X4", "O3111X5", 
    "O3111X9", "O3112X0", "O3112X1", "O3112X2", "O3112X3", "O3112X4", "O3112X5", "O3112X9", "O3113X0", 
    "O3113X1", "O3113X2", "O3113X3", "O3113X4", "O3113X5", "O3113X9", "O3120X0", "O3120X1", "O3120X2", 
    "O3120X3", "O3120X4", "O3120X5", "O3120X9", "O3121X0", "O3121X1", "O3121X2", "O3121X3", "O3121X4", 
    "O3121X5", "O3121X9", "O3122X0", "O3122X1", "O3122X2", "O3122X3", "O3122X4", "O3122X5", "O3122X9", 
    "O3123X0", "O3123X1", "O3123X2", "O3123X3", "O3123X4", "O3123X5", "O3123X9", "O3130X0", "O3130X1", 
    "O3130X2", "O3130X3", "O3130X4", "O3130X5", "O3130X9", "O3131X0", "O3131X1", "O3131X2", "O3131X3", 
    "O3131X4", "O3131X5", "O3131X9", "O3132X0", "O3132X1", "O3132X2", "O3132X3", "O3132X4", "O3132X5", 
    "O3132X9", "O3133X0", "O3133X1", "O3133X2", "O3133X3", "O3133X4", "O3133X5", "O3133X9", "O318X10", 
    "O318X11", "O318X12", "O318X13", "O318X14", "O318X15", "O318X19", "O318X20", "O318X21", "O318X22", 
    "O318X23", "O318X24", "O318X25", "O318X29", "O318X30", "O318X31", "O318X32", "O318X33", "O318X34", 
    "O318X35", "O318X39", "O318X90", "O318X91", "O318X92", "O318X93", "O318X94", "O318X95", "O318X99", 
    "O320XX0", "O320XX1", "O320XX2", "O320XX3", "O320XX4", "O320XX5", "O320XX9", "O321XX0", "O321XX1", 
    "O321XX2", "O321XX3", "O321XX4", "O321XX5", "O321XX9", "O322XX0", "O322XX1", "O322XX2", "O322XX3", 
    "O322XX4", "O322XX5", "O322XX9", "O323XX0", "O323XX1", "O323XX2", "O323XX3", "O323XX4", "O323XX5", 
    "O323XX9", "O324XX0", "O324XX1", "O324XX2", "O324XX3", "O324XX4", "O324XX5", "O324XX9", "O326XX0", 
    "O326XX1", "O326XX2", "O326XX3", "O326XX4", "O326XX5", "O326XX9", "O328XX0", "O328XX1", "O328XX2", 
    "O328XX3", "O328XX4", "O328XX5", "O328XX9", "O329XX0", "O329XX1", "O329XX2", "O329XX3", "O329XX4", 
    "O329XX5", "O329XX9", "O330", "O331", "O332", "O333XX0", "O333XX1", "O333XX2", "O333XX3", "O333XX4", 
    "O333XX5", "O333XX9", "O334XX0", "O334XX1", "O334XX2", "O334XX3", "O334XX4", "O334XX5", "O334XX9", 
    "O335XX0", "O335XX1", "O335XX2", "O335XX3", "O335XX4", "O335XX5", "O335XX9", "O336XX0", "O336XX1", 
    "O336XX2", "O336XX3", "O336XX4", "O336XX5", "O336XX9", "O337XX0", "O337XX1", "O337XX2", "O337XX3", 
    "O337XX4", "O337XX5", "O337XX9", "O338", "O339", "O3400", "O3401", "O3402", "O3403", "O3410", 
    "O3411", "O3412", "O3413", "O34211", "O34212", "O34219", "O3429", "O3430", "O3431", "O3432", 
    "O3433", "O3440", "O3441", "O3442", "O3443", "O34511", "O34512", "O34513", "O34519", "O34521", 
    "O34522", "O34523", "O34529", "O34531", "O34532", "O34533", "O34539", "O34591", "O34592", "O34593", 
    "O34599", "O3460", "O3461", "O3462", "O3463", "O3470", "O3471", "O3472", "O3473", "O3480", 
    "O3481", "O3482", "O3483", "O3490", "O3491", "O3492", "O3493", "O350XX0", "O350XX1", "O350XX2", 
    "O350XX3", "O350XX4", "O350XX5", "O350XX9", "O351XX0", "O351XX1", "O351XX2", "O351XX3", "O351XX4", 
    "O351XX5", "O351XX9", "O352XX0", "O352XX1", "O352XX2", "O352XX3", "O352XX4", "O352XX5", "O352XX9", 
    "O353XX0", "O353XX1", "O353XX2", "O353XX3", "O353XX4", "O353XX5", "O353XX9", "O354XX0", "O354XX1", 
    "O354XX2", "O354XX3", "O354XX4", "O354XX5", "O354XX9", "O355XX0", "O355XX1", "O355XX2", "O355XX3", 
    "O355XX4", "O355XX5", "O355XX9", "O356XX0", "O356XX1", "O356XX2", "O356XX3", "O356XX4", "O356XX5", 
    "O356XX9", "O357XX0", "O357XX1", "O357XX2", "O357XX3", "O357XX4", "O357XX5", "O357XX9", "O358XX0", 
    "O358XX1", "O358XX2", "O358XX3", "O358XX4", "O358XX5", "O358XX9", "O359XX0", "O359XX1", "O359XX2", 
    "O359XX3", "O359XX4", "O359XX5", "O359XX9", "O360110", "O360111", "O360112", "O360113", "O360114", 
    "O360115", "O360119", "O360120", "O360121", "O360122", "O360123", "O360124", "O360125", "O360129", 
    "O360130", "O360131", "O360132", "O360133", "O360134", "O360135", "O360139", "O360190", "O360191", 
    "O360192", "O360193", "O360194", "O360195", "O360199", "O360910", "O360911", "O360912", "O360913", 
    "O360914", "O360915", "O360919", "O360920", "O360921", "O360922", "O360923", "O360924", "O360925", 
    "O360929", "O360930", "O360931", "O360932", "O360933", "O360934", "O360935", "O360939", "O360990", 
    "O360991", "O360992", "O360993", "O360994", "O360995", "O360999", "O361110", "O361111", "O361112", 
    "O361113", "O361114", "O361115", "O361119", "O361120", "O361121", "O361122", "O361123", "O361124", 
    "O361125", "O361129", "O361130", "O361131", "O361132", "O361133", "O361134", "O361135", "O361139", 
    "O361190", "O361191", "O361192", "O361193", "O361194", "O361195", "O361199", "O361910", "O361911", 
    "O361912", "O361913", "O361914", "O361915", "O361919", "O361920", "O361921", "O361922", "O361923", 
    "O361924", "O361925", "O361929", "O361930", "O361931", "O361932", "O361933", "O361934", "O361935", 
    "O361939", "O361990", "O361991", "O361992", "O361993", "O361994", "O361995", "O361999", "O3620X0", 
    "O3620X1", "O3620X2", "O3620X3", "O3620X4", "O3620X5", "O3620X9", "O3621X0", "O3621X1", "O3621X2", 
    "O3621X3", "O3621X4", "O3621X5", "O3621X9", "O3622X0", "O3622X1", "O3622X2", "O3622X3", "O3622X4", 
    "O3622X5", "O3622X9", "O3623X0", "O3623X1", "O3623X2", "O3623X3", "O3623X4", "O3623X5", "O3623X9", 
    "O364XX0", "O364XX1", "O364XX2", "O364XX3", "O364XX4", "O364XX5", "O364XX9", "O365110", "O365111", 
    "O365112", "O365113", "O365114", "O365115", "O365119", "O365120", "O365121", "O365122", "O365123", 
    "O365124", "O365125", "O365129", "O365130", "O365131", "O365132", "O365133", "O365134", "O365135", 
    "O365139", "O365190", "O365191", "O365192", "O365193", "O365194", "O365195", "O365199", "O365910", 
    "O365911", "O365912", "O365913", "O365914", "O365915", "O365919", "O365920", "O365921", "O365922", 
    "O365923", "O365924", "O365925", "O365929", "O365930", "O365931", "O365932", "O365933", "O365934", 
    "O365935", "O365939", "O365990", "O365991", "O365992", "O365993", "O365994", "O365995", "O365999", 
    "O3660X0", "O3660X1", "O3660X2", "O3660X3", "O3660X4", "O3660X5", "O3660X9", "O3661X0", "O3661X1", 
    "O3661X2", "O3661X3", "O3661X4", "O3661X5", "O3661X9", "O3662X0", "O3662X1", "O3662X2", "O3662X3", 
    "O3662X4", "O3662X5", "O3662X9", "O3663X0", "O3663X1", "O3663X2", "O3663X3", "O3663X4", "O3663X5", 
    "O3663X9", "O3670X0", "O3670X1", "O3670X2", "O3670X3", "O3670X4", "O3670X5", "O3670X9", "O3671X0", 
    "O3671X1", "O3671X2", "O3671X3", "O3671X4", "O3671X5", "O3671X9", "O3672X0", "O3672X1", "O3672X2", 
    "O3672X3", "O3672X4", "O3672X5", "O3672X9", "O3673X0", "O3673X1", "O3673X2", "O3673X3", "O3673X4", 
    "O3673X5", "O3673X9", "O3680X0", "O3680X1", "O3680X2", "O3680X3", "O3680X4", "O3680X5", "O3680X9", 
    "O368120", "O368121", "O368122", "O368123", "O368124", "O368125", "O368129", "O368130", "O368131", 
    "O368132", "O368133", "O368134", "O368135", "O368139", "O368190", "O368191", "O368192", "O368193", 
    "O368194", "O368195", "O368199", "O368210", "O368211", "O368212", "O368213", "O368214", "O368215", 
    "O368219", "O368220", "O368221", "O368222", "O368223", "O368224", "O368225", "O368229", "O368230", 
    "O368231", "O368232", "O368233", "O368234", "O368235", "O368239", "O368290", "O368291", "O368292", 
    "O368293", "O368294", "O368295", "O368299", "O368910", "O368911", "O368912", "O368913", "O368914", 
    "O368915", "O368919", "O368920", "O368921", "O368922", "O368923", "O368924", "O368925", "O368929", 
    "O368930", "O368931", "O368932", "O368933", "O368934", "O368935", "O368939", "O368990", "O368991", 
    "O368992", "O368993", "O368994", "O368995", "O368999", "O3690X0", "O3690X1", "O3690X2", "O3690X3", 
    "O3690X4", "O3690X5", "O3690X9", "O3691X0", "O3691X1", "O3691X2", "O3691X3", "O3691X4", "O3691X5", 
    "O3691X9", "O3692X0", "O3692X1", "O3692X2", "O3692X3", "O3692X4", "O3692X5", "O3692X9", "O3693X0", 
    "O3693X1", "O3693X2", "O3693X3", "O3693X4", "O3693X5", "O3693X9", "O401XX0", "O401XX1", "O401XX2", 
    "O401XX3", "O401XX4", "O401XX5", "O401XX9", "O402XX0", "O402XX1", "O402XX2", "O402XX3", "O402XX4", 
    "O402XX5", "O402XX9", "O403XX0", "O403XX1", "O403XX2", "O403XX3", "O403XX4", "O403XX5", "O403XX9", 
    "O409XX0", "O409XX1", "O409XX2", "O409XX3", "O409XX4", "O409XX5", "O409XX9", "O4100X0", "O4100X1", 
    "O4100X2", "O4100X3", "O4100X4", "O4100X5", "O4100X9", "O4101X0", "O4101X1", "O4101X2", "O4101X3", 
    "O4101X4", "O4101X5", "O4101X9", "O4102X0", "O4102X1", "O4102X2", "O4102X3", "O4102X4", "O4102X5", 
    "O4102X9", "O4103X0", "O4103X1", "O4103X2", "O4103X3", "O4103X4", "O4103X5", "O4103X9", "O411010", 
    "O411011", "O411012", "O411013", "O411014", "O411015", "O411019", "O411020", "O411021", "O411022", 
    "O411023", "O411024", "O411025", "O411029", "O411030", "O411031", "O411032", "O411033", "O411034", 
    "O411035", "O411039", "O411090", "O411091", "O411092", "O411093", "O411094", "O411095", "O411099", 
    "O411210", "O411211", "O411212", "O411213", "O411214", "O411215", "O411219", "O411220", "O411221", 
    "O411222", "O411223", "O411224", "O411225", "O411229", "O411230", "O411231", "O411232", "O411233", 
    "O411234", "O411235", "O411239", "O411290", "O411291", "O411292", "O411293", "O411294", "O411295", 
    "O411299", "O411410", "O411411", "O411412", "O411413", "O411414", "O411415", "O411419", "O411420", 
    "O411421", "O411422", "O411423", "O411424", "O411425", "O411429", "O411430", "O411431", "O411432", 
    "O411433", "O411434", "O411435", "O411439", "O411490", "O411491", "O411492", "O411493", "O411494", 
    "O411495", "O411499", "O418X10", "O418X11", "O418X12", "O418X13", "O418X14", "O418X15", "O418X19", 
    "O418X20", "O418X21", "O418X22", "O418X23", "O418X24", "O418X25", "O418X29", "O418X30", "O418X31", 
    "O418X32", "O418X33", "O418X34", "O418X35", "O418X39", "O418X90", "O418X91", "O418X92", "O418X93", 
    "O418X94", "O418X95", "O418X99", "O4190X0", "O4190X1", "O4190X2", "O4190X3", "O4190X4", "O4190X5", 
    "O4190X9", "O4191X0", "O4191X1", "O4191X2", "O4191X3", "O4191X4", "O4191X5", "O4191X9", "O4192X0", 
    "O4192X1", "O4192X2", "O4192X3", "O4192X4", "O4192X5", "O4192X9", "O4193X0", "O4193X1", "O4193X2", 
    "O4193X3", "O4193X4", "O4193X5", "O4193X9", "O4200", "O42011", "O42012", "O42013", "O42019", 
    "O4202", "O4210", "O42111", "O42112", "O42113", "O42119", "O4212", "O4290", "O42911", "O42912", 
    "O42913", "O42919", "O4292", "O43011", "O43012", "O43013", "O43019", "O43021", "O43022", "O43023", 
    "O43029", "O43101", "O43102", "O43103", "O43109", "O43111", "O43112", "O43113", "O43119", "O43121", 
    "O43122", "O43123", "O43129", "O43191", "O43192", "O43193", "O43199", "O43211", "O43212", "O43213", 
    "O43219", "O43221", "O43222", "O43223", "O43229", "O43231", "O43232", "O43233", "O43239", "O43811", 
    "O43812", "O43813", "O43819", "O43891", "O43892", "O43893", "O43899", "O4390", "O4391", "O4392", 
    "O4393", "O4400", "O4401", "O4402", "O4403", "O4410", "O4411", "O4412", "O4413", "O4420", "O4421", 
    "O4422", "O4423", "O4430", "O4431", "O4432", "O4433", "O4440", "O4441", "O4442", "O4443", "O4450", 
    "O4451", "O4452", "O4453", "O45001", "O45002", "O45003", "O45009", "O45011", "O45012", "O45013", 
    "O45019", "O45021", "O45022", "O45023", "O45029", "O45091", "O45092", "O45093", "O45099", "O458X1", 
    "O458X2", "O458X3", "O458X9", "O4590", "O4591", "O4592", "O4593", "O46001", "O46002", "O46003", 
    "O46009", "O46011", "O46012", "O46013", "O46019", "O46021", "O46022", "O46023", "O46029", "O46091", 
    "O46092", "O46093", "O46099", "O468X1", "O468X2", "O468X3", "O468X9", "O4690", "O4691", "O4692", 
    "O4693", "O4700", "O4702", "O4703", "O471", "O479", "O480", "O481", "O6000", "O6002", "O6003", 
    "O6010X0", "O6010X1", "O6010X2", "O6010X3", "O6010X4", "O6010X5", "O6010X9", "O6012X0", "O6012X1", 
    "O6012X2", "O6012X3", "O6012X4", "O6012X5", "O6012X9", "O6013X0", "O6013X1", "O6013X2", "O6013X3", 
    "O6013X4", "O6013X5", "O6013X9", "O6014X0", "O6014X1", "O6014X2", "O6014X3", "O6014X4", "O6014X5", 
    "O6014X9", "O6020X0", "O6020X1", "O6020X2", "O6020X3", "O6020X4", "O6020X5", "O6020X9", "O6022X0", 
    "O6022X1", "O6022X2", "O6022X3", "O6022X4", "O6022X5", "O6022X9", "O6023X0", "O6023X1", "O6023X2", 
    "O6023X3", "O6023X4", "O6023X5", "O6023X9", "O610", "O611", "O618", "O619", "O620", "O621", "O622", 
    "O623", "O624", "O628", "O629", "O630", "O631", "O632", "O639", "O640XX0", "O640XX1", "O640XX2", 
    "O640XX3", "O640XX4", "O640XX5", "O640XX9", "O641XX0", "O641XX1", "O641XX2", "O641XX3", "O641XX4", 
    "O641XX5", "O641XX9", "O642XX0", "O642XX1", "O642XX2", "O642XX3", "O642XX4", "O642XX5", "O642XX9", 
    "O643XX0", "O643XX1", "O643XX2", "O643XX3", "O643XX4", "O643XX5", "O643XX9", "O644XX0", "O644XX1", 
    "O644XX2", "O644XX3", "O644XX4", "O644XX5", "O644XX9", "O645XX0", "O645XX1", "O645XX2", "O645XX3", 
    "O645XX4", "O645XX5", "O645XX9", "O648XX0", "O648XX1", "O648XX2", "O648XX3", "O648XX4", "O648XX5", 
    "O648XX9", "O649XX0", "O649XX1", "O649XX2", "O649XX3", "O649XX4", "O649XX5", "O649XX9", "O650", 
    "O651", "O652", "O653", "O654", "O655", "O658", "O659", "O660", "O661", "O662", "O663", "O6640", 
    "O6641", "O665", "O666", "O668", "O669", "O670", "O678", "O679", "O68", "O690XX0", "O690XX1", 
    "O690XX2", "O690XX3", "O690XX4", "O690XX5", "O690XX9", "O691XX0", "O691XX1", "O691XX2", "O691XX3", 
    "O691XX4", "O691XX5", "O691XX9", "O692XX0", "O692XX1", "O692XX2", "O692XX3", "O692XX4", "O692XX5", 
    "O692XX9", "O693XX0", "O693XX1", "O693XX2", "O693XX3", "O693XX4", "O693XX5", "O693XX9", "O694XX0", 
    "O694XX1", "O694XX2", "O694XX3", "O694XX4", "O694XX5", "O694XX9", "O695XX0", "O695XX1", "O695XX2", 
    "O695XX3", "O695XX4", "O695XX5", "O695XX9", "O6981X0", "O6981X1", "O6981X2", "O6981X3", "O6981X4", 
    "O6981X5", "O6981X9", "O6982X0", "O6982X1", "O6982X2", "O6982X3", "O6982X4", "O6982X5", "O6982X9", 
    "O6989X0", "O6989X1", "O6989X2", "O6989X3", "O6989X4", "O6989X5", "O6989X9", "O699XX0", "O699XX1", 
    "O699XX2", "O699XX3", "O699XX4", "O699XX5", "O699XX9", "O700", "O701", "O7020", "O7021", "O7022", 
    "O7023", "O703", "O704", "O709", "O7100", "O7102", "O7103", "O711", "O712", "O713", "O714", 
    "O715", "O716", "O717", "O7181", "O7182", "O7189", "O719", "O720", "O721", "O722", "O723", 
    "O730", "O731", "O740", "O741", "O742", "O743", "O744", "O745", "O746", "O747", "O748", 
    "O749", "O750", "O751", "O752", "O753", "O754", "O755", "O7581", "O7582", "O7589", "O759", 
    "O76", "O770", "O771", "O778", "O779", "O80", "O82", "O85", "O860", "O8600", "O8601", 
    "O8602", "O8603", "O8604", "O8609", "O8611", "O8612", "O8613", "O8619", "O8620", "O8621", 
    "O8622", "O8629", "O864", "O8681", "O8689", "O870", "O871", "O872", "O873", "O874", 
    "O878", "O879", "O88011", "O88012", "O88013", "O88019", "O8802", "O8803", "O88111", "O88112", 
    "O88113", "O88119", "O8812", "O8813", "O88211", "O88212", "O88213", "O88219", "O8822", "O8823", 
    "O88311", "O88312", "O88313", "O88319", "O8832", "O8833", "O88811", "O88812", "O88813", "O88819", 
    "O8882", "O8883", "O8901", "O8909", "O891", "O892", "O893", "O894", "O895", "O896", 
    "O898", "O899", "O900", "O901", "O902", "O903", "O904", "O905", "O906", "O9081", 
    "O9089", "O909", "O91011", "O91012", "O91013", "O91019", "O9102", "O91111", "O91112", "O91113", 
    "O91119", "O9112", "O91211", "O91212", "O91213", "O91219", "O9122", "O94", "O98011", "O98012", 
    "O98013", "O98019", "O9802", "O9803", "O98111", "O98112", "O98113", "O98119", "O9812", "O9813", 
    "O98211", "O98212", "O98213", "O98219", "O9822", "O9823", "O98311", "O98312", "O98313", "O98319", 
    "O9832", "O9833", "O98411", "O98412", "O98413", "O98419", "O9842", "O9843", "O98511", "O98512", 
    "O98513", "O98519", "O9852", "O9853", "O98611", "O98612", "O98613", "O98619", "O9862", "O9863", 
    "O98711", "O98712", "O98713", "O98719", "O9872", "O9873", "O98811", "O98812", "O98813", "O98819", 
    "O9882", "O9883", "O98911", "O98912", "O98913", "O98919", "O9892", "O9893", "O99011", "O99012", 
    "O99013", "O99019", "O9902", "O9903", "O99111", "O99112", "O99113", "O99119", "O9912", "O9913", 
    "O99210", "O99211", "O99212", "O99213", "O99214", "O99215", "O99280", "O99281", "O99282", "O99283", 
    "O99284", "O99285", "O99310", "O99311", "O99312", "O99313", "O99314", "O99315", "O99320", "O99321", 
    "O99322", "O99323", "O99324", "O99325", "O99330", "O99331", "O99332", "O99333", "O99334", "O99335", 
    "O99340", "O99341", "O99342", "O99343", "O99344", "O99345", "O99350", "O99351", "O99352", "O99353", 
    "O99354", "O99355", "O99411", "O99412", "O99413", "O99419", "O9942", "O9943", "O99511", "O99512", 
    "O99513", "O99519", "O9952", "O9953", "O99611", "O99612", "O99613", "O99619", "O9962", "O9963", 
    "O99711", "O99712", "O99713", "O99719", "O9972", "O9973", "O99810", "O99814", "O99815", "O99820", 
    "O99824", "O99825", "O99830", "O99834", "O99835", "O99840", "O99841", "O99842", "O99843", "O99844", 
    "O99845", "O9989", "O9A111", "O9A112", "O9A113", "O9A119", "O9A12", "O9A13", "O9A211", "O9A212", 
    "O9A213", "O9A219", "O9A22", "O9A23", "O9A311", "O9A312", "O9A313", "O9A319", "O9A32", "O9A33", 
    "O9A411", "O9A412", "O9A413", "O9A419", "O9A42", "O9A43", "O9A511", "O9A512", "O9A513", "O9A519", 
    "O9A52", "O9A53", "Z3201", "Z331", "Z332", "Z333", "Z3400", "Z3401", "Z3402", "Z3403", "Z3480", 
    "Z3481", "Z3482", "Z3483", "Z3490", "Z3491", "Z3492", "Z3493", "Z36", "Z370", "Z371", "Z372", 
    "Z373", "Z374", "Z3750", "Z3751", "Z3752", "Z3753", "Z3754", "Z3759", "Z3760", "Z3761", "Z3762", 
    "Z3763", "Z3764", "Z3769", "Z377", "Z379", "Z3800", "Z3801", "Z381", "Z382", "Z3830", "Z3831", 
    "Z384", "Z385", "Z3861", "Z3862", "Z3863", "Z3864", "Z3865", "Z3866", "Z3868", "Z3869", "Z387", 
    "Z388", "Z390", "Z392", "Z3A00", "Z3A01", "Z3A08", "Z3A09", "Z3A10", "Z3A11", "Z3A12", "Z3A13", 
    "Z3A14", "Z3A15", "Z3A16", "Z3A17", "Z3A18", "Z3A19", "Z3A20", "Z3A21", "Z3A22", "Z3A23", "Z3A24", 
    "Z3A25", "Z3A26", "Z3A27", "Z3A28", "Z3A29", "Z3A30", "Z3A31", "Z3A32", "Z3A33", "Z3A34", "Z3A35", 
    "Z3A36", "Z3A37", "Z3A38", "Z3A39", "Z3A40", "Z3A41", "Z3A42", "Z3A49", "O00101", "O00102", 
    "O00109", "O00111", "O00112", "O00119", "O002", "O00201", "O00202", "O00209", "O00211", "O00212", 
    "O00219", "O008", "O009", "O30131", "O30132", "O30133", "O30139", "O30231", "O30232", "O30233", 
    "O30239", "O30831", "O30832", "O30833", "O30839", "O337", "O3421", "O34218", "O3422", "O368310", 
    "O368311", "O368312", "O368313", "O368314", "O368315", "O368319", "O368320", "O368321", "O368322", 
    "O368323", "O368324", "O368325", "O368329", "O368330", "O368331", "O368332", "O368333", "O368334", 
    "O368335", "O368339", "O368390", "O368391", "O368392", "O368393", "O368394", "O368395", "O368399", 
    "O702", "O9103", "O9113", "O9123", "O92011", "O92012", "O92013", "O92019", "O9202", "O9203", 
    "O92111", "O92112", "O92113", "O92119", "O9212", "O9213", "O9220", "O9229", "O923", "O924", 
    "O925", "O926", "O9270", "O9279", "O99891", "O99892", "O99893", "Z0371", "Z0372", "Z0373", 
    "Z0374", "Z0375", "Z0379", "Z360", "Z361", "Z362", "Z363", "Z364", "Z365", "Z3681", "Z3682", 
    "Z3683", "Z3684", "Z3685", "Z3686", "Z3687", "Z3688", "Z3689", "Z368A", "Z369"
]

# Define the in vitro fertilization codes
in_vitro_codes = ["Z312", "Z3183", "O0981"]

in_vitro_codes_cpt = ["S4015", "S4016", "S4018", "S4020", "S4021"]


# Combine all codes into one list
all_codes = pregnancy_codes + in_vitro_codes + in_vitro_codes_cpt

# Create a column in the dataset to flag the codes
all_claims = all_claims.withColumn(
    "exclude1",
    F.when(F.col("DGNS_CD_1").isin(all_codes) | F.col("LINE_PRCDR_CD").isin(all_codes), 1 
    ).otherwise(0)
)

all_claims.show()

# COMMAND ----------

exclude1 = all_claims.groupBy("exclude1").count()
exclude1.show()

# COMMAND ----------

# Assuming df is your DataFrame
exclude1 = all_claims.filter(all_claims.exclude1 != 0)

# Show the result
exclude1.show()

# COMMAND ----------

print(exclude1.count())

# COMMAND ----------

# MAGIC %md
# MAGIC During the Year Prior and Measurement Period
# MAGIC - Perinatal Conditions

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Apply conditional logic directly using PySpark SQL functions
all_claims = all_claims.withColumn(
    'exclude2',
    when(
        (col('DGNS_CD_1').startswith('P')) & 
        (col('DGNS_CD_1').substr(2, 2).cast('int').between(0, 96)),
        1
    ).otherwise(0)
)

# Show the results to verify correct application
all_claims.show()

# COMMAND ----------

exclude2 = all_claims.groupBy("exclude2").count()
exclude2.show()

# COMMAND ----------

# Assuming df is your DataFrame
exclude2 = all_claims.filter(all_claims.exclude2 != 0)

# Show the result
exclude2.show()

# COMMAND ----------

print(exclude2.count())

# COMMAND ----------

# MAGIC %md
# MAGIC PRIOR YEAR AND CURRENT YEAR EXCLUSION [EXCLUDE3]
# MAGIC
# MAGIC During the Year Prior to the Measurement Year:
# MAGIC - Chemotherapy maintenance
# MAGIC - Principle diagnosis of rehabilitation
# MAGIC - Organ transplant
# MAGIC - Potentially planned procedure without a principal acute diagnosis

# COMMAND ----------

outpat2017 = spark.table("dua_058828_spa240.paper_4_otherservices2017_12_months")
outpat2018 = spark.table("dua_058828_spa240.paper_4_otherservices2018_12_months")
outpat2019 = spark.table("dua_058828_spa240.paper_4_otherservices2019_12_months")

inpat2017 = spark.table("dua_058828_spa240.paper_4_inpatient2017_12_months_new")
inpat2018 = spark.table("dua_058828_spa240.paper_4_inpatient2018_12_months_new")
inpat2019 = spark.table("dua_058828_spa240.paper_4_inpatient2019_12_months_new")

# COMMAND ----------

outpat2017 = outpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")
outpat2018 = outpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "LINE_PRCDR_CD")

inpat2017 = inpat2017.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")
inpat2018 = inpat2018.select("beneID","state","CLM_ID","SRVC_BGN_DT","DGNS_CD_1", "PRCDR_CD_1")

inpat2017 = inpat2017.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")
inpat2018 = inpat2018.withColumnRenamed("PRCDR_CD_1", "LINE_PRCDR_CD")

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("PlannedAdmissionsCheck").getOrCreate()

# List of relevant ICD-10 codes without periods
planned_admission_codes = [
    'Z5111', 'Z5112',  # Chemotherapy maintenance
    'Z5189', 'Z509', 'Z508',  # Rehabilitation
    'Z940', 'Z941', 'Z942', 'Z943', 'Z944', 'Z945', 'Z946', 'Z947', 'Z9481', 'Z9482', 'Z9483', 'Z9484', 'Z9489', 'Z949',  # Organ transplant
    'Z01818', 'Z0184', 'Z0189', 'Z0289'  # Potentially planned procedure without a principal acute diagnosis
]

# Apply conditional logic to create the 'exclud3' indicator
all_claims = all_claims.withColumn(
    'exclude3',
    when(col('DGNS_CD_1').isin(planned_admission_codes), 1).otherwise(0)
)

# Show the results to verify correct application
all_claims.show()

# COMMAND ----------

exclude3 = all_claims.groupBy("exclude3").count()
exclude3.show()

# COMMAND ----------

# Assuming df is your DataFrame
exclude3 = all_claims.filter(all_claims.exclude3 != 0)

# Show the result
exclude3.show()

# COMMAND ----------

print(exclude3.count())

# COMMAND ----------

# MAGIC %md
# MAGIC REMOVE EXCLUDdE MEMBERS

# COMMAND ----------

# Select distinct beneID and state from each DataFrame
distinct1 = exclude1.select("beneID", "state").distinct()
distinct2 = exclude2.select("beneID", "state").distinct()
distinct3 = exclude3.select("beneID", "state").distinct()

# Union the DataFrames
combined_df = distinct1.union(distinct2).union(distinct3)

# Select distinct beneID and state from the combined DataFrame
final_distinct_df = combined_df.distinct()

# Show the result
final_distinct_df.show()

# COMMAND ----------

# Add a new column 'exclude' with a constant value of 1
final_distinct_df = final_distinct_df.withColumn("exclude", lit(1))

# Show the result
final_distinct_df.show()

# COMMAND ----------

eligible_population= eligible_population.join(final_distinct_df, on=['beneID','state'], how='left').fillna(0)
eligible_population.show()

# COMMAND ----------

# Drop rows where exclude is 1
eligible_population = eligible_population.filter(eligible_population.exclude != 1)

test = eligible_population.groupBy("exclude").count()
test.show()

#14,178,331 is originla value

# COMMAND ----------

# MAGIC %md
# MAGIC STORE INITIAL DENOMINATOR

# COMMAND ----------

eligible_population.write.saveAsTable("dua_058828_spa240.paper_4_pcr_denom_12_months", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC IDENTIFY HOSPITALIZATIONS

# COMMAND ----------

sample = spark.table("dua_058828_spa240.paper_4_pcr_denom_12_months")
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

from pyspark.sql import functions as F

# Aggregating by beneID, state, episode
aggregated_df = result_df.groupBy("beneID", "state", "episode").agg(
    F.min("StartDate").alias("min_StartDate"),
    F.max("EndDate").alias("max_EndDate"),
    F.max("inpatientVisit").alias("max_inpatientVisit")
)

# Calculating the number of days
# Ensure StartDate and EndDate are in date format
aggregated_df = aggregated_df.withColumn(
    "days",
    F.datediff(F.col("max_EndDate"), F.col("min_StartDate"))
)

aggregated_df.show()

# COMMAND ----------

# Dropping rows where max_inpatientVisit is 0
aggregated_df = aggregated_df.filter(F.col("max_inpatientVisit") != 0)
aggregated_df.show()

# COMMAND ----------

print(aggregated_df.count())
# Dropping rows where days is 0
aggregated_df = aggregated_df.filter(F.col("days") != 0)
print(aggregated_df.count())

# COMMAND ----------

aggregated_df.write.saveAsTable("dua_058828_spa240.paper_4_pcr_all_acute_care_visits", mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC FIND INDEX VISITS vs. VISITS POST

# COMMAND ----------

visits = spark.table("dua_058828_spa240.paper_4_pcr_all_acute_care_visits")
visits.show()

# COMMAND ----------

from pyspark.sql.functions import col

# Assuming df is your DataFrame named 'visits'
# Create index_visits where max_EndDate is any date before 2018-12-01
index_visits = visits.filter(col('max_EndDate') < '2018-12-01')
print(index_visits.count())

# Create outcome_visits where min_StartDate is any date on or after 2018-12-01
outcome_visits =visits
print(outcome_visits.count())

# COMMAND ----------

# Convert dates to appropriate format if necessary
index_visits = index_visits.withColumn("max_EndDate", col("max_EndDate").cast("date"))
outcome_visits = outcome_visits.withColumn("min_StartDate", col("min_StartDate").cast("date"))

# COMMAND ----------

# Perform a self-join on beneID and state
joined_df = index_visits.alias("iv").join(
    outcome_visits.alias("ov"),
    (col("iv.beneID") == col("ov.beneID")) & (col("iv.state") == col("ov.state")),
    "inner"
)

joined_df.show(200)

# COMMAND ----------

from pyspark.sql.functions import col, datediff, expr

# Filter rows where min_StartDate is 30 days or less after max_EndDate and dates are not equal
filtered_df = joined_df.filter(
    (datediff(col("ov.min_StartDate"), col("iv.max_EndDate")) <= 30) &
    (col("iv.max_EndDate") != col("ov.min_StartDate"))
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
    col("iv.days"),    
    col("iv.max_EndDate"),
    col("ov.min_StartDate"),
    # Calculate time_to_readmit
    datediff(col("ov.min_StartDate"), col("iv.max_EndDate")).alias("time_to_readmit")
).withColumn("readmission_yes", lit(1))

# Filter rows where time_to_readmit is 30 days or less and not 0
result_df = result_df.filter(
    (col("time_to_readmit") > 0) & (col("time_to_readmit") <= 30)
)

# # Select the final set of columns
# result_df = result_df.select(
#     col("beneID"),
#     col("state"),
#     col("episode"),
#     col("time_to_readmit"),
#     col("readmission_yes")
# )

# result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC REMOVE MEMBERS WITH 4 OR MORE ACUTE CARE VISITS

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, count

# Group by beneID and state and aggregate sum of max_inpatientVisit
aggregated_df = outcome_visits.groupBy("beneID", "state").agg(
    _sum("max_inpatientVisit").alias("sum_max_inpatientVisit"),
    count("max_inpatientVisit").alias("visit_count")
)

# Filter to get only those with sum_max_inpatientVisit >= 4 and visit_count >= 4
filtered_df = aggregated_df.filter(
    (col("sum_max_inpatientVisit") >= 4) &
    (col("visit_count") >= 4)
)

# Show the results
filtered_df = filtered_df.select("beneID","state","visit_count")
filtered_df.show()

# COMMAND ----------

print(index_visits.count())
index_visits = index_visits.join(filtered_df, on=['beneID','state'], how ='left')
index_visits.show()

# COMMAND ----------

# Filter out rows where visit_count is 4 or more
from pyspark.sql.functions import col

# Filter out rows where visit_count is 4 or more, but keep rows with null values in visit_count
index_visits = index_visits.filter(
    (col("visit_count") < 4) | (col("visit_count").isNull())
)

# Show the results
print(index_visits.count())
index_visits.show()

# COMMAND ----------

# MAGIC %md
# MAGIC MERGE ALL THE DATA

# COMMAND ----------

# Perform a left join between index_visits and result_df based on beneID, state, and episode
merged_df = index_visits.alias("iv").join(
    result_df.alias("rd"),
    (col("iv.beneID") == col("rd.beneID")) &
    (col("iv.state") == col("rd.state")) &
    (col("iv.episode") == col("rd.episode")),
    "left"
)

# Select relevant columns and fill NaN values in readmission_yes with 0
final_df = merged_df.select(
    col("iv.beneID"),
    col("iv.state"),
    col("iv.episode"),
    col("iv.max_EndDate"),
    # Use coalesce to fill NaN values in readmission_yes with 0
    col("rd.time_to_readmit"),
    col("rd.readmission_yes").alias("readmission_yes")
).fillna({"readmission_yes": 0})

final_df.show()

#merged_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC sensitivity analysis

# COMMAND ----------

filtered_visits = final_df.filter(F.col("readmission_yes") == 1)

# Assuming your DataFrame is named df
filtered_visits = filtered_visits.withColumn(
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
# MAGIC end sensitivity analysis

# COMMAND ----------

full = final_df

# COMMAND ----------

outcome1_value = full.groupBy("readmission_yes").count()
outcome1_value.show()

# COMMAND ----------

# Count the total number of rows in the DataFrame
total_rows = full.count()

# Calculate the percentage of total rows for each value in 'any_visit'
any_visit_percentage_df = full.groupBy("readmission_yes") \
                                  .agg(count("*").alias("count")) \
                                  .withColumn("percentage", round((col("count") / total_rows) * 100, 2))

# Show the result for any_visit
any_visit_percentage_df.show()

# COMMAND ----------

full.show()

# COMMAND ----------

full.write.saveAsTable("dua_058828_spa240.paper_4_pcr_outcome_12_months", mode='overwrite')

# COMMAND ----------

test = spark.table("dua_058828_spa240.paper_4_pcr_outcome_12_months")
print(test.count())

# COMMAND ----------

