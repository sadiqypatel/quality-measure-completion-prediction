# Databricks notebook source
from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

outpat = spark.table("dua_058828_spa240.paper_4_otherservices2017_12_months")
inpat = spark.table("dua_058828_spa240.paper_4_inpatient2017_12_months_new")

# COMMAND ----------

print((outpat.count(), len(outpat.columns)))
print(outpat.printSchema())

# COMMAND ----------

print((inpat.count(), len(inpat.columns)))
print(inpat.printSchema())

# COMMAND ----------

outpat_selected = outpat.select("beneID", "state", "CLM_ID" ,"REV_CNTR_CD" ,"LINE_PRCDR_CD" ,"SRVC_BGN_DT" ,"SRVC_END_DT", "DGNS_CD_1", "POS_CD")
outpat_selected = outpat_selected.withColumn("inpatientVisit", lit(0))
outpat_selected.show(10)

# COMMAND ----------

inpat_selected = inpat.select("beneID", "state", "CLM_ID" ,"REV_CNTR_CD" ,"SRVC_BGN_DT", "SRVC_END_DT", "DGNS_CD_1")
inpat_selected = inpat_selected.withColumn("inpatientVisit", lit(1))
inpat_selected = inpat_selected.withColumn("EDvisit", lit(0))
print(inpat_selected.printSchema())
inpat_selected.show()

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
outpat_selected.show(1000)

# COMMAND ----------

inpatFinal = inpat_selected.select("beneID", "state", "CLM_ID","SRVC_BGN_DT" ,"SRVC_END_DT", "DGNS_CD_1", "EDvisit", "inpatientVisit")
outpatFinal = outpat_selected.select("beneID", "state", "CLM_ID","SRVC_BGN_DT" ,"SRVC_END_DT", "DGNS_CD_1", "EDvisit", "inpatientVisit")

# Show the result
inpatFinal.show(5)
outpatFinal.show(5)

# COMMAND ----------

df =  inpatFinal.union(outpatFinal)
df = df.withColumnRenamed("SRVC_BGN_DT", "StartDate").withColumnRenamed("SRVC_END_DT", "EndDate")
print(df.printSchema())

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
result_df.show(1000)

# COMMAND ----------

import pandas as pd
#ed = spark.read.csv("/Volumes/analytics/dua_058828_spa240/files/AvoidableEdVisit.csv")
ed = pd.read_csv("/Volumes/analytics/dua_058828_spa240/files/AvoidableEdVisit.csv")
ed = spark.createDataFrame(ed)
ed.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as pyspark_sum

def cleanAvoidEdFile(df):

    # Read the CSV file into a PySpark DataFrame
    avoidFile = ed

    # Calculate the 'sum' column as the sum of 'ednp', 'pct', and 'nonemergent'
    avoidFile = avoidFile.withColumn('sum', col('ednp') + col('pct') + col('nonemergent'))

    # Create the 'AvoidableEdVisit' column based on the value of the 'sum' column
    avoidFile = avoidFile.withColumn('AvoidableEdVisit', when(col('sum') > 0.5, 1).otherwise(0))

    # Select the 'dx' and 'AvoidableEdVisit' columns to create the 'avoidEdVisitFile' DataFrame
    #avoidEdVisitFile = avoidFile.select('dx', 'AvoidableEdVisit')
    avoidEdVisitFile = avoidFile.drop('sum', '_c0')
    
    #rename diagnosis column
    avoidEdVisitFile = avoidEdVisitFile.withColumnRenamed("dx", "DGNS_CD_1")

    # Print the value counts for the 'AvoidableEdVisit' column
    avoidFile.groupBy('AvoidableEdVisit').count().show()
    avoidEdVisitFile.groupBy('AvoidableEdVisit').count().show()
    
    return avoidEdVisitFile

    # Write the 'avoidEdVisitFile' DataFrame to a CSV file
    #avoidEdVisitFile.write.csv('avoidEdVisitFinal.csv', header=True)

# Call the cleanAvoidEdFile function (pass the file path as an argument if needed)
avoidEdVisitFile = cleanAvoidEdFile(ed)
avoidEdVisitFile.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as pyspark_max, greatest, when

avoidEdVisitFile1 = avoidEdVisitFile.withColumnRenamed('AvoidableEdVisit', 'avoid1') \
       .withColumn('avoid1', col('avoid1').cast('string'))

# Drop duplicates and reset index (PySpark DataFrames do not have an index)
avoidEdVisitFile1  = avoidEdVisitFile1.dropDuplicates()
avoidEdVisitFile1.show()

# Read the input DataFrame 'df' (replace this with your actual DataFrame)
df = result_df
df.show(5)

# # Left join 'df' with 'ed' based on the 'DGNS_CD_1' column
df = df.join(avoidEdVisitFile1, on='DGNS_CD_1', how='left')
df.show(5)

# # Clean the 'avoid1' column
df = df.withColumn('avoid1', when(col('avoid1').isNull(), 0)
                           .when(col('avoid1') == 'N/A', 0)
                           .when(col('avoid1') == '', 0)
                           .otherwise(col('avoid1')).cast('int'))
df.show()

# # Extract the first three characters of the 'DGNS_CD_1' column into a new column 'dx'
# df = df.withColumn('dx', col('DGNS_CD_1').substr(1, 3))
# df.show()

# # # Rename columns
# avoidEdVisitFile2 = avoidEdVisitFile.withColumnRenamed('DGNS_CD_1', 'dx').withColumnRenamed('AvoidableEdVisit', 'avoid2')

# # Convert 'avoid2' to string type
# avoidEdVisitFile2 = avoidEdVisitFile2.withColumn('avoid2', col('avoid2').cast('string'))

# # # Drop duplicates
# avoidEdVisitFile2 = avoidEdVisitFile2.dropDuplicates()
# avoidEdVisitFile2.show() 

# # # Join 'df' with 'ed' based on the 'dx' column (left join)
# df = df.join(avoidEdVisitFile2, on='dx', how='left')

# # # Clean the 'avoid2' column
# df = df.withColumn('avoid2', when(col('avoid2').isNull(), 0)
#                            .when(col('avoid2') == 'N/A', 0)
#                            .when(col('avoid2') == '', 0)
#                            .otherwise(col('avoid2')).cast('int'))

# # # Create the 'avoidableEdVisit' column as the maximum of 'avoid1' and 'avoid2'
# df = df.withColumn('avoidableEdVisit', greatest(col('avoid1'), col('avoid2')))

df = df.withColumn('avoidableEdVisit', (col('avoid1')))

# # Drop temporary columns 'dx', 'avoid1', and 'avoid2'
#df = df.drop('dx', 'avoid1', 'avoid2')
df = df.drop('dx', 'avoid1')

# # Show the result (optional)
df.show()

# COMMAND ----------

import pandas as pd
ip = spark.read.csv("/Volumes/analytics/dua_058828_spa240/files/avoidableIpVisits.csv", header=True, inferSchema=True)
ip.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Rename the column 'diagnosisOne' to 'DGNS_CD_1' in the 'pqi' DataFrame
ip = ip.withColumnRenamed('diagnosisOne', 'DGNS_CD_1')

# Drop the columns 'pqiLabel', 'icd10Label', and 'pqiNum'
ip = ip.drop('pqiLabel', 'icd10Label', 'pqiNum')

# Drop duplicates
ip = ip.dropDuplicates()

# Add a new column 'avoidableIpVisit' and set it to 1 for all rows
ip = ip.withColumn('avoidableIpVisit', lit(1))

# Left join 'ip' with 'pqi' based on the 'DGNS_CD_1' column
df = df.join(ip, on='DGNS_CD_1', how='left')

# Clean the 'avoidableIpVisit' column
df = df.withColumn('avoidableIpVisit', when(col('avoidableIpVisit').isNull(), 0)
                                      .when(col('avoidableIpVisit') == 'N/A', 0)
                                      .when(col('avoidableIpVisit') == '', 0)
                                      .otherwise(col('avoidableIpVisit')).cast('int'))

# Show the result (optional)
df.show()

# COMMAND ----------

# Define the conditions and create new columns 'allCauseEdVisit' and 'avoidEdVisit'
df = df.withColumn('allCauseEdVisit', when(col('EDvisit') == 1, 1).otherwise(0)) 
df = df.withColumn('avoidEdVisit', when((col('EDvisit') == 1) & (col('avoidableEdVisit') == 1), 1).otherwise(0))
df = df.withColumn('nonAvoidEdVisit', when((col('EDvisit') == 1) & (col('avoidableEdVisit') == 0), 1).otherwise(0))

# Define the conditions and create new columns 'allCauseIpVisit' and 'avoidIpVisit'
df = df.withColumn('allCauseIpVisit', when(col('inpatientVisit') == 1, 1).otherwise(0)) 
df = df.withColumn('avoidIpVisit', when((col('inpatientVisit') == 1) & (col('avoidableIpVisit') == 1), 1).otherwise(0))
df = df.withColumn('nonAvoidIpVisit', when((col('inpatientVisit') == 1) & (col('avoidableIpVisit') == 0), 1).otherwise(0))

# Show the result (optional)
df.show(250)

# COMMAND ----------

df = df.drop("CLM_ID","ednnp","alcohol","drug","injury","psych","unclassified")
df.show()

# COMMAND ----------

df.registerTempTable("connections")

dfAgg = spark.sql('''
SELECT beneID, state, episode, min(StartDate) as StartDate, max(EndDate) as EndDate, max(allCauseEdVisit) as allCauseEdVisit, max(avoidEdVisit) as avoidEdVisit, max(nonAvoidEdVisit) as nonAvoidEdVisit, max(allCauseIpVisit) as allCauseIpVisit, max(avoidIpVisit) as avoidIpVisit, max(nonAvoidIpVisit) as nonAvoidIpVisit
FROM connections
GROUP BY beneID, state, episode;
''')

dfAgg.show(250)

# COMMAND ----------

final = dfAgg.withColumn('allCauseIp', when(col('allCauseIpVisit') == 1, 1).otherwise(0))
final = final.withColumn('avoidIp', when(col('avoidIpVisit') == 1, 1).otherwise(0))
final = final.withColumn('nonAvoidIp', when(col('nonAvoidIpVisit') == 1, 1).otherwise(0))

# Define the conditions and create new columns 'allCauseEd' and 'avoidableEd' only for non-IP visits
final = final.withColumn('allCauseEd', when((col('allCauseIp') == 0) & (col('allCauseEdVisit') == 1), 1).otherwise(0)) 
final = final.withColumn('avoidableEd', when((col('allCauseIp') == 0) & (col('avoidEdVisit') == 1), 1).otherwise(0))
final = final.withColumn('nonAvoidableEd', when((col('allCauseIp') == 0) & (col('nonAvoidEdVisit') == 1), 1).otherwise(0))

# Show the result (optional)
final.show(250)

# COMMAND ----------

final.groupBy('avoidIp','nonAvoidIp', 'avoidableEd','nonAvoidableEd').count().show()

# COMMAND ----------

final1 = final.withColumn('non_avoid_ip', when(col('nonAvoidIp') == 1, 1).otherwise(0))
final1 = final1.withColumn('avoid_ip', when((col('avoidIp') == 1) & (col('nonAvoidIp') == 0), 1).otherwise(0))

# # Define the conditions and create new columns 'allCauseEd' and 'avoidableEd' only for non-IP visits
final1 = final1.withColumn('non_avoid_ed', when((col('allCauseIp') == 0) & (col('nonAvoidableEd') == 1), 1).otherwise(0)) 
final1 = final1.withColumn('avoid_ed', when((col('allCauseIp') == 0) & (col('nonAvoidableEd') == 0) & (col('avoidableEd') == 1), 1).otherwise(0)) 

# Show the result (optional)
final1.show(250)

# COMMAND ----------

final1.groupBy('non_avoid_ip','avoid_ip', 'non_avoid_ed','avoid_ed').count().show()

# COMMAND ----------

final2 = final1.select('beneID','state','episode','StartDate','EndDate','non_avoid_ip','avoid_ip','non_avoid_ed','avoid_ed')
final2.show()

# COMMAND ----------

# Add the column 'all_cause_ip' based on conditions
final2 = final2.withColumn('all_cause_ip', when((col('non_avoid_ip') == 1) | (col('avoid_ip') == 1), 1).otherwise(0))

# Add the column 'all_cause_ed' based on conditions
final2 = final2.withColumn('all_cause_ed', when((col('non_avoid_ed') == 1) | (col('avoid_ed') == 1), 1).otherwise(0))

# Add the column 'all_cause_acute' based on conditions
final2 = final2.withColumn('all_cause_acute', when((col('all_cause_ed') == 1) | (col('all_cause_ip') == 1), 1).otherwise(0))

# Add the column 'avoid_acute' based on conditions
final2 = final2.withColumn('avoid_acute', when((col('avoid_ip') == 1) | (col('avoid_ed') == 1), 1).otherwise(0))

# Display the updated DataFrame
final2.show()

# COMMAND ----------

# Sort the DataFrame by beneID, state, and episode
sorted_df = final2.orderBy(col("beneID"), col("state"), col("episode"))

# Display the sorted DataFrame
sorted_df.show()

# COMMAND ----------

final2.groupBy('non_avoid_ip','avoid_ip', 'non_avoid_ed','avoid_ed','all_cause_ip','all_cause_ed','all_cause_acute','avoid_acute').count().show()

# COMMAND ----------

df = spark.table("dua_058828_spa240.paper_4_final_patient_sample_2017_2019_12_months")
df = df.select("beneID","state")
print((df.count(), len(df.columns)))
print(df.printSchema())

# COMMAND ----------

# Show the final result
df.show(500)

# COMMAND ----------

final2.show

# COMMAND ----------

from pyspark.sql.functions import month, year, col, lit
from pyspark.sql import functions as F

# Assuming your DataFrame is named df and StartDate is in the format yyyy-mm-dd

# Extract the month from StartDate and create a new column service_month
acute_care = final2.withColumn('service_month', F.month(final2['StartDate']))

# Show the DataFrame to verify the new column
acute_care.show()

# COMMAND ----------

# Select the desired columns
acute_care_final = acute_care.select("beneID", "state", "episode", "StartDate", "EndDate", 'non_avoid_ip','avoid_ip', 'non_avoid_ed','avoid_ed','all_cause_ip','all_cause_ed','all_cause_acute','avoid_acute',"service_month")

# Show the final result
acute_care_final.show(500)

# COMMAND ----------

acute_care_final.registerTempTable("connections")

acute_care_agg = spark.sql('''
SELECT beneID, state, sum(non_avoid_ip) as non_avoid_ip, sum(avoid_ip) as avoid_ip, sum(non_avoid_ed) as non_avoid_ed, sum(avoid_ed) as avoid_ed, sum(all_cause_ip) as all_cause_ip, sum(all_cause_ed) as all_cause_ed, sum(all_cause_acute) as all_cause_acute, sum(avoid_acute) as avoid_acute
FROM connections
GROUP BY beneID, state;
''')

acute_care_agg.show(250)

# COMMAND ----------

member = spark.table("dua_058828_spa240.paper_4_final_patient_sample_2017_2019_12_months")
member = member.select("beneID", "state")
print((member.count(), len(member.columns)))
print(member.printSchema())

from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf

# # Left join 'df' with 'ed' based on the 'DGNS_CD_1' column
outcome_final = member.join(acute_care_agg, on=['beneID','state'], how='left').fillna(0)
outcome_final.show(200)

# COMMAND ----------

# Select the two columns of interest
selected_columns = ["all_cause_acute", "avoid_acute"]
selected_df = outcome_final.select(selected_columns)

# Compute the correlation between the two columns
correlation = selected_df.stat.corr("all_cause_acute", "avoid_acute")

# Print the correlation value
print("Correlation between 'all_cause_acute' and 'avoid_acute':", correlation)

# Measure categorical variable "all_cause_acute_post"
all_cause_acute_counts = outcome_final.groupBy("all_cause_acute").count().orderBy(col("count").desc())

# Measure categorical variable "avoid_acute_post"
avoid_acute_counts = outcome_final.groupBy("avoid_acute").count().orderBy(col("count").desc())

# Show the results
print("Counts for categorical variable 'all_cause_acute':")
all_cause_acute_counts.show(truncate=False)

print("Counts for categorical variable 'avoid_acute':")
avoid_acute_counts.show(truncate=False)

#total rows = 20,890,549

# COMMAND ----------

outcome_final.write.saveAsTable("dua_058828_spa240.paper_4_acute_care_visits", mode='overwrite') 

# COMMAND ----------

acute_care_final.write.saveAsTable("dua_058828_spa240.paper_4_all_ed_inpatient_visits", mode='overwrite') 

# COMMAND ----------

sadiq = spark.table("dua_058828_spa240.paper_4_acute_care_visits")
sadiq.show()

# COMMAND ----------

acute_care_final = spark.table("dua_058828_spa240.paper_4_all_ed_inpatient_visits")
acute_care_final.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, datediff

# Calculate inpatient days based on "all_cause_ip" column
acute_care_final = acute_care_final.withColumn("inpatient_days", when(col("all_cause_ip") == 1, datediff(col("EndDate"), col("StartDate"))).otherwise(None))

# Select the desired columns
selected_columns = ["beneID", "state", "StartDate", "EndDate", "all_cause_ip", "inpatient_days", "all_cause_acute", "avoid_acute"]
selected_df = acute_care_final.select(selected_columns)

# Show the results
selected_df.show()

# COMMAND ----------

acute_care_final.show()

# COMMAND ----------

acute_care_final.registerTempTable("connections")

acute_care_final_agg = spark.sql('''
SELECT beneID, state, sum(inpatient_days) as total_inpatient_days, sum(all_cause_acute) as total_all_cause_visits, sum(avoid_acute) as total_avoid_acute_visits
FROM connections
GROUP BY beneID, state;
''')

acute_care_final_agg = acute_care_final_agg.fillna(0, subset=["total_inpatient_days"])

# Calculate the percent_non_emergent column
acute_care_final_agg = acute_care_final_agg.withColumn("percent_non_emergent",
                                                     when(col("total_all_cause_visits") == 0, 0.0000)
                                                     .otherwise(round(col("total_avoid_acute_visits") / col("total_all_cause_visits"), 5)))

acute_care_final_agg.show(250)

# COMMAND ----------

# Select the two columns of interest
selected_columns = ["total_all_cause_visits", "total_avoid_acute_visits"]
selected_df = acute_care_final_agg.select(selected_columns)

# Compute the correlation between the two columns
correlation = selected_df.stat.corr("total_all_cause_visits", "total_avoid_acute_visits")

# Print the correlation value
print("Correlation between 'total_all_cause_visits' and 'total_avoid_acute_visits':", correlation)

# Measure categorical variable "all_cause_acute_post"
all_cause_acute_final_counts = acute_care_final_agg.groupBy("total_all_cause_visits").count().orderBy(col("count").desc())

# Measure categorical variable "avoid_acute_post"
avoid_acute_final_counts = acute_care_final_agg.groupBy("total_avoid_acute_visits").count().orderBy(col("count").desc())

# Show the results
print("Counts for categorical variable 'total_all_cause_visits':")
all_cause_acute_final_counts.show(truncate=False)

print("Counts for categorical variable 'total_avoid_acute_visits':")
avoid_acute_final_counts.show(truncate=False)

#total rows = 15309717

# COMMAND ----------

# Select the desired columns
selected_columns = ["beneID", "state", "episode", "StartDate", "service_month", "all_cause_acute", "avoid_acute"]
selected_df = acute_care_final.select(selected_columns)

# Show the results
selected_df.show()

# COMMAND ----------

# Update the columns based on conditions
updated_df = acute_care_final.withColumn("all_cause_acute_month1", when(col("service_month") == 1, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month1", when(col("service_month") == 1, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month2", when(col("service_month") == 2, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month2", when(col("service_month") == 2, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month3", when(col("service_month") == 3, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month3", when(col("service_month") == 3, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month4", when(col("service_month") == 4, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month4", when(col("service_month") == 4, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month5", when(col("service_month") == 5, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month5", when(col("service_month") == 5, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month6", when(col("service_month") == 6, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month6", when(col("service_month") == 6, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month7", when(col("service_month") == 7, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month7", when(col("service_month") == 7, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month8", when(col("service_month") == 8, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month8", when(col("service_month") == 8, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month9", when(col("service_month") == 9, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month9", when(col("service_month") == 9, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month10", when(col("service_month") == 10, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month10", when(col("service_month") == 10, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month11", when(col("service_month") == 11, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month11", when(col("service_month") == 11, col("avoid_acute")).otherwise(0))

updated_df = updated_df.withColumn("all_cause_acute_month12", when(col("service_month") == 12, col("all_cause_acute")).otherwise(0))
updated_df = updated_df.withColumn("avoid_acute_month12", when(col("service_month") == 12, col("avoid_acute")).otherwise(0))

# COMMAND ----------

# Select the desired columns
selected_columns = ["beneID", "state", "episode", "StartDate", "service_month", "all_cause_acute", "avoid_acute", "all_cause_acute_month1", "avoid_acute_month1", "all_cause_acute_month2", "avoid_acute_month2", "all_cause_acute_month3", "avoid_acute_month3"]
selected_df = updated_df.select(selected_columns)

# Show the results
selected_df.show(1000)

# COMMAND ----------

updated_df.registerTempTable("connections")

acute_care_slope_agg = spark.sql('''
SELECT beneID, state, sum(all_cause_acute_month1) as allcause_month1, sum(all_cause_acute_month2) as allcause_month2, sum(all_cause_acute_month3) as allcause_month3,
sum(all_cause_acute_month4) as allcause_month4, sum(all_cause_acute_month5) as allcause_month5, sum(all_cause_acute_month6) as allcause_month6,

sum(all_cause_acute_month7) as allcause_month7, sum(all_cause_acute_month8) as allcause_month8, sum(all_cause_acute_month9) as allcause_month9,
sum(all_cause_acute_month10) as allcause_month10, sum(all_cause_acute_month11) as allcause_month11, sum(all_cause_acute_month12) as allcause_month12,


sum(avoid_acute_month1) as avoid_month1, sum(avoid_acute_month2) as avoid_month2, sum(avoid_acute_month3) as avoid_month3,
sum(avoid_acute_month4) as avoid_month4, sum(avoid_acute_month5) as avoid_month5, sum(avoid_acute_month6) as avoid_month6,

sum(avoid_acute_month7) as avoid_month7, sum(avoid_acute_month8) as avoid_month8, sum(avoid_acute_month9) as avoid_month9,
sum(avoid_acute_month10) as avoid_month10, sum(avoid_acute_month11) as avoid_month11, sum(avoid_acute_month12) as avoid_month12

FROM connections
GROUP BY beneID, state;
''')

# COMMAND ----------

acute_care_slope_agg.show()

# COMMAND ----------

print(acute_care_slope_agg.printSchema())

# COMMAND ----------

from pyspark.sql.functions import array, col

# Assuming you have the DataFrame "acute_care_slope_agg"
column_names = ["allcause_month1", "allcause_month2", "allcause_month3", "allcause_month4", "allcause_month5", "allcause_month6", "allcause_month7", "allcause_month8", "allcause_month9", "allcause_month10", "allcause_month11", "allcause_month12"]

# Create a new column with a vector of values
test = acute_care_slope_agg.withColumn("y_allcause", array(*[col(column) for column in column_names]))
test = test.withColumn("x", array(*[lit(i) for i in range(1, 13)]))

column_names = ["avoid_month1", "avoid_month2", "avoid_month3", "avoid_month4", "avoid_month5", "avoid_month6", "avoid_month7", "avoid_month8", "avoid_month9", "avoid_month10", "avoid_month11", "avoid_month12"]
test = test.withColumn("y_avoid", array(*[col(column) for column in column_names]))

test = test.select("beneID", "state", "x", "y_allcause", "y_avoid")

test.show()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import numpy as np

# Define a UDF to calculate the slope
def get_slope_func(x, y, order=1):
    coeffs = np.polyfit(x, y, order)
    slope = coeffs[-2]
    return float(slope)

# Register the UDF
get_slope = F.udf(get_slope_func, DoubleType())

# Calculate the slope for each row using linear fit (order=1)
test = test.withColumn("allcause_slope", get_slope(F.col("x"), F.col("y_allcause"), F.lit(1)))
# Round the slope values to 6 decimals
test = test.withColumn("allcause_slope", F.round(F.col("allcause_slope"), 6))

# Calculate the slope for each row using linear fit (order=1)
test = test.withColumn("avoid_slope", get_slope(F.col("x"), F.col("y_avoid"), F.lit(1)))
test = test.withColumn("avoid_slope", F.round(F.col("avoid_slope"), 6))

# Show the results
test.show(500)

# COMMAND ----------

# # Left join 'df' with 'ed' based on the 'DGNS_CD_1' column

member = spark.table("dua_058828_spa240.paper_4_final_patient_sample_2017_2019_12_months")
member = member.select("beneID", "state")
print((member.count(), len(member.columns)))
print(member.printSchema())

# COMMAND ----------

acute_care_agg = spark.table("dua_058828_spa240.paper_4_acute_care_visits")
acute_care_agg.show()

# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count, desc, concat, expr, array, struct, expr, lit, col, concat, substring, array, explode, exp, expr, sum, round, mean, posexplode, first, udf

# # Left join 'df' with 'ed' based on the 'DGNS_CD_1' column
acute_care_predictor = member.join(test, on=['beneID','state'], how='left').fillna(0)
acute_care_predictor = acute_care_predictor.join(acute_care_agg, on=['beneID','state'], how='left').fillna(0)

acute_care_predictor.show(200)

# COMMAND ----------

print(acute_care_predictor.count())

# COMMAND ----------

acute_care_predictor.write.saveAsTable("dua_058828_spa240.paper_4_acute_care_predictors_12_months", mode='overwrite') 

# COMMAND ----------

