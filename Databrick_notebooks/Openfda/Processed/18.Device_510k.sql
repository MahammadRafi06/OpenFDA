-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.device_510k_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_510k_raw(
address_1	string,
address_2	string,
advisory_committee	string,
advisory_committee_description	string,
applicant	string,
city	string,
clearance_type	string,
contact	string,
country_code	string,
date_received	timestamp,
decision_code	string,
decision_date	timestamp,
decision_description	string,
device_name	string,
expedited_review_flag	string,
k_number	string,
openfda	struct<device_class:string,device_name:string,fei_number:array<string>,medical_specialty_description:string,registration_number:array<string>,regulation_number:string>,
postal_code	string,
product_code	string,
review_advisory_committee	string,
state	string,
statement_or_summary	string,
third_party_flag	string,
zip_code	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/510k"
)

-- COMMAND ----------

-- Below code was giving error due to memory size, I used df repartitioning

-- COMMAND ----------

-- DROP TABLE IF EXISTS openfda.openfdasilver.device_510k;
-- CREATE TABLE IF NOT EXISTS openfda.openfdasilver.device_510k
-- AS 
-- SELECT 
--    openfda.device_name AS name,
--    openfda.medical_specialty_description AS category,
--    openfda.regulation_number AS regulation_number,
--    openfda.device_class AS device_class,
--    decision_date,
--    decision_code,
--    country_code,
--    device_name,
--    decision_description,
--    clearance_type
-- FROM  openfda.openfdaraw.device_510k_raw;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC # Load the data
-- MAGIC df = spark.table("openfda.openfdaraw.device_510k_raw")
-- MAGIC
-- MAGIC # Repartition the DataFrame
-- MAGIC df_repartitioned = df.repartition(10)  # Adjust the number of partitions as needed
-- MAGIC
-- MAGIC # Perform the transformation
-- MAGIC df_transformed = df_repartitioned.select(
-- MAGIC     col("openfda.device_name").alias("name"),
-- MAGIC     col("openfda.medical_specialty_description").alias("category"),
-- MAGIC     col("openfda.regulation_number").alias("regulation_number"),
-- MAGIC     col("openfda.device_class").alias("device_class"),
-- MAGIC     "decision_date",
-- MAGIC     "decision_code",
-- MAGIC     "country_code",
-- MAGIC     "device_name",
-- MAGIC     "decision_description",
-- MAGIC     "clearance_type"
-- MAGIC )
-- MAGIC
-- MAGIC # Write the transformed DataFrame to the table
-- MAGIC df_transformed.write.mode("overwrite").saveAsTable("openfda.openfdasilver.device_510k")