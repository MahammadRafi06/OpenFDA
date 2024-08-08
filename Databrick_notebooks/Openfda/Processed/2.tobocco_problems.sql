-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.toboccop_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.toboccop_raw (
date_submitted	string,
nonuser_affected	string,
number_health_problems	bigint,
number_product_problems	bigint,
number_tobacco_products	bigint,
report_id	bigint,
reported_health_problems	array<string>,
reported_product_problems	array<string>,
tobacco_products	array<string>
)
USING JSON
OPTIONS (path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/tobacco/problems/")


-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.toboccop;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.toboccop
AS
SELECT 
report_id,
to_date(date_submitted, 'mm/dd/yyyy')	as submitted_date,
nonuser_affected,
number_health_problems,
number_product_problems,
number_tobacco_products
FROM openfda.openfdaraw.toboccop_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.tobocco_hlth_problems;
DROP TABLE IF EXISTS openfda.openfdasilver.tobocco_prdct_problems;
DROP TABLE IF EXISTS openfda.openfdasilver.tobocco_products;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC ####Cleaning and creating health problems table
-- MAGIC from pyspark.sql.functions import explode,col
-- MAGIC df = spark.sql("SELECT report_id, reported_health_problems FROM openfda.openfdaraw.toboccop_raw").withColumn("health_problems", explode("reported_health_problems"))
-- MAGIC df1 = df.drop(df.reported_health_problems)
-- MAGIC df1.write.saveAsTable('openfda.openfdasilver.tobocco_hlth_problems')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ####Cleaning and creating product problems table
-- MAGIC from pyspark.sql.functions import explode
-- MAGIC df = spark.sql("SELECT report_id, reported_product_problems FROM openfda.openfdaraw.toboccop_raw").withColumn("product_problems", explode("reported_product_problems"))
-- MAGIC df1= df.drop(df.reported_product_problems)
-- MAGIC df1.write.saveAsTable('openfda.openfdasilver.tobocco_prdct_problems')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ####Cleaning and creating product table
-- MAGIC from pyspark.sql.functions import explode
-- MAGIC df = spark.sql("SELECT report_id, tobacco_products FROM openfda.openfdaraw.toboccop_raw").withColumn("products", explode("tobacco_products"))
-- MAGIC df1 = df.drop(df.tobacco_products)
-- MAGIC df1.write.saveAsTable('openfda.openfdasilver.tobocco_products')