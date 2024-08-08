-- Databricks notebook source

USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.food_event_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.food_event_raw(
consumer	struct<age:string,age_unit:string,gender:string>,
date_created	string,
date_started	string,
outcomes	array<string>,
products	array<struct<industry_code:string,industry_name:string,name_brand:string,role:string>>,
reactions	array<string>,
report_number	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/food/event/"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.food_events;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.food_events
AS
SELECT 
report_number,
consumer.gender as consumer_gender,
CASE WHEN consumer.age_unit = "month(s)" THEN round((consumer.age)/12)
     WHEN consumer.age_unit = "year(s)" THEN consumer.age
     WHEN consumer.age_unit = "week(s)" THEN round((consumer.age)/52)
     WHEN consumer.age_unit = "decade(s)" THEN round((consumer.age)*10)
     WHEN consumer.age_unit = "day(s)" THEN round((consumer.age)/365)
     END AS consumer_age,
date_created,
date_started
FROM openfda.openfdaraw.food_event_raw;

-- COMMAND ----------

-- Drop tables crated using python 
DROP TABLE IF EXISTS openfda.openfdasilver.food_events_outcomes;
DROP TABLE IF EXISTS openfda.openfdasilver.food_events_products;
DROP TABLE IF EXISTS openfda.openfdasilver.food_events_reactions



-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create outcomes table
-- MAGIC from pyspark.sql.functions import explode,col
-- MAGIC df = spark.sql("SELECT report_number,outcomes FROM openfda.openfdaraw.food_event_raw").withColumn("outcome", explode("outcomes")).drop("outcomes").write.mode("overwrite").saveAsTable("openfda.openfdasilver.food_events_outcomes")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create products table
-- MAGIC from pyspark.sql.functions import explode,col
-- MAGIC df = spark.sql("SELECT report_number,products FROM openfda.openfdaraw.food_event_raw").withColumn("product", explode("products")).withColumns({"industry_code":col("product.industry_code"),"industry_name":col("product.industry_name"),"name_brand":col("product.name_brand"),"role":col("product.role")}).drop(col("products"),col("product")).write.mode("overwrite").saveAsTable("openfda.openfdasilver.food_events_products")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create reactions table
-- MAGIC from pyspark.sql.functions import explode,col
-- MAGIC df = spark.sql("SELECT report_number,reactions FROM openfda.openfdaraw.food_event_raw").withColumn("reaction", explode("reactions")).drop("reactions").write.mode("overwrite").saveAsTable("openfda.openfdasilver.food_events_reactions")