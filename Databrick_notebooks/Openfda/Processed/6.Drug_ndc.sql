-- Databricks notebook source

USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.drug_ndc_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.drug_ndc_raw (
active_ingredients	array<struct<name:string,strength:string>>,
application_number	string,
brand_name	string,
brand_name_base	string,
dosage_form	string,
finished	boolean,
generic_name	string,
labeler_name	string,
listing_expiration_date	string,
marketing_category	string,
marketing_end_date	string,
marketing_start_date	string,
openfda	struct<is_original_packager:array<boolean>,manufacturer_name:array<string>,nui:array<string>,pharm_class_cs:array<string>,pharm_class_epc:array<string>,pharm_class_moa:array<string>,rxcui:array<string>,spl_set_id:array<string>,unii:array<string>,upc:array<string>>,
packaging	array<struct<description:string,marketing_end_date:string,marketing_start_date:string,package_ndc:string,sample:boolean>>,
pharm_class	array<string>,
product_id	string,
product_ndc	string,
product_type	string,
route	array<string>,
spl_id	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/drug/ndc/"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.drug_ndc;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.drug_ndc
AS
SELECT
product_ndc,
generic_name,
dosage_form,
product_type
FROM openfda.openfdaraw.drug_ndc_raw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.drug_ndc_active_ingredients;
DROP TABLE IF EXISTS openfda.openfdasilver.drug_ndc_route;
DROP TABLE IF EXISTS openfda.openfdasilver.drug_ndc_pharm_class;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create ACTIVE INGRAIDENTS table
-- MAGIC from pyspark.sql.functions import explode,col
-- MAGIC df = spark.sql("SELECT product_ndc,active_ingredients FROM openfda.openfdaraw.drug_ndc_raw").withColumn("active_ingredients", explode("active_ingredients")).drop("active_ingredients").write.mode("overwrite").saveAsTable("openfda.openfdasilver.drug_ndc_active_ingredients")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create route table
-- MAGIC from pyspark.sql.functions import explode,col
-- MAGIC df = spark.sql("SELECT product_ndc,route FROM openfda.openfdaraw.drug_ndc_raw").withColumn("route", explode("route")).drop("route").write.mode("overwrite").saveAsTable("openfda.openfdasilver.drug_ndc_route")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Create pharm_class table
-- MAGIC from pyspark.sql.functions import explode,col
-- MAGIC df = spark.sql("SELECT product_ndc,pharm_class FROM openfda.openfdaraw.drug_ndc_raw").withColumn("pharm_class", explode("pharm_class")).drop("pharm_class").write.mode("overwrite").saveAsTable("openfda.openfdasilver.drug_ndc_pharm_class")