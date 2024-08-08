-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.drug_fda_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.drug_fda_raw (
application_number	string,
openfda	struct<application_number:array<string>,brand_name:array<string>,generic_name:array<string>,manufacturer_name:array<string>,nui:array<string>,package_ndc:array<string>,pharm_class_cs:array<string>,pharm_class_epc:array<string>,pharm_class_moa:array<string>,pharm_class_pe:array<string>,product_ndc:array<string>,product_type:array<string>,route:array<string>,rxcui:array<string>,spl_id:array<string>,spl_set_id:array<string>,substance_name:array<string>,unii:array<string>>,
products	array<struct<active_ingredients:array<struct<name:string,strength:string>>,brand_name:string,dosage_form:string,marketing_status:string,product_number:string,reference_drug:string,reference_standard:string,route:string,te_code:string>>,
sponsor_name	string,
submissions	array<struct<application_docs:array<struct<date:string,id:string,title:string,type:string,url:string>>,review_priority:string,submission_class_code:string,submission_class_code_description:string,submission_number:string,submission_property_type:array<struct<code:string>>,submission_public_notes:string,submission_status:string,submission_status_date:string,submission_type:string>>
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/drug/drugsfda/"
)

-- COMMAND ----------

---DROP TABLES CREATED WITH PYTHON
DROP TABLE IF EXISTS openfda.openfdasilver.drug_fda

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode,concat,col
-- MAGIC df = spark.sql("SELECT application_number,products FROM openfda.openfdaraw.drug_fda_raw").withColumn("product", explode("products.active_ingredients")).withColumn("activeing", explode("product")).withColumn("activeingradient",concat(col("activeing.name"),col("activeing.strength")))
-- MAGIC df.select(col("application_number"),col("activeingradient")).write.mode("overwrite").saveAsTable("openfda.openfdasilver.drug_fda")
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.drug_fda;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.drug_fda 
AS
SELECT
application_number,
sponsor_name
FROM openfda.openfdaraw.drug_fda_raw;