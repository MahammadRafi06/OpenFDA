-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.device_udi_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_udi_raw(
brand_name	string,
catalog_number	string,
commercial_distribution_end_date	timestamp,
commercial_distribution_status	string,
company_name	string,
customer_contacts	array<struct<email:string,ext:string,phone:string>>,
device_count_in_base_package	string,
device_description	string,
device_sizes	array<struct<text:string,type:string,unit:string,value:string>>,
gmdn_terms	array<struct<code:timestamp,code_status:string,definition:string,implantable:string,name:string>>,
has_donation_id_number	string,
has_expiration_date	string,
has_lot_or_batch_number	string,
has_manufacturing_date	string,
has_serial_number	string,
identifiers	array<struct<id:string,issuing_agency:string,package_discontinue_date:timestamp,package_status:string,package_type:string,quantity_per_package:string,type:string,unit_of_use_id:string>>,
is_combination_product	string,
is_direct_marking_exempt	string,
is_hct_p	string,
is_kit	string,
is_labeled_as_no_nrl	string,
is_labeled_as_nrl	string,
is_otc	string,
is_pm_exempt	string,
is_rx	string,
is_single_use	string,
labeler_duns_number	string,
mri_safety	string,
premarket_submissions	array<struct<submission_number:string,supplement_number:string>>,
product_codes	array<struct<code:string,name:string,openfda:struct<device_class:string,device_name:string,medical_specialty_description:string,regulation_number:string>>>,
public_device_record_key	string,
public_version_date	timestamp,
public_version_number	string,
public_version_status	string,
publish_date	timestamp,
record_status	string,
sterilization	struct<is_sterile:string,is_sterilization_prior_use:string,sterilization_methods:string>,
storage	array<struct<high:struct<unit:string,value:string>,low:struct<unit:string,value:string>,special_conditions:string,type:string>>,
version_or_model_number	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/udi/"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.device_udi;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.device_udi
AS
SELECT 
public_device_record_key AS device_record_key , 
device_description AS description,
is_single_use,
is_otc,
publish_date
FROM openfda.openfdaraw.device_udi_raw;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ### Create python table for products within registrationlisting
-- MAGIC from pyspark.sql.functions import explode,concat,col
-- MAGIC df = spark.sql("SELECT public_device_record_key,product_codes FROM openfda.openfdaraw.device_udi_raw").withColumn("product_codes", explode("product_codes.openfda")).withColumns({"device_class":col("product_codes.device_class"),"device_name":col("product_codes.device_name"),"medical_specialty_description":col("product_codes.medical_specialty_description")}) .drop(col("product_codes"))
-- MAGIC df.write.mode("overwrite").saveAsTable("openfda.openfdasilver.device_udi_products")