-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.device_registrationlisting_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_registrationlisting_raw(
  proprietary_name array<string>, 
  establishment_type array<string>,
  registration struct<registration_number:string,fei_number:string,status_code:string,initial_importer_flag:string,reg_expiry_date_year:string,name:string, address_line_1:string,address_line_2:string,city:string,state_code:string,iso_country_code:string,zip_code:string,postal_code:string,
  us_agent struct<name:string>, -- Assuming a placeholder field for demonstration
  owner_operator struct<
    firm_name:string,
    owner_operator_number:string,
    official_correspondent struct<name:string>, -- Assuming a placeholder field for demonstration
    contact_address struct<
      address_1:string,
      address_2:string,
      city:string,
      state_code:string,
      state_province:string,
      iso_country_code:string,
      postal_code:string
    >
  >
  >,
  pma_number string,
  k_number string,
  products array<struct<
    product_code:string,
    created_date:string,
    owner_operator_number:string,
    exempt:string,
    openfda:struct<
      device_name:string,
      medical_specialty_description:string,
      regulation_number:string,
      device_class:string
    >
  >>
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/registrationlisting"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.device_registrationlisting;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.device_registrationlisting
AS
SELECT 
registration.registration_number AS registration_number , 
registration.name AS name,
registration.iso_country_code AS country
FROM openfda.openfdaraw.device_registrationlisting_raw;

-- COMMAND ----------


---DROP TABLES CREATED WITH PYTHON
DROP TABLE IF EXISTS openfda.openfdasilver.device_rl_products;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ### Create python table for products within registrationlisting
-- MAGIC from pyspark.sql.functions import explode,concat,col
-- MAGIC df = spark.sql("SELECT registration.registration_number,products FROM openfda.openfdaraw.device_registrationlisting_raw").withColumn("products", explode("products.openfda")).withColumns({"device_class":col("products.device_class"),"device_name":col("products.device_name"),"medical_specialty_description":col("products.medical_specialty_description")}) .drop(col("products"))
-- MAGIC df.write.mode("overwrite").saveAsTable("openfda.openfdasilver.device_rl_products")