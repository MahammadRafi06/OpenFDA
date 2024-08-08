-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;


-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.device_classification_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_classification_raw(
definition	string,
device_class	string,
device_name	string,
gmp_exempt_flag	string,
implant_flag	string,
life_sustain_support_flag	string,
medical_specialty	string,
medical_specialty_description	string,
openfda	struct<fei_number:array<string>,k_number:array<string>,pma_number:array<string>,registration_number:array<string>>,
product_code	string,
regulation_number	string,
review_code	string,
review_panel	string,
submission_type_id	string,
summary_malfunction_reporting	string,
third_party_flag	string,
unclassified_reason	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/classification/"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.device_classification;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.device_classification
AS 
SELECT 
definition	,
device_class	,
device_name	,
gmp_exempt_flag	,
implant_flag	,
life_sustain_support_flag	,
medical_specialty	,
medical_specialty_description	,
product_code	,
regulation_number	,
review_code	,
review_panel	,
submission_type_id	,
summary_malfunction_reporting	,
third_party_flag	,
unclassified_reason
FROM openfda.openfdaraw.device_classification_raw;