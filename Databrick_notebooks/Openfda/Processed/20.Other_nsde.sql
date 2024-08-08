-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.other_nsde_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.other_nsde_raw(
  proprietary_name string,
  application_number_or_citation string,
  product_type string,
  marketing_start_date string,
  package_ndc string,
  reactivation_date string,
  inactivation_date string,
  marketing_category string,
  package_ndc11 string,
  dosage_form string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/other/nsde"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.other_nsde;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.other_nsde
AS 
SELECT *
FROM  openfda.openfdaraw.other_nsde_raw;