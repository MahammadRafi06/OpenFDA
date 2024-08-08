-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.other_unii_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.other_unii_raw(
substance_name	string,
unii	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/other/unii"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.other_unii;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.other_unii
AS 
SELECT *
FROM  openfda.openfdaraw.other_unii_raw;