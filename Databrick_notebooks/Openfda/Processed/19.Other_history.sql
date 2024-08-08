-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.other_hd_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.other_hd_raw(
doc_type	string,
num_of_pages	bigint,
text	string,
year	bigint
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/other/historicaldocument/"
)