-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.device_covid19_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_covid19_raw(
antibody_agree	string,
antibody_truth	string,
control	string,
date_performed	string,
days_from_symptom	string,
device	string,
evaluation_id	string,
group	string,
iga_agree	string,
iga_result	string,
igg_agree	string,
igg_result	string,
igg_titer	string,
igg_truth	string,
igm_agree	string,
igm_iga_agree	string,
igm_iga_result	string,
igm_igg_agree	string,
igm_igg_result	string,
igm_result	string,
igm_titer	string,
igm_truth	string,
lot_number	string,
manufacturer	string,
pan_agree	string,
pan_result	string,
pan_titer	string,
panel	string,
sample_id	string,
sample_no	string,
type	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/covid19serology/"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.device_covid19;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.device_covid19
AS SELECT * FROM openfda.openfdaraw.device_covid19_raw;