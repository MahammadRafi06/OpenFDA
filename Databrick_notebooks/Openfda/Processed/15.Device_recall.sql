-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.device_recall_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_recall_raw(
action	string,
additional_info_contact	string,
address_1	string,
address_2	string,
cfres_id	timestamp,
city	string,
code_info	string,
distribution_pattern	string,
event_date_initiated	timestamp,
event_date_posted	timestamp,
k_numbers	array<string>,
openfda	struct<device_class:string,device_name:string,fei_number:array<string>,k_number:array<string>,medical_specialty_description:string,pma_number:array<string>,registration_number:array<string>,regulation_number:string>,
postal_code	string,
product_code	string,
product_description	string,
product_quantity	string,
product_res_number	string,
reason_for_recall	string,
recall_status	string,
recalling_firm	string,
res_event_number	string,
root_cause_description	string,
state	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/recall"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.device_recall;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.device_recall
AS
SELECT 
concat(product_res_number, product_code) product_res_id,
event_date_posted,
res_event_number,
reason_for_recall,
root_cause_description,
action,
openfda.device_name as device_name,
openfda.medical_specialty_description as category,
openfda.device_class as device_class
FROM openfda.openfdaraw.device_recall_raw;
