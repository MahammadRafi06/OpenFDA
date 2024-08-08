-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.device_pma_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_pma_raw(
advisory_committee	string,
advisory_committee_description	string,
ao_statement	string,
applicant	string,
city	string,
date_received	timestamp,
decision_code	string,
decision_date	timestamp,
docket_number	string,
expedited_review_flag	string,
fed_reg_notice_date	timestamp,
generic_name	string,
openfda	struct<device_class:string,device_name:string,fei_number:array<string>,medical_specialty_description:string,registration_number:array<string>,regulation_number:string>,
pma_number	string,
product_code	string,
state	string,
street_1	string,
street_2	string,
supplement_number	string,
supplement_reason	string,
supplement_type	string,
trade_name	string,
zip	string,
zip_ext	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/pma"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.device_pma;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.device_pma
AS
SELECT 
concat(pma_number,supplement_number ) AS pma_number, 
openfda.device_name as device_name,
openfda.medical_specialty_description as category,
openfda.device_class as device_class ,
trade_name,
decision_date,
date_received,
decision_code
FROM openfda.openfdaraw.device_pma_raw;