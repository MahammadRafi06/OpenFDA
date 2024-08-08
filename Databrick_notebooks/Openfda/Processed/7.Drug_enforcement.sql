-- Databricks notebook source

USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.drug_enforcement_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.drug_enforcement_raw (
address_1	string,
address_2	string,
center_classification_date	string,
city	string,
classification	string,
code_info	string,
country	string,
distribution_pattern	string,
event_id	STRING,
initial_firm_notification	string,
more_code_info	string,
openfda	struct<application_number:array<string>,brand_name:array<string>,generic_name:array<string>,is_original_packager:array<boolean>,manufacturer_name:array<string>,nui:array<string>,original_packager_product_ndc:array<string>,package_ndc:array<string>,pharm_class_cs:array<string>,pharm_class_epc:array<string>,pharm_class_moa:array<string>,pharm_class_pe:array<string>,product_ndc:array<string>,product_type:array<string>,route:array<string>,rxcui:array<string>,spl_id:array<string>,spl_set_id:array<string>,substance_name:array<string>,unii:array<string>,upc:array<string>>,
postal_code	string,
product_description	string,
product_quantity	string,
product_type	string,
reason_for_recall	string,
recall_initiation_date	string,
recall_number	string,
recalling_firm	string,
report_date	string,
state	string,
status	string,
termination_date	string,
voluntary_mandated	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/drug/enforcement"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.drug_enforcement;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.drug_enforcement
AS
SELECT 
recall_number	, event_id,
address_1,
address_2	,
center_classification_date	,
city,
classification	,
code_info	,
country	,
distribution_pattern	,
initial_firm_notification	,
more_code_info	,
postal_code	,
product_description	,
product_quantity	,
product_type	,
reason_for_recall	,
recall_initiation_date	,
recalling_firm	,
report_date	,
state	,
status	,
termination_date	,
voluntary_mandated
FROM openfda.openfdaraw.drug_enforcement_raw;