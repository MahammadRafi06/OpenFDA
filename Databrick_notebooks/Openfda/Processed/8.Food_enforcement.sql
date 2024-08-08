-- Databricks notebook source

USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.food_enforcement_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.food_enforcement_raw (
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
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/food/enforcement/"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.food_enforcement;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.food_enforcement
AS
SELECT * FROM openfda.openfdaraw.food_enforcement_raw;