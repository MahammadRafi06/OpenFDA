-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.drug_label_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.drug_label_raw (
active_ingredient	array<string>,
active_ingredient_table	array<string>,
ask_doctor	array<string>,
do_not_use	array<string>,
dosage_and_administration	array<string>,
dosage_and_administration_table	array<string>,
effective_time	string,
id	string,
inactive_ingredient	array<string>,
indications_and_usage	array<string>,
instructions_for_use	array<string>,
keep_out_of_reach_of_children	array<string>,
openfda	struct<application_number:array<string>,brand_name:array<string>,generic_name:array<string>,is_original_packager:array<boolean>,manufacturer_name:array<string>,nui:array<string>,package_ndc:array<string>,pharm_class_cs:array<string>,pharm_class_epc:array<string>,pharm_class_moa:array<string>,pharm_class_pe:array<string>,product_ndc:array<string>,product_type:array<string>,route:array<string>,spl_id:array<string>,spl_set_id:array<string>,substance_name:array<string>,unii:array<string>,upc:array<string>>,
other_safety_information	array<string>,
package_label_principal_display_panel	array<string>,
pregnancy_or_breast_feeding	array<string>,
purpose	array<string>,
purpose_table	array<string>,
questions	array<string>,
set_id	string,
spl_product_data_elements	array<string>,
spl_unclassified_section	array<string>,
spl_unclassified_section_table	array<string>,
stop_use	array<string>,
storage_and_handling	array<string>,
version	string,
warnings	array<string>,
when_using	array<string>
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/drug/label/"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.drug_label;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.drug_label
AS 
SELECT array_join(active_ingredient, '|') AS active_ingredient,
array_join(ask_doctor, '|') AS ask_doctor,
array_join(do_not_use, '|') AS do_not_use,
array_join(dosage_and_administration, '|') AS dosage_and_administration,
effective_time,
id,
array_join(inactive_ingredient, '|') AS inactive_ingredient,
array_join(indications_and_usage, '|') AS indications_and_usage,
array_join(instructions_for_use, '|') AS instructions_for_use,
array_join(keep_out_of_reach_of_children, '|') AS keep_out_of_reach_of_children,
array_join(other_safety_information, '|') AS other_safety_information,
array_join(package_label_principal_display_panel, '|') AS package_label_principal_display_panel,
array_join(pregnancy_or_breast_feeding, '|') AS pregnancy_or_breast_feeding,
array_join(purpose, '|') AS purpose, 
array_join(purpose_table, '|') AS purpose_table, 
array_join(questions, '|') AS questions,  
set_id,
array_join(spl_product_data_elements, '|') AS spl_product_data_elements, 
array_join(spl_unclassified_section, '|') AS spl_unclassified_section, 
array_join(spl_unclassified_section_table, '|') AS spl_unclassified_section_table,
array_join(stop_use, '|') AS stop_use,
array_join(storage_and_handling, '|') AS storage_and_handling,
version	,
array_join(warnings, '|') AS warnings,
array_join(when_using, '|') AS when_using
FROM openfda.openfdaraw.drug_label_raw;