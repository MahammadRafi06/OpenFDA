-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.device_event_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_event_raw(
adverse_event_flag	string,
date_added	string,
date_changed	string,
date_facility_aware	string,
date_manufacturer_received	string,
date_of_event	string,
date_received	string,
date_report	string,
date_report_to_fda	string,
date_report_to_manufacturer	string,
device	array<struct<brand_name:string,catalog_number:string,combination_product_flag:string,date_received:string,date_removed_flag:string,date_returned_to_manufacturer:string,device_age_text:string,device_availability:string,device_evaluated_by_manufacturer:string,device_event_key:string,device_operator:string,device_report_product_code:string,device_sequence_number:string,expiration_date_of_device:string,generic_name:string,implant_flag:string,lot_number:string,manufacturer_d_address_1:string,manufacturer_d_address_2:string,manufacturer_d_city:string,manufacturer_d_country:string,manufacturer_d_name:string,manufacturer_d_postal_code:string,manufacturer_d_state:string,manufacturer_d_zip_code:string,manufacturer_d_zip_code_ext:string,model_number:string,openfda:struct<device_class:string,device_name:string,fei_number:string,medical_specialty_description:string,registration_number:string,regulation_number:string>,other_id_number:string,udi_di:string,udi_public:string>>,
device_date_of_manufacturer	string,
distributor_address_1	string,
distributor_address_2	string,
distributor_city	string,
distributor_name	string,
distributor_state	string,
distributor_zip_code	string,
distributor_zip_code_ext	string,
event_key	string,
event_location	string,
event_type	string,
exemption_number	string,
health_professional	string,
initial_report_to_fda	string,
manufacturer_address_1	string,
manufacturer_address_2	string,
manufacturer_city	string,
manufacturer_contact_address_1	string,
manufacturer_contact_address_2	string,
manufacturer_contact_area_code	string,
manufacturer_contact_city	string,
manufacturer_contact_country	string,
manufacturer_contact_exchange	string,
manufacturer_contact_extension	string,
manufacturer_contact_f_name	string,
manufacturer_contact_l_name	string,
manufacturer_contact_pcity	string,
manufacturer_contact_pcountry	string,
manufacturer_contact_phone_number	string,
manufacturer_contact_plocal	string,
manufacturer_contact_postal_code	string,
manufacturer_contact_state	string,
manufacturer_contact_t_name	string,
manufacturer_contact_zip_code	string,
manufacturer_contact_zip_ext	string,
manufacturer_country	string,
manufacturer_g1_address_1	string,
manufacturer_g1_address_2	string,
manufacturer_g1_city	string,
manufacturer_g1_country	string,
manufacturer_g1_name	string,
manufacturer_g1_postal_code	string,
manufacturer_g1_state	string,
manufacturer_g1_zip_code	string,
manufacturer_g1_zip_code_ext	string,
manufacturer_link_flag	string,
manufacturer_name	string,
manufacturer_postal_code	string,
manufacturer_state	string,
manufacturer_zip_code	string,
manufacturer_zip_code_ext	string,
mdr_report_key	string,
mdr_text	array<struct<mdr_text_key:string,patient_sequence_number:string,text:string,text_type_code:string>>,
noe_summarized	string,
number_devices_in_event	string,
number_patients_in_event	string,
patient	array<struct<date_received:string,patient_age:string,patient_ethnicity:string,patient_race:string,patient_sequence_number:string,patient_sex:string,patient_weight:string,sequence_number_outcome:array<string>,sequence_number_treatment:array<string>>>,
pma_pmn_number	string,
previous_use_code	string,
product_problem_flag	string,
product_problems array<string>,
remedial_action	array<string>,
removal_correction_number	string,
report_date	string,
report_number	string,
report_source_code	string,
report_to_fda	string,
report_to_manufacturer	string,
reporter_country_code	string,
reporter_occupation_code	string,
reprocessed_and_reused_flag	string,
single_use_flag	string,
source_type	array<string>,
summary_report_flag	string,
suppl_dates_fda_received	string,
suppl_dates_mfr_received	string,
type_of_report	array<string>
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/event/"
)

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.device_events;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_events
AS
SELECT report_number,
event_location,
event_type,
product_problem_flag,
date_of_event,
reporter_country_code,
adverse_event_flag,
single_use_flag FROM openfda.openfdaraw.device_event_raw;

-- COMMAND ----------

---DROP TABLES CREATED WITH PYTHON
DROP TABLE IF EXISTS openfda.openfdasilver.device_events_device;
DROP TABLE IF EXISTS openfda.openfdasilver.device_events_product_problems;
DROP TABLE IF EXISTS openfda.openfdasilver.device_events_mdr;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ### Create python table for device within events
-- MAGIC from pyspark.sql.functions import explode,concat,col
-- MAGIC df = spark.sql("SELECT report_number,device FROM openfda.openfdaraw.device_event_raw").withColumn("device", explode("device.openfda")).withColumns({"device_class":col("device.device_class"),"device_name":col("device.device_name"),"medical_specialty_description":col("device.medical_specialty_description")}).drop(col("device"))
-- MAGIC df.write.mode("overwrite").saveAsTable("openfda.openfdasilver.device_events_device")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ### Create python table for product probelems within events
-- MAGIC from pyspark.sql.functions import explode,concat,col
-- MAGIC df = spark.sql("SELECT report_number,product_problems FROM openfda.openfdaraw.device_event_raw").withColumn("product_problems", explode("product_problems"))
-- MAGIC df.write.mode("overwrite").saveAsTable("openfda.openfdasilver.device_events_product_problems")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ### Create python table for mdr within events
-- MAGIC from pyspark.sql.functions import explode,concat,col
-- MAGIC df = spark.sql("SELECT report_number,mdr_text FROM openfda.openfdaraw.device_event_raw").withColumn("mdr_text", explode("mdr_text")).withColumns({"mdr_text_key":
-- MAGIC     col("mdr_text.mdr_text_key"),"text": col("mdr_text.text"),"text_type_code": col("mdr_text.text_type_code")}).drop(col("mdr_text"))
-- MAGIC df.write.mode("overwrite").saveAsTable("openfda.openfdasilver.device_events_mdr")