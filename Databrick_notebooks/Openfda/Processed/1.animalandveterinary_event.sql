-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdaraw.ave_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.ave_raw (
reaction	array<struct<veddra_version:string,veddra_term_code:string,veddra_term_name:string,number_of_animals_affected:string,accuracy:string>>,
receiver	struct<city:string,country:string,organization:string,postal_code:string,state:string,street_address:string>,
unique_aer_id_number	string,
original_receive_date	string,
number_of_animals_affected	string,
primary_reporter	string,
number_of_animals_treated	string,
drug	array<struct<active_ingredients:array<struct<dose:struct<denominator:string,denominator_unit:string,numerator:string,numerator_unit:string>,name:string>>,administered_by:string,atc_vet_code:string,brand_name:string,dosage_form:string,dose:struct<denominator:string,denominator_unit:string,numerator:string,numerator_unit:string>,frequency_of_administration:struct<unit:string,value:string>,lot_number:string,manufacturer:struct<name:string,registration_number:string>,off_label_use:string,route:string,used_according_to_label:string>>,
health_assessment_prior_to_exposure	struct<assessed_by:string,condition:string>,
onset_date	string,
outcome	array<struct<medical_status:string,number_of_animals_affected:string>>,
animal	struct<age:struct<max:string,min:string,qualifier:string,unit:string>,breed:struct<breed_component:string,is_crossbred:string>,female_animal_physiological_status:string,gender:string,reproductive_status:string,species:string,weight:struct<max:string,min:string,qualifier:string,unit:string>>,
report_id	 string, 
time_between_exposure_and_onset	string,
type_of_information	string
)
USING JSON
OPTIONS (path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/animalandveterinary/event")


-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.ave;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.ave
AS
SELECT unique_aer_id_number as UAID,
report_id, 
health_assessment_prior_to_exposure.assessed_by as assessed_by,
health_assessment_prior_to_exposure.condition as condition,
number_of_animals_affected	as no_animals_affected,
primary_reporter,
number_of_animals_treated	as no_animals_treated,
onset_date,
original_receive_date,
time_between_exposure_and_onset	as duration_exposure_to_onset ,
type_of_information,
current_timestamp() as ingestion_date 
FROM openfda.openfdaraw.ave_raw;

-- COMMAND ----------

-- Dropping all tables created through python
DROP TABLE IF EXISTS openfda.openfdasilver.ave_outcomes;
DROP TABLE IF EXISTS openfda.openfdasilver.ave_reactions;
DROP TABLE IF EXISTS openfda.openfdasilver.ave_drugs;



-- COMMAND ----------

-- MAGIC %python
-- MAGIC ####Cleaning and creating outcomes table
-- MAGIC from pyspark.sql.functions import explode
-- MAGIC df = spark.sql("SELECT unique_aer_id_number as UAID, report_id, outcome FROM openfda.openfdaraw.ave_raw").withColumn("outcomes", explode("outcome"))
-- MAGIC df1 = df.withColumns({"medical_status":df.outcomes.medical_status,"no_animals_afctd":df.outcomes.number_of_animals_affected}).drop(df.outcome,df.outcomes)
-- MAGIC df1.write.saveAsTable('openfda.openfdasilver.ave_outcomes')

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.ave_receiver;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.ave_receiver
AS
SELECT unique_aer_id_number as UAID,
report_id, 
receiver.city as rcvr_city,
receiver.country as rcvr_country,
receiver.organization as rcvr_org,
receiver.postal_code as rcvr_zipcode,
receiver.state as rcvr_state,
receiver.street_address as rcvr_address
FROM openfda.openfdaraw.ave_raw;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ####Cleaning and creating reactions table
-- MAGIC from pyspark.sql.functions import explode
-- MAGIC df = spark.sql("SELECT unique_aer_id_number as UAID, report_id, reaction FROM openfda.openfdaraw.ave_raw").withColumn("reactions", explode("reaction"))
-- MAGIC df1 = df.withColumns({"veddra_version":df.reactions.veddra_version,"veddra_term_code":df.reactions.veddra_term_code,"veddra_term_name":df.reactions.veddra_term_name,"n0_animals_afctd":df.reactions.number_of_animals_affected,"accuracy":df.reactions.accuracy}).drop(df.reaction,df.reactions)
-- MAGIC df1.write.saveAsTable('openfda.openfdasilver.ave_reactions')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ####Cleaning and creating drug table
-- MAGIC from pyspark.sql.functions import explode,col
-- MAGIC df = spark.sql("SELECT unique_aer_id_number as UAID, report_id, drug FROM openfda.openfdaraw.ave_raw").withColumn("drugs", explode("drug"))
-- MAGIC df1 = df.withColumn("route",df.drugs.route).withColumn("dosage_form",df.drugs.dosage_form).withColumn("brand_name",df.drugs.brand_name).withColumn("administered_by",df.drugs.administered_by).withColumn("activing", df.drugs.active_ingredients).withColumn("activings", explode("activing"))
-- MAGIC df2 =df1.withColumn("activings_name", col("activings.name")).drop(df1.activings,df1.activing, df1.drugs,df1.drug)
-- MAGIC df2.write.saveAsTable('openfda.openfdasilver.ave_drugs')

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.ave_animal;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.ave_animal 
AS
SELECT unique_aer_id_number as UAID,
report_id,
CASE WHEN animal.age.unit = 'Year' THEN animal.age.min 
     WHEN animal.age.unit = 'Month' THEN round((animal.age.min)/12)
     WHEN animal.age.unit = 'Week' THEN round((animal.age.min)/52)
     WHEN animal.age.unit = 'Day' THEN round((animal.age.min)/365)
     ELSE 0 END as age_years,
animal.breed.breed_component as breed,
animal.breed.is_crossbred as is_crossbred,
animal.gender as gender,
animal.weight.min as weight_kg,
animal.reproductive_status as reproductive_status,
animal.female_animal_physiological_status as Fmale_physioloh_sts,
animal.species as species
from openfda.openfdaraw.ave_raw;