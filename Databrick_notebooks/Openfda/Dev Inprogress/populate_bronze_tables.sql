-- Databricks notebook source
-- MAGIC %md
-- MAGIC This notebook will populate the bronze/raw tables from json files

-- COMMAND ----------

USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

--Animal and Veternary Adverse Events 
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

--Device Covid19 
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

--Device Classification
DROP TABLE IF EXISTS openfda.openfdaraw.device_classification_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_classification_raw(
definition	string,
device_class	string,
device_name	string,
gmp_exempt_flag	string,
implant_flag	string,
life_sustain_support_flag	string,
medical_specialty	string,
medical_specialty_description	string,
openfda	struct<fei_number:array<string>,k_number:array<string>,pma_number:array<string>,registration_number:array<string>>,
product_code	string,
regulation_number	string,
review_code	string,
review_panel	string,
submission_type_id	string,
summary_malfunction_reporting	string,
third_party_flag	string,
unclassified_reason	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/classification/"
)

-- COMMAND ----------

--Device Enforcement
DROP TABLE IF EXISTS openfda.openfdaraw.device_enforcement_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_enforcement_raw(
address_1	string,
address_2	string,
center_classification_date	string,
city	string,
classification	string,
code_info	string,
country	string,
distribution_pattern	string,
event_id	string,
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
voluntary_mandated	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/enforcement/"
)

-- COMMAND ----------

--Device Events
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

--Device PMA
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

--Device Recall
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

--Device Registration and Listing 
%sql
DROP TABLE IF EXISTS openfda.openfdaraw.device_registrationlisting_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_registrationlisting_raw(
  proprietary_name array<string>, 
  establishment_type array<string>,
  registration struct<registration_number:string,fei_number:string,status_code:string,initial_importer_flag:string,reg_expiry_date_year:string,name:string, address_line_1:string,address_line_2:string,city:string,state_code:string,iso_country_code:string,zip_code:string,postal_code:string,
  us_agent struct<name:string>, -- Assuming a placeholder field for demonstration
  owner_operator struct<
    firm_name:string,
    owner_operator_number:string,
    official_correspondent struct<name:string>, -- Assuming a placeholder field for demonstration
    contact_address struct<
      address_1:string,
      address_2:string,
      city:string,
      state_code:string,
      state_province:string,
      iso_country_code:string,
      postal_code:string
    >
  >
  >,
  pma_number string,
  k_number string,
  products array<struct<
    product_code:string,
    created_date:string,
    owner_operator_number:string,
    exempt:string,
    openfda:struct<
      device_name:string,
      medical_specialty_description:string,
      regulation_number:string,
      device_class:string
    >
  >>
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/registrationlisting"
)

-- COMMAND ----------

--Device UDI
%sql
DROP TABLE IF EXISTS openfda.openfdaraw.device_udi_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_udi_raw(
brand_name	string,
catalog_number	string,
commercial_distribution_end_date	timestamp,
commercial_distribution_status	string,
company_name	string,
customer_contacts	array<struct<email:string,ext:string,phone:string>>,
device_count_in_base_package	string,
device_description	string,
device_sizes	array<struct<text:string,type:string,unit:string,value:string>>,
gmdn_terms	array<struct<code:timestamp,code_status:string,definition:string,implantable:string,name:string>>,
has_donation_id_number	string,
has_expiration_date	string,
has_lot_or_batch_number	string,
has_manufacturing_date	string,
has_serial_number	string,
identifiers	array<struct<id:string,issuing_agency:string,package_discontinue_date:timestamp,package_status:string,package_type:string,quantity_per_package:string,type:string,unit_of_use_id:string>>,
is_combination_product	string,
is_direct_marking_exempt	string,
is_hct_p	string,
is_kit	string,
is_labeled_as_no_nrl	string,
is_labeled_as_nrl	string,
is_otc	string,
is_pm_exempt	string,
is_rx	string,
is_single_use	string,
labeler_duns_number	string,
mri_safety	string,
premarket_submissions	array<struct<submission_number:string,supplement_number:string>>,
product_codes	array<struct<code:string,name:string,openfda:struct<device_class:string,device_name:string,medical_specialty_description:string,regulation_number:string>>>,
public_device_record_key	string,
public_version_date	timestamp,
public_version_number	string,
public_version_status	string,
publish_date	timestamp,
record_status	string,
sterilization	struct<is_sterile:string,is_sterilization_prior_use:string,sterilization_methods:string>,
storage	array<struct<high:struct<unit:string,value:string>,low:struct<unit:string,value:string>,special_conditions:string,type:string>>,
version_or_model_number	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/udi/"
)

-- COMMAND ----------

--Device 510k
%sql
DROP TABLE IF EXISTS openfda.openfdaraw.device_510k_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.device_510k_raw(
address_1	string,
address_2	string,
advisory_committee	string,
advisory_committee_description	string,
applicant	string,
city	string,
clearance_type	string,
contact	string,
country_code	string,
date_received	timestamp,
decision_code	string,
decision_date	timestamp,
decision_description	string,
device_name	string,
expedited_review_flag	string,
k_number	string,
openfda	struct<device_class:string,device_name:string,fei_number:array<string>,medical_specialty_description:string,registration_number:array<string>,regulation_number:string>,
postal_code	string,
product_code	string,
review_advisory_committee	string,
state	string,
statement_or_summary	string,
third_party_flag	string,
zip_code	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/device/510k"
)

-- COMMAND ----------

--Other Historical Documnets 
%sql
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

-- COMMAND ----------

--Tobocco Problems
DROP TABLE IF EXISTS openfda.openfdaraw.toboccop_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.toboccop_raw (
date_submitted	string,
nonuser_affected	string,
number_health_problems	bigint,
number_product_problems	bigint,
number_tobacco_products	bigint,
report_id	bigint,
reported_health_problems	array<string>,
reported_product_problems	array<string>,
tobacco_products	array<string>
)
USING JSON
OPTIONS (path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/tobacco/problems/")


-- COMMAND ----------

--other NSDE
%sql
DROP TABLE IF EXISTS openfda.openfdaraw.other_nsde_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.other_nsde_raw(
  proprietary_name string,
  application_number_or_citation string,
  product_type string,
  marketing_start_date string,
  package_ndc string,
  reactivation_date string,
  inactivation_date string,
  marketing_category string,
  package_ndc11 string,
  dosage_form string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/other/nsde"
)

-- COMMAND ----------

--Other UNII
%sql
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

--Drug event
DROP TABLE IF EXISTS openfda.openfdaraw.drugevent_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.drugevent_raw(
  authoritynumb	string,
  companynumb	string,
  duplicate	string,
  fulfillexpeditecriteria	string,
  occurcountry	string,
  patient struct<
    drug:array<struct<
      actiondrug:string,
      activesubstance:struct<activesubstancename:string>,
      drugadditional:string,
      drugadministrationroute:string,
      drugauthorizationnumb:string,
      drugbatchnumb:string,
      drugcharacterization:string,
      drugcumulativedosagenumb:string,
      drugcumulativedosageunit:string,
      drugdosageform:string,
      drugdosagetext:string,
      drugenddate:string,
      drugenddateformat:string,
      drugindication:string,
      drugintervaldosagedefinition:string,
      drugintervaldosageunitnumb:string,
      drugrecurreadministration:string,
      drugseparatedosagenumb:string,
      drugstartdate:string,
      drugstartdateformat:string,
      drugstructuredosagenumb:string,
      drugstructuredosageunit:string,
      drugtreatmentduration:string,
      drugtreatmentdurationunit:string,
      medicinalproduct:string,
      openfda:struct<
        application_number:array<string>,
        brand_name:array<string>,
        generic_name:array<string>,
        manufacturer_name:array<string>,
        nui:array<string>,
        package_ndc:array<string>,
        pharm_class_cs:array<string>,
        pharm_class_epc:array<string>,
        pharm_class_moa:string,
        pharm_class_pe:string,
        product_ndc:array<string>,
        product_type:array<string>,
        route:array<string>,
        rxcui:array<string>,
        spl_id:array<string>,
        spl_set_id:array<string>,
        substance_name:array<string>,
        unii:array<string>
      >
    >>,
    patientagegroup:string,
    patientonsetage:string,
    patientonsetageunit:string,
    patientsex:string,
    patientweight:string,
    reaction:array<struct<
      reactionmeddrapt:string,
      reactionmeddraversionpt:string,
      reactionoutcome:string
    >>,
    summary:struct<narrativeincludeclinical:string>>,
  primarysource	struct<literaturereference:string,qualification:string,reportercountry:string>,
  primarysourcecountry	string,
  receiptdate	string,
  receiptdateformat	string,
  receivedate	string,
  receivedateformat	string,
  receiver	struct<receiverorganization:string,receivertype:string>,
  reportduplicate	string,
  reporttype	string,
  safetyreportid	string,
  safetyreportversion	string,
  sender	struct<senderorganization:string,sendertype:string>,
  serious	string,
  seriousnesscongenitalanomali	string,
  seriousnessdeath	string,
  seriousnessdisabling	string,
  seriousnesshospitalization	string,
  seriousnesslifethreatening	string,
  seriousnessother	string,
  transmissiondate	string,
  transmissiondateformat	string
  )
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/drug/event/"
)

-- COMMAND ----------

--Drug FDA
DROP TABLE IF EXISTS openfda.openfdaraw.drug_fda_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.drug_fda_raw (
application_number	string,
openfda	struct<application_number:array<string>,brand_name:array<string>,generic_name:array<string>,manufacturer_name:array<string>,nui:array<string>,package_ndc:array<string>,pharm_class_cs:array<string>,pharm_class_epc:array<string>,pharm_class_moa:array<string>,pharm_class_pe:array<string>,product_ndc:array<string>,product_type:array<string>,route:array<string>,rxcui:array<string>,spl_id:array<string>,spl_set_id:array<string>,substance_name:array<string>,unii:array<string>>,
products	array<struct<active_ingredients:array<struct<name:string,strength:string>>,brand_name:string,dosage_form:string,marketing_status:string,product_number:string,reference_drug:string,reference_standard:string,route:string,te_code:string>>,
sponsor_name	string,
submissions	array<struct<application_docs:array<struct<date:string,id:string,title:string,type:string,url:string>>,review_priority:string,submission_class_code:string,submission_class_code_description:string,submission_number:string,submission_property_type:array<struct<code:string>>,submission_public_notes:string,submission_status:string,submission_status_date:string,submission_type:string>>
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/drug/drugsfda/"
)

-- COMMAND ----------

--Drug label
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

--Drug NDC
DROP TABLE IF EXISTS openfda.openfdaraw.drug_ndc_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.drug_ndc_raw (
active_ingredients	array<struct<name:string,strength:string>>,
application_number	string,
brand_name	string,
brand_name_base	string,
dosage_form	string,
finished	boolean,
generic_name	string,
labeler_name	string,
listing_expiration_date	string,
marketing_category	string,
marketing_end_date	string,
marketing_start_date	string,
openfda	struct<is_original_packager:array<boolean>,manufacturer_name:array<string>,nui:array<string>,pharm_class_cs:array<string>,pharm_class_epc:array<string>,pharm_class_moa:array<string>,rxcui:array<string>,spl_set_id:array<string>,unii:array<string>,upc:array<string>>,
packaging	array<struct<description:string,marketing_end_date:string,marketing_start_date:string,package_ndc:string,sample:boolean>>,
pharm_class	array<string>,
product_id	string,
product_ndc	string,
product_type	string,
route	array<string>,
spl_id	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/drug/ndc/"
)

-- COMMAND ----------

--Drug Enforcement
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

--Food Enforcement
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

--Food Events
DROP TABLE IF EXISTS openfda.openfdaraw.food_event_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.food_event_raw(
consumer	struct<age:string,age_unit:string,gender:string>,
date_created	string,
date_started	string,
outcomes	array<string>,
products	array<struct<industry_code:string,industry_name:string,name_brand:string,role:string>>,
reactions	array<string>,
report_number	string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/food/event/"
)