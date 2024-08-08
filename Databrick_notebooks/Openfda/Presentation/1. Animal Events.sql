-- Databricks notebook source
DROP TABLE IF EXISTS openfda.openfdagold.ave;
CREATE TABLE IF NOT EXISTS openfda.openfdagold.ave
AS
SELECT DISTINCT
main.UAID AS Unique_ID, 
main.report_id AS Report_ID,
main.assessed_by AS Assessed_By,
main.condition AS Condition,
main.no_animals_affected AS No_Animals_Affected,
main.primary_reporter as Reporter,
main.no_animals_treated AS No_Animals_Treated,
to_date(main.onset_date) as Onset_Date,
main.type_of_information as Issue_Type,
to_date(main.ingestion_date) as Ingestion_Date,
oc.medical_status as Medical_Status,
oc.no_animals_afctd as No_of_Animal_in_Medical_Status,
d.route as Route,
d.dosage_form AS Dosage_Form,
d.administered_by AS administered_By,
d.activings_name AS ActiveIngradient_Name,
r.rcvr_country AS Reported_Country,
a.age_years AS Animal_Age,
a.breed AS Animal_Breed,
CASE 
    WHEN upper(a.is_crossbred) = 'FALSE' THEN 'NO'
    WHEN upper(a.is_crossbred) = 'TRUE' THEN 'YES'
    ELSE 'NO' 
END AS Is_Crossbreed,
CASE 
    WHEN upper(a.gender) = 'MALE' THEN 'MALE'
    WHEN upper(a.gender) = 'FEMALE' THEN 'FEMALE'
    ELSE 'UNKNOWN' 
END AS Gender,
nvl(round(a.weight_kg,2),'UNKNOWN') AS Animal_Weight_KG,
nvl(a.species,'UNKNOWN' ) AS Species
FROM openfda.openfdasilver.ave main
LEFT JOIN openfda.openfdasilver.ave_outcomes oc ON main.UAID = oc.UAID AND main.report_id = oc.report_id
LEFT JOIN openfda.openfdasilver.ave_drugs d ON main.UAID = d.UAID AND main.report_id = d.report_id
LEFT JOIN openfda.openfdasilver.ave_receiver r ON main.UAID = r.UAID AND main.report_id = r.report_id
LEFT JOIN openfda.openfdasilver.ave_animal a ON main.UAID = a.UAID AND main.report_id = a.report_id;

-- COMMAND ----------

SELECT * FROM openfda.openfdagold.ave LIMIT 20;