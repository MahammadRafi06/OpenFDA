-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------

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

DROP TABLE IF EXISTS openfda.openfdasilver.drugevent;
CREATE TABLE IF NOT EXISTS openfda.openfdasilver.drugevent
AS
SELECT  concat(safetyreportid, safetyreportversion) as report_id,
  authoritynumb,
  companynumb,
  duplicate,
  fulfillexpeditecriteria,
  occurcountry, 
  primarysource.literaturereference as ps_lit_ref,
  primarysource.qualification as ps_qual,
  primarysource.reportercountry as ps_rpt_country,
  primarysourcecountry,
  receiptdate,
  receiptdateformat,
  receivedate,
  receivedateformat,
  receiver.receiverorganization as rcvr_org,
  receiver.receivertype as rcvr_type,
  reportduplicate,
  reporttype,
  sender.senderorganization as sender_org,
  sender.sendertype as sender_type,
  serious,
  seriousnesscongenitalanomali,
  seriousnessdeath,
  seriousnessdisabling,
  seriousnesshospitalization,
  seriousnesslifethreatening,
  seriousnessother,
  transmissiondate,
  transmissiondateformat
FROM openfda.openfdaraw.drugevent_raw;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode,monotonically_increasing_id
-- MAGIC df = spark.sql("SELECT concat(safetyreportid, safetyreportversion) as report_id, patient FROM openfda.openfdaraw.drugevent_raw")
-- MAGIC df1 = df.withColumn("drug", explode("patient.drug")).withColumn("drug_id",monotonically_increasing_id()).withColumn("reaction", explode("patient.reaction")).withColumn("reaction_id",monotonically_increasing_id())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_drug = df1.select("report_id","drug_id","drug").withColumns({"drug_id":df1.drug_id,"actiondrug":df1.drug.actiondrug,"activesubstancename":df1.drug.activesubstance.activesubstancename,"drugadditional":df1.drug.drugadditional,"drugadministrationroute":df1.drug.drugadministrationroute,"drugauthorizationnumb":df1.drug.drugauthorizationnumb,"drugbatchnumb":df1.drug.drugbatchnumb,"drugcharacterization":df1.drug.drugcharacterization,"drugcumulativedosagenumb":df1.drug.drugcumulativedosagenumb,"drugcumulativedosageunit":df1.drug.drugcumulativedosageunit,"drugdosageform":df1.drug.drugdosageform,"drugdosagetext":df1.drug.drugdosagetext,"drugenddate":df1.drug.drugenddate,"drugenddateformat":df1.drug.drugenddateformat,"drugindication":df1.drug.drugindication,"drugintervaldosagedefinition":df1.drug.drugintervaldosagedefinition,"drugintervaldosageunitnumb":df1.drug.drugintervaldosageunitnumb,"drugrecurreadministration":df1.drug.drugrecurreadministration,"drugseparatedosagenumb":df1.drug.drugseparatedosagenumb,"drugstartdate":df1.drug.drugstartdate,"drugstartdateformat":df1.drug.drugstartdateformat,"drugstructuredosagenumb":df1.drug.drugstructuredosagenumb,"drugstructuredosageunit":df1.drug.drugstructuredosageunit,"drugtreatmentduration":df1.drug.drugtreatmentduration,                         "drugtreatmentdurationunit":df1.drug.drugtreatmentdurationunit,"medicinalproduct":df1.drug.medicinalproduct,}).drop("drug").dropDuplicates()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_reaction = df1.select("report_id","reaction_id","reaction").withColumns({
-- MAGIC     "reactionmeddrapt":df1.reaction.reactionmeddrapt,
-- MAGIC     "reactionmeddraversionpt":df1.reaction.reactionmeddraversionpt,
-- MAGIC     "reactionoutcome":df1.reaction.reactionoutcome}).drop("reaction").dropDuplicates()

-- COMMAND ----------

DROP TABLE IF EXISTS openfda.openfdasilver.drugevent_drug;
DROP TABLE IF EXISTS openfda.openfdasilver.drugevent_reaction;
DROP TABLE IF EXISTS openfda.openfdasilver.drugevent_drug_ofda;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_drug_ofda = df1.select("report_id","drug_id","drug").withColumns({"nui":df1.drug.openfda.nui,"pharm_class_cs":df1.drug.openfda.pharm_class_cs,"pharm_class_epc":df1.drug.openfda.pharm_class_epc,"pharm_class_moa":df1.drug.openfda.pharm_class_moa,"pharm_class_pe":df1.drug.openfda.pharm_class_pe,"product_type":df1.drug.openfda.product_type[0],"route0":df1.drug.openfda.route[0],"route":df1.drug.openfda.route[1],"substance_name":df1.drug.openfda.substance_name,"unii":df1.drug.openfda.unii })

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_drug.write.saveAsTable("openfda.openfdasilver.drugevent_drug")
-- MAGIC df_reaction.write.saveAsTable("openfda.openfdasilver.drugevent_reaction")
-- MAGIC df_drug_ofda.write.saveAsTable("openfda.openfdasilver.drugevent_drug_ofda")