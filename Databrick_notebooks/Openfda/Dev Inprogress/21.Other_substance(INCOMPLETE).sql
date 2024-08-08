-- Databricks notebook source
USE CATALOG openfda;
USE SCHEMA openfdaraw;

-- COMMAND ----------


DROP TABLE IF EXISTS openfda.openfdaraw.other_substance_raw;
CREATE TABLE IF NOT EXISTS openfda.openfdaraw.other_substance_raw(
  codes array<struct<uuid:string,code:string,type:string,url:string,code_system:string,references:array<string>>>,
  relationships array<struct<uuid:string,type:string,related_substance:struct<uuid:string,refuuid:string,name:string,unii:string,linking_id:string,ref_pname:string,substance_class:string>>>,
  substance_class string,
  names array<struct<uuid:string,name:string,stdName:string,type:string,languages:array<string>,preferred:string,references:array<string>,display_name:string>>,
  references array<struct<uuid:string,citation:string,doc_type:string,public_domain:string,tags:array<string>>>,
  definition_type string,
  moieties array<struct<uuid:string,id:string,molfile:string,smiles:string,formula:string,atropisomerism:string,charge:string,count:string,stereochemistry:string,count_amount:struct<type:string,average:string,units:string,uuid:string>,defined_stereo:string,ez_centers:string,molecular_weight:string,optical_activity:string,stereo_centers:string>>,
  definition_level string,
  uuid string,
  version string,
  structure struct<id:string,molfile:string,smiles:string,formula:string,atropisomerism:string,charge:string,count:string,stereochemistry:string,defined_stereo:string,ez_centers:string,molecular_weight:string,optical_activity:string,references:array<string>,stereo_centers:string>,
  unii string
)
USING JSON
OPTIONS (
  path "abfss://openfda@openfdafiles.dfs.core.windows.net/cleaneddata/other/"
);

-- COMMAND ----------

SELECT * FROM openfda.openfdaraw.other_substance_raw LIMIT 10;