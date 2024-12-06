## ---------------------------
##
## Script name: Import nursery
##
## Purpose of script: Load nursing home data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-08-01
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

data_list_id<-"7db4a005-0186-4e72-8a7a-e82c2030508c"
log_folder <- "C:/temp/logs/"

### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))

# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(sf)
library(purrr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(rvest)
library(DBI)
library(RPostgres)
library(httr)
library(readxl)


# EXTRACT ----
# """""""""""""""""" ----

# This is taken care of in the Import_cobrha.R script



# LOAD ----
# """""""" ----



### Create SQL for proper ingestion table ----


ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.nursing_homes CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.nursing_homes
(
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  original_id text,  
  name jsonb,
  legend_item jsonb,
  data_list_id uuid,
  risk_level integer,
  properties jsonb,
  properties_secondary jsonb,
  imported_at timestamptz,
  tags jsonb,
  deleted_at timestamptz,
  updated_at timestamptz,
  created_at timestamptz,
  created_by uuid,
  updated_by uuid,
  geometry geometry(geometry, 4326),
  CONSTRAINT nursing_homes_pkey PRIMARY KEY (id)
);",paste0("
with nulls_cleaned AS (
  SELECT 
  NULLIF(hco_id, '') AS hco_id,
  NULLIF(cbe_id, '') AS cbe_id,
  NULLIF(hco_type_des, '') AS hco_type_des,
  NULLIF(hco_type_code, '') AS hco_type_code,
  NULLIF(as_code, '') AS as_code,
  NULLIF(hco_approval_status, '') AS hco_approval_status,
  NULLIF(hco_approval_id, '') AS hco_approval_id,
  NULLIF(some_other_id, '') AS some_other_id,
  NULLIF(hco_name_nl, '') AS hco_name_nl,
  NULLIF(hco_name_fr, '') AS hco_name_fr,
  NULLIF(hco_name_de, '') AS hco_name_de,
  NULLIF(nihii_id, '') AS nihii_id,
  NULLIF(nihii_qual_code, '') AS nihii_qual_code,
  NULLIF(nihii_sit_code, '') AS nihii_sit_code,
  NULLIF(hco_street, '') AS hco_street,
  NULLIF(hco_house_number, '') AS hco_house_number,
  NULLIF(hco_zip_code, '') AS hco_zip_code,
  NULLIF(hco_municipality, '') AS hco_municipality,
  NULLIF(hco_contact, '') AS hco_contact,
  NULLIF(site_id, '') AS site_id,
  NULLIF(site_name_nl, '') AS site_name_nl,
  NULLIF(site_name_fr, '') AS site_name_fr,
  NULLIF(site_name_de, '') AS site_name_de,	
  NULLIF(site_approval_id, '') AS site_approval_id,
  NULLIF(site_approval_status, '') AS site_approval_status,
  NULLIF(site_contact, '') AS site_contact,
  ad_hoc_id,
  NULLIF(municipality, '') AS municipality,
  NULLIF(zip_code, '') AS zip_code,
  NULLIF(street, '') AS street,
  NULLIF(house_number, '') AS house_number,
  geometry,
  ogc_fid,
  CASE 
  WHEN hco_name_nl = site_name_nl THEN hco_name_nl
  WHEN hco_name_nl != site_name_nl AND 
  (SELECT COUNT(*) FROM raw_data.ehealth_cobrha_geocoded AS sub WHERE sub.hco_name_nl = raw_data.ehealth_cobrha_geocoded.hco_name_nl) > 1
  THEN hco_name_nl || ' (' || site_name_nl || ')'
  ELSE hco_name_nl
  END AS name_nl,
  CASE 
  WHEN hco_name_fr = site_name_fr THEN hco_name_fr
  WHEN hco_name_fr != site_name_fr AND 
  (SELECT COUNT(*) FROM raw_data.ehealth_cobrha_geocoded AS sub WHERE sub.hco_name_fr = raw_data.ehealth_cobrha_geocoded.hco_name_fr) > 1
  THEN hco_name_fr || ' (' || site_name_fr || ')'
  ELSE hco_name_fr
  END AS name_fr,
  CASE 
  WHEN hco_name_de = site_name_de THEN hco_name_de
  WHEN hco_name_de != site_name_de AND 
  (SELECT COUNT(*) FROM raw_data.ehealth_cobrha_geocoded AS sub WHERE sub.hco_name_de = raw_data.ehealth_cobrha_geocoded.hco_name_de) > 1
  THEN hco_name_de || ' (' || site_name_de || ')'
  ELSE hco_name_de
  END AS name_de
  FROM raw_data.ehealth_cobrha_geocoded
  WHERE hco_approval_status != 'Ended' and site_approval_status != 'Ended'),

filtered AS (select *,
             CONCAT(hco_id,'_',cbe_id,'_',as_code,'_',hco_approval_id,'_',nihii_id,'_',site_id) as original_id,
             CASE 
             WHEN hco_type_code in ('034', '751', 'AWH_MRPA', '740', '730') THEN 1
             WHEN hco_type_code in ('038', 'CSJ_TP', '757', '756') THEN 2
             WHEN hco_type_code in ('035') THEN 3
             WHEN hco_type_code in ('968') THEN 4
             ELSE 999 END AS activity_type,
			 case when hco_type_code in ('034', '751', 'AWH_MRPA', '740', '730') then 2
				when hco_type_code in ('038', 'CSJ_TP', '757', '756') then 1
				when hco_type_code in ('035') then 3
				when hco_type_code in ('968') then 1 END
			 as risk_level
             from nulls_cleaned
             where hco_type_code in ('034', '038', '035', '751', 'AWH_MRPA', '740', '730', 'CSJ_TP', '757', '756', '968')),

spatial_grouped AS (
select 
	max(risk_level) as risk_level,
string_agg(DISTINCT activity_type::text,',') as activity_type,
string_agg(DISTINCT original_id,', ') as original_id, 
string_agg(DISTINCT hco_id,', ') as hco_id,
string_agg(DISTINCT cbe_id,', ') as cbe_id,
string_agg(DISTINCT hco_type_des,', ') as hco_type_des,
string_agg(DISTINCT hco_type_code,', ') as hco_type_code,
string_agg(DISTINCT as_code,', ') as as_code,
string_agg(DISTINCT hco_approval_status,', ') as hco_approval_status,
string_agg(DISTINCT hco_approval_id,', ') as hco_approval_id,
string_agg(DISTINCT some_other_id,', ') as some_other_id,
string_agg(DISTINCT hco_name_nl,', ') as hco_name_nl,
string_agg(DISTINCT hco_name_fr,', ') as hco_name_fr,
string_agg(DISTINCT hco_name_de,', ') as hco_name_de,
string_agg(DISTINCT nihii_id,', ') as nihii_id,
string_agg(DISTINCT nihii_qual_code,', ') as nihii_qual_code,
string_agg(DISTINCT nihii_sit_code,', ') as nihii_sit_code,
string_agg(DISTINCT hco_street,', ') as hco_street,
string_agg(DISTINCT hco_house_number,', ') as hco_house_number,
string_agg(DISTINCT hco_zip_code,', ') as hco_zip_code,
string_agg(DISTINCT hco_municipality,', ') as hco_municipality,
string_agg(DISTINCT hco_contact,', ') as hco_contact,
string_agg(DISTINCT site_id,', ') as site_id,
string_agg(DISTINCT site_name_nl,', ') as site_name_nl,
string_agg(DISTINCT site_name_fr,', ') as site_name_fr,
string_agg(DISTINCT site_name_de,', ') as site_name_de,
string_agg(DISTINCT site_approval_id,', ') as site_approval_id,
string_agg(DISTINCT site_approval_status,', ') as site_approval_status,
string_agg(DISTINCT site_contact,', ') as site_contact,
string_agg(DISTINCT ad_hoc_id::text,', ') as ad_hoc_id,
string_agg(DISTINCT municipality,', ') as municipality,
string_agg(DISTINCT zip_code,', ') as zip_code,
string_agg(DISTINCT street,', ') as street,
string_agg(DISTINCT house_number,', ') as house_number,
string_agg(DISTINCT name_nl,', ') as name_nl,
string_agg(DISTINCT name_fr,', ') as name_fr,
string_agg(DISTINCT name_de,', ') as name_de,
geometry, count(*) as count
from filtered
group by geometry),

cleaned AS (select original_id, activity_type, risk_level,
jsonb_strip_nulls(jsonb_build_object(
	'address', LTRIM(CONCAT(replace(street,',',''),' ' || house_number, ', ' || zip_code, ' ' || municipality)),
	'hco_address', LTRIM(CONCAT(replace(hco_street,',',''),' ' || hco_house_number, ', ' || hco_zip_code, ' ' || hco_municipality)),
	'kbo_bce',cbe_id,
'hco_id',hco_id,
'original_legend_item', hco_type_des,
'hco_type_code', hco_type_code,
'as_code', as_code,
'hco_approval_status',hco_approval_status,
'hco_approval_id',hco_approval_id,
'hco_name_nl',hco_name_nl,
'site_name_fr',site_name_fr,
'hco_name_fr',hco_name_fr,
'site_name_de',site_name_de,
'hco_name_de',hco_name_de,
	'nihii_id',nihii_id,
	'nihii_qual_code',nihii_qual_code,
	'nihii_sit_code',nihii_sit_code,
	'site_id',site_id,
	'site_approval_id',site_approval_id
)) as properties,
jsonb_strip_nulls(jsonb_build_object(
	'dut', NULLIF(name_nl, ''),
	'fre', NULLIF(name_fr, ''),
	'ger', NULLIF(name_de, ''))) as name,
	jsonb_build_object(
  'dut',case when activity_type='1' then 'woonzorgcentrum'
  when activity_type='2' then 'centrum voor dagverzorging'
  when activity_type='3' then 'RVT ziekenhuis'
  when activity_type='4' then 'palliatief centrum' 
	ELSE 'gecombineerd woonzorgcentrum' END,
  'fre',case when activity_type='1' then 'maison de repos'
  when activity_type='2' then 'centre de soins de jour'
  when activity_type='3' then 'MRS hôpital'
  when activity_type='4' then 'centre palliatif' 
	ELSE 'maison de repos combiné' END,
  'ger',case when activity_type='1' then 'Altenheim'
  when activity_type='2' then 'Tagespflegezentrum'
  when activity_type='3' then 'RVT Krankenhaus'
  when activity_type='4' then 'Palliativzentrum' 
	ELSE 'kombiniertes Altenheim' END
  ) as legend_item,
	geometry
from spatial_grouped)

INSERT INTO ingestion.nursing_homes 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
select original_id, 
CASE when name='{}' then legend_item else name end as name, 
legend_item, 
'",data_list_id,"' as data_list_id,
  risk_level,
	properties,
	ST_transform(geometry,4326) as geometry,
	CURRENT_DATE as created_at
from cleaned;"))

                         
                         
### Create transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.nursing_homes   CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.nursing_homes  
  (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    original_id text,    
    name jsonb,
    legend_item jsonb,
	data_list_id uuid,
	risk_level integer,
    properties jsonb,
	properties_secondary jsonb,
	imported_at timestamptz,
	tags jsonb,
	deleted_at timestamptz,
	updated_at timestamptz,
	created_at timestamptz,
	created_by uuid,
	updated_by uuid,
    geometry geometry(geometry, 4326),
    CONSTRAINT nursing_homes_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.nursing_homes 
(id, original_id, name, legend_item, data_list_id, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id, geometry, created_at FROM ingestion.nursing_homes;
","
ALTER TABLE IF EXISTS transformation.nursing_homes
OWNER to pgn_group_data_team_w;")
        
### Execute the SQL commands ----
   
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}

# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("nursing_homes", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}





# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
}


if(F){
  main_function()
}

