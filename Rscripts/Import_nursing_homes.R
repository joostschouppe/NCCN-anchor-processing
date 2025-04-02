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


#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

# connection details
db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

# run status
run_status<-Sys.getenv("RUN_STATUS")
## this is set to false and prevents any accidental changes to the database by switching off the main_function(). On Airflow, this is set to true.
run_status<-ifelse(tolower(run_status) == "true", TRUE, FALSE)

# overrule the checks
overrule_checks<-Sys.getenv("OVERRULE_CHECKS")
## Set to FALSE by default. That means we do not update the anchors if some tests fail. Those tests include "the data has grown or shrunk by a lot of objects". If, after review of the log, you decide that nothing is wrong, set this manually to TRUE.
# If the input is not correctly understood as boolean, this will force it to it.
overrule_checks<-ifelse(tolower(overrule_checks) == "true", TRUE, FALSE)

# Do not run the main part of the processing, but just do an update based on the ingestion table already in the dbase
reuse_ingestion_data<-Sys.getenv("REUSE_INGESTION_DATA")
reuse_ingestion_data<-ifelse(tolower(reuse_ingestion_data) == "true", TRUE, FALSE)

# Only run the comparison script & update the ingestion table, but do not attempt to update the transformation table
do_dry_run<-Sys.getenv("DO_DRY_RUN")
do_dry_run<-ifelse(tolower(do_dry_run) == "true", TRUE, FALSE)

# Set data list id
data_list_id<-"7db4a005-0186-4e72-8a7a-e82c2030508c"

# Set log folder
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------
rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))


# Libraries -------------------------------
# """""""""""""""""" ----------------------

# all are loaded via the utils scripts



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
  NULLIF(cbe_id::text, '') AS cbe_id,
  NULLIF(hco_type_des, '') AS hco_type_des,
  NULLIF(hco_type_code, '') AS hco_type_code,
  NULLIF(as_code, '') AS as_code,
  NULLIF(hco_approval_status, '') AS hco_approval_status,
  NULLIF(hco_approval_id, '') AS hco_approval_id,
  NULLIF(hco_name_nl, '') AS hco_name_nl,
  NULLIF(hco_name_fr, '') AS hco_name_fr,
  NULLIF(hco_name_de::text, '') AS hco_name_de,
  NULLIF(nihii_id::text, '') AS nihii_id,
  NULLIF(nihii_qual_code::text, '') AS nihii_qual_code,
  NULLIF(nihii_sit_code, '') AS nihii_sit_code,
  NULLIF(hco_street, '') AS hco_street,
  NULLIF(hco_house_number, '') AS hco_house_number,
  NULLIF(hco_zip_code::text, '') AS hco_zip_code,
  NULLIF(hco_municipality, '') AS hco_municipality,
  NULLIF(site_id::text, '') AS site_id,
  NULLIF(site_name_nl, '') AS site_name_nl,
  NULLIF(site_name_fr, '') AS site_name_fr,
  NULLIF(site_name_de::text, '') AS site_name_de,	
  NULLIF(site_approval_id, '') AS site_approval_id,
  NULLIF(site_approval_status, '') AS site_approval_status,
  ad_hoc_id,
  NULLIF(regexp_replace(hco_contact, '.*Fax:([^|]+).*', '\1', 'g'), hco_contact) AS operator_fax,
  NULLIF(regexp_replace(site_contact, '.*Fax:([^|]+).*', '\1', 'g'), site_contact) AS local_fax,
  NULLIF(regexp_replace(hco_contact, '.*Mail:([^|]+).*', '\1', 'g'), hco_contact) AS operator_email,
  NULLIF(regexp_replace(site_contact, '.*Mail:([^|]+).*', '\1', 'g'), site_contact) AS local_email,
  NULLIF(regexp_replace(hco_contact, '.*Phone:([^|]+).*', '\1', 'g'), hco_contact) AS operator_phone,
  NULLIF(regexp_replace(site_contact, '.*Phone:([^|]+).*', '\1', 'g'), site_contact) AS local_phone,
  NULLIF(regexp_replace(hco_contact, '.*Url:([^|]+).*', '\1', 'gi'), hco_contact) AS operator_website,
  NULLIF(regexp_replace(site_contact, '.*Url:([^|]+).*', '\1', 'gi'), site_contact) AS local_website,
  NULLIF(municipality, '') AS municipality,
  NULLIF(zip_code::text, '') AS zip_code,
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
  WHEN hco_name_de::text = site_name_de THEN hco_name_de::text
  WHEN hco_name_de::text != site_name_de AND 
  (SELECT COUNT(*) FROM raw_data.ehealth_cobrha_geocoded AS sub WHERE sub.hco_name_de = raw_data.ehealth_cobrha_geocoded.hco_name_de) > 1
  THEN hco_name_de::text || ' (' || site_name_de::text || ')'
  ELSE hco_name_de::text
  END AS name_de
  FROM raw_data.ehealth_cobrha_geocoded
  WHERE (hco_approval_status != 'Ended' OR hco_approval_status IS NULL) AND (site_approval_status != 'Ended' OR site_approval_status IS NULL)),

filtered AS (select cbe_id,hco_id,street,as_code,name_de,name_fr,name_nl,ogc_fid,site_id,geometry,nihii_id,zip_code,ad_hoc_id,hco_street,hco_name_de,hco_name_fr,hco_name_nl,hco_type_des,hco_zip_code,house_number,municipality,site_name_de,site_name_fr,site_name_nl,hco_type_code,nihii_sit_code,hco_approval_id,nihii_qual_code,hco_house_number,hco_municipality,site_approval_id,hco_approval_status,site_approval_status,
             	CASE WHEN operator_fax=local_fax OR operator_fax='- ' THEN NULL ELSE operator_fax END AS operator_fax,
	            CASE WHEN local_fax='- ' THEN NULL ELSE local_fax END AS local_fax,
	            CASE WHEN operator_email=local_email OR operator_email='- ' THEN NULL ELSE operator_email END AS operator_email,
	            CASE WHEN local_email='- ' THEN NULL ELSE local_email END AS local_email,
	            CASE WHEN operator_phone=local_phone OR operator_phone='- ' THEN NULL ELSE operator_phone END AS operator_phone,
	            CASE WHEN local_phone='- ' THEN NULL ELSE local_phone END AS local_phone,
	            CASE WHEN operator_website=local_website OR operator_website='- ' THEN NULL ELSE operator_website END AS operator_website,
	            CASE WHEN local_website='- ' THEN NULL ELSE local_website END AS local_website,
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
string_agg(DISTINCT site_id,', ') as site_id,
string_agg(DISTINCT site_name_nl,', ') as site_name_nl,
string_agg(DISTINCT site_name_fr,', ') as site_name_fr,
string_agg(DISTINCT site_name_de,', ') as site_name_de,
string_agg(DISTINCT site_approval_id,', ') as site_approval_id,
string_agg(DISTINCT site_approval_status,', ') as site_approval_status,
string_agg(DISTINCT ad_hoc_id::text,', ') as ad_hoc_id,
string_agg(DISTINCT municipality,', ') as municipality,
string_agg(DISTINCT zip_code,', ') as zip_code,
string_agg(DISTINCT street,', ') as street,
string_agg(DISTINCT house_number,', ') as house_number,
string_agg(DISTINCT name_nl,', ') as name_nl,
string_agg(DISTINCT name_fr,', ') as name_fr,
string_agg(DISTINCT name_de,', ') as name_de,
string_agg(DISTINCT operator_fax,', ') as operator_fax,
string_agg(DISTINCT operator_email,', ') as operator_email,
string_agg(DISTINCT operator_phone,', ') as operator_phone,
string_agg(DISTINCT operator_website,', ') as operator_website,
string_agg(DISTINCT local_fax,', ') as local_fax,
string_agg(DISTINCT local_email,', ') as local_email,
string_agg(DISTINCT local_phone,', ') as local_phone,
string_agg(DISTINCT local_website,', ') as local_website,
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
'hco_name_fr',hco_name_fr,
'hco_name_nl',hco_name_nl,
'hco_name_de',hco_name_de,
'site_name_fr',site_name_fr,
'site_name_de',site_name_de,
'site_name_nl',site_name_nl,
	'nihii_id',nihii_id,
	'nihii_qual_code',nihii_qual_code,
	'nihii_sit_code',nihii_sit_code,
	'site_id',site_id,
	'site_approval_id',site_approval_id,
	'operator_fax',operator_fax,
	'operator_email',operator_email,
	'operator_phone',operator_phone,
	'operator_website',operator_website,
	'local_fax',local_fax,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website
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

run_smart_update = function() {
  smart_update_process("nursing_homes", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    create_ingestion_table()
  }
  run_smart_update()
  #create_transformation_table()
}


if(run_status){
  main_function()
}

