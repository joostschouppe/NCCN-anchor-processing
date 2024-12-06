## ---------------------------
##
## Script name: Import jails from OSM
##
## Purpose of script: Load OSM jails data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-24
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

data_list_id_jails <- "e3277b09-8cb1-491e-8fc7-3ab9ef8dbb2a"
data_list_id_asylum <- "fbc8ea91-872d-4d6d-b4dc-5eadd1e5f5a1"
log_folder <- "C:/temp/logs/"

### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))

# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(sf)
library(RPostgres)
library(DBI)

## Data processing libraries
library(dplyr)


## OSM library
library(osmdata)



# EXTRACT ----
# """""""""""""""""" ----



# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list <- list("amenity" = "prison")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("start_date", "capacity","capacity:planned","capacity:female","capacity:theoretically","prison")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, alternative_overpass_server)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})




# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function



# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.jails CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.jails
  (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    original_id text,    
    name jsonb,
    legend_item jsonb,
	data_list_id text,
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
    CONSTRAINT jails_pkey PRIMARY KEY (id)
  );
",paste0("
WITH 
cleaned as (SELECT
'https://osm.org/' || osm_id as original_id,
jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL AND prison='rejected_asylum_seekers' THEN 'closed centre for rejected asylum seekers'
						WHEN name IS NULL THEN 'jail' ELSE name END,
              'fre', name_fr,
              'ger', name_de,
              'dut', name_nl)) as name,
CASE WHEN operator_wikidata='Q1469956' THEN
jsonb_build_object(
	'dut', 'Belgische federale gevangenis',
	'fre', 'prison Belge federal',
	'ger', 'Belgisches Bundesgefängnis',
	'eng', 'Belgian federal prison') 
WHEN prison='rejected_asylum_seekers' THEN
jsonb_build_object(
	'dut', 'gesloten centrum voor uitgeprocedeerde asielzoekers',
	'fre', 'centre fermé pour demandeurs d''asile ayant épuisé tous les recours légaux',
	'ger', 'geschlossenes Zentrum für Asylbewerber, die alle Rechtsmittel ausgeschöpft haben',
	'eng', 'closed centre for rejected asylum seekers') 
ELSE jsonb_build_object(
	'dut', 'gevangenis (overige)',
	'fre', 'prison (autre)',
	'ger', 'Gefängnis (andere)',
	'eng', 'prison (other)') END
			as legend_item,
NULLIF(CONCAT_WS('; ',name, short_name, official_name, alt_name, old_name), '') AS other_names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END
	AS address,
NULLIF(CONCAT_WS('; ',contact_email, email), '') AS local_email,
operator_email,
NULLIF(CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2),'') AS local_phone,
NULLIF(CONCAT_WS('; ',website, contact_website),'') AS local_website,
operator_website,
operator_wikidata, operator, operator_type, image, 
start_date,capacity,capacity_planned,capacity_female,capacity_theoretically,prison,		
geometry
FROM raw_data.osm_jails)

INSERT INTO ingestion.jails 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
CASE WHEN prison='rejected_asylum_seekers' THEN '",data_list_id_asylum,"'
ELSE '",data_list_id_jails,"' END as data_list_id,
2 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'other_names', other_names,
	'address', address,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image,
	'start_date',start_date,
	'capacity',capacity,
	'capacity_planned',capacity_planned,
	'capacity_female',capacity_female,
	'capacity_theoretically',capacity_theoretically,
	'prison_type',prison
	)) as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))

### Create SQL for transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.jails CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.jails
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
    CONSTRAINT jails_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.jails 
(original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT original_id, name, legend_item, data_list_id::uuid, properties, geometry, created_at FROM ingestion.osm_jails;
")




### Execute the SQL commands ----


create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}
create_fdw_views <- function() {execute_sql_commands(fdw_views_sql, "FDW view")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("jails", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_jails")  
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
}


if(F){
  main_function()
}


