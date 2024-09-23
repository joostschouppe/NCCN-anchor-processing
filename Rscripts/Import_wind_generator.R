## ---------------------------
##
## Script name: Import wind generator from OSM
##
## Purpose of script: Load OSM wind generators data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-27
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

data_list_id<-"484a4303-9a16-4a01-a63c-e2f50025a095"
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
features_list <- list("generator:source" = "wind")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("generator:output:electricity", "generator:type", "generator:model", "manufacturer", "manufacturer:ref","manufacturer:type",
                   "manufacturer:url","model", "rotor:diameter", "diameter", "est_height:hub", "hub:height", 
                   "height:hub", "height", "power","offshore","ref")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")



### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, alternative_overpass_server, bbox = c(2.15,49.15,7.07,51.8))
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

plot(osm_all)


# remove if power=* is missing (this is usually because that tag has been used to archived the object with a lifecycle tag) or generator:output:electricity=small_installation
osm_all <- osm_all %>%
  filter(!is.na(power)) %>%
  filter(!(generator_output_electricity=='small_installation') | is.na(generator_output_electricity))







# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function



# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.wind_generators CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.wind_generators
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
    CONSTRAINT wind_generators_pkey PRIMARY KEY (id)
  );
",paste0("
WITH 
cleaned as (SELECT
'https://osm.org/' || osm_id AS original_id,
jsonb_build_object(
    	'fre', 'éolienne',
    	'ger', 'Windkraftanlage',
    	'dut', 'windturbine',
    	'eng', 'wind turbine') as name,
jsonb_build_object(
    	'fre', 'éolienne',
    	'ger', 'Windkraftanlage',
    	'dut', 'windturbine',
    	'eng', 'wind turbine') as legend_item,
CASE WHEN name IS NULL AND short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL 
	ELSE CONCAT_WS('; ',name, short_name, official_name, alt_name, old_name) END AS names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END
	AS address,
CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_email, email) END AS local_email,
operator_email,
CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS local_phone,
CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS local_website,
operator_website,
operator_wikidata, operator, operator_type, image, 
generator_output_electricity as generator_output, 
CASE WHEN generator_type = 'horizontal_axis' THEN 'vertical rotor'
	 WHEN generator_type = 'vertical_axis' THEN 'horizontal rotor'
	 ELSE NULL END as rotor_orientation,
NULLIF(CONCAT_WS(',',manufacturer_ref,generator_model,manufacturer_type,model), '') model_info,
manufacturer,manufacturer_url, 
TRIM(TRAILING 'mM ' FROM COALESCE(rotor_diameter, diameter)) as rotor_diameter, 
NULLIF(CASE WHEN est_height_hub IS NOT NULL THEN CONCAT(est_height_hub,' (estimated)') 
	ELSE TRIM(TRAILING 'mM ' FROM CONCAT_WS(',',hub_height,hub_height,height_hub)) END, '')
	as hub_height,
height as total_height,
CASE WHEN offshore='yes' THEN 'yes' ELSE NULL END AS offshore,ref as reference,
geometry
FROM raw_data.osm_wind_generators)

INSERT INTO ingestion.wind_generators 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",data_list_id,"' as data_list_id,
3 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'generator_output', generator_output,
	'rotor_otientation', rotor_orientation,
	'rotor_diameter', rotor_diameter,
	'model_info', model_info,
	'manufacturer', manufacturer,
	'manufacturer_url', manufacturer_url,
	'hub_height', hub_height,
	'total_height', total_height,
	'offshore', offshore,
	'reference', reference,
	'names', names,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image)) as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))



### Create SQL for transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.wind_generators CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.wind_generators
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
    CONSTRAINT wind_generators_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.wind_generators
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT original_id, name, legend_item, data_list_id::uuid, risk_level, properties, geometry, created_at FROM ingestion.osm_wind_generators;
")



### Create fdw views ----
fdw_views_sql <- c("
CREATE OR REPLACE VIEW fdw.fdw_wind_generators
AS
SELECT id,
original_id,
name,
legend_item,
NULL::uuid as best_address_id,
NULL::uuid as capakey_id,
data_list_id,
risk_level,
properties,
properties_secondary,
imported_at,
tags,
deleted_at,
updated_at,
created_at,
created_by,
updated_by,
geometry,
st_pointonsurface(geometry) AS geometry_pt
FROM transformation.wind_generators;
","
ALTER TABLE fdw.fdw_wind_generators
OWNER TO paragon;
","
GRANT SELECT ON TABLE fdw.fdw_wind_generators TO fdw4dev;
","
GRANT ALL ON TABLE fdw.fdw_wind_generators TO paragon;
")

### Execute the SQL commands ----

create_ingestion_table <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in ingestion_table_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("Ingestion table SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the ingestion table failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


create_transformation_table <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in transformation_table_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("Transformation table SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the transformation table failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


create_fdw_views <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in fdw_views_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("FDW view SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the FDW views failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}

# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("wind_generators", 50, 200, 100, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_wind_generators")  
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
  #create_fdw_views()
}


if(F){
  main_function()
}

