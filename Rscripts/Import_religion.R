## ---------------------------
##
## Script name: Import places of worship from OSM
##
## Purpose of script: Load OSM places of worship data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-28
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

data_list_id<-"45f9f964-4fe7-4966-8140-b622cb69d224"
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
features_list <- list("amenity" = "place_of_worship")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("building","place_of_worship","place_of_worship:type",
                   "religion","denomination",
                   "services","service_times",
                   "basilica","deanery","diocese","parish","historic")
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






# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.place_of_worship CASCADE;",
"CREATE TABLE IF NOT EXISTS ingestion.place_of_worship
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
  CONSTRAINT place_of_worship_pkey PRIMARY KEY (id)
);
",
paste0("WITH cleaned as (SELECT
            'https://osm.org/' || osm_id as original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN 'place of worship' ELSE name END,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
            jsonb_build_object(
              'dut', 'gebedshuis',
              'fre', 'lieu de culte',
              'ger', 'Anbetungsstätte',
              'eng', 'place of worship') as legend_item,
            CASE WHEN short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL 
	ELSE CONCAT_WS('; ',short_name, official_name, alt_name, old_name) END AS other_names,
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
			CASE WHEN building='yes' THEN null ELSE building END AS building,
CASE WHEN place_of_worship IS NULL AND place_of_worship_type IS NULL THEN NULL 
  ELSE CONCAT_WS('; ',place_of_worship,place_of_worship_type) END as place_of_worship_type,
            religion,denomination,
CASE WHEN services IS NULL AND service_times IS NULL THEN NULL
  ELSE CONCAT_WS('; ',services,service_times) END as service_time,
            basilica,deanery,diocese,parish,
            geometry
            FROM raw_data.osm_religion
			WHERE 
	(building IS NULL OR (building != 'wayside_chapel' AND building != 'wayside_cross' AND building != 'wayside_shrine' AND building != 'chapel'))
	AND NOT (name IS NULL and religion IS NULL)
	AND (place_of_worship IS NULL OR (place_of_worship!= 'wayside_chapel' AND place_of_worship!='wayside_shrine'))
	AND (place_of_worship_type IS NULL OR (place_of_worship_type != 'wayside_chapel' AND place_of_worship_type!= 'wayside_shrine' AND place_of_worship_type!= 'wayside_cross'))
	AND (historic IS NULL OR (historic != 'wayside_shrine' AND historic != 'wayside_cross'))
			)

INSERT INTO ingestion.place_of_worship 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",data_list_id,"' as data_list_id,
1 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'other_names', other_names,
  'address', address,
  	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
  'building',building,
	'place_of_worship_type',place_of_worship_type,
    'religion',religion,
	'denomination',denomination,
	'service_time',service_time,
    'basilica',basilica,
	'deanery',deanery,
	'diocese',diocese,
	'parish',parish
)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))

### ONLY IF YOU NEED TO START FROM SCRATCH - Create SQL for transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.place_of_worship CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.place_of_worship
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
    CONSTRAINT place_of_worship_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.place_of_worship 
(id, original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id::uuid, risk_level, properties, geometry, created_at FROM ingestion.place_of_worship;
")


### Create fdw views ----
fdw_views_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_place_of_worship CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_place_of_worship
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
FROM transformation.place_of_worship;
","
ALTER TABLE fdw.fdw_place_of_worship
OWNER TO paragon;
","
GRANT SELECT ON TABLE fdw.fdw_place_of_worship TO fdw4dev;
","
GRANT ALL ON TABLE fdw.fdw_place_of_worship TO paragon;
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
      print("FDW table SQL ran without error")
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
  smart_update_process("place_of_worship", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_religion")  
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
  #create_fdw_views()
}


if(F){
  main_function()
}

