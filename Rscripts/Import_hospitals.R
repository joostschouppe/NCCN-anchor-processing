## ---------------------------
##
## Script name: OSM hospitals
##
## Purpose of script: load OSM hospitals to proto-anchors
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-16
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

data_list_id_osm<-"bd623971-34e3-4518-a220-eb8d26d757ad"
data_list_id_helipad<-"14d5bc60-ffb0-4565-b132-fc4bd9dd156d"
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

# TODO
## analyse for hospital within hospital with new cc logic?



# Goals
## Load hospitals from OSM
## Keep interesting attributes
## Check for hospital within hospital
## add emergency wards, but without creating a full anchor (pass info on to Marc to add as vector tile layer)
## add helipads as a separate layer

# EXTRACT ----
# """""""""""""""""" ----


# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list <- list("amenity"="hospital", "healthcare"="hospital")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-TRUE
# Define extra tags to use as columns for properties
extra_columns <- c("amenity","description","email","emergency","emergency:phone","fax","full_name","healthcare", "healthcare:speciality","loc_name","opening_hours:visitors","start_date")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")

### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})






# TRANSFORM ----
# """""""""""""""""" ----

# remove objects that do NOT actually have a hospital tag (this is an artifact from the site relation)
osm_all <- osm_all %>%
  filter(amenity=="hospital" | healthcare=="hospital")

# remove emergency ward entrances (these are interesting, but not actual hospitals)
osm_all <- osm_all %>%
  filter(emergency!="emergency_ward_entrance" | is.na(emergency))




# Find all hospitals within other hospitals
joined_all <- st_join(osm_all, osm_all, join = st_within)

# there is now a row for every hospital where it intersects with itself, and a row for every time it intersects with another one.
# Filter out just the ones that are within another one
joined_all <- joined_all %>%
  filter(osm_id.x != osm_id.y)
# possible st_within makes more sense
#write.csv(st_drop_geometry(joined_all), file = "c:/temp/joined_data_poly.csv")
# reviewed 17/5/2024: quite a few sites had duplicate geometries, with info spread out over them; these were remapped with most info on the outer and deletion of the inner if it wasn't a "subhospital"

# if healthcare_speciality contains psychiatry, child_psychiatry or neuropsychiatry, set risk_level=3, else riks_level=2
osm_all <- osm_all %>%
  mutate(risk_level=ifelse(grepl("psychiatry|child_psychiatry|neuropsychiatry",healthcare_speciality),3,2))


# Load helipads


# Define the list of features
features_list <-list("operator:type"="hospital")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-TRUE
# Define extra tags to use as columns for properties
extra_columns <- c("description","no:network","not:network","icao","maxweight","reservation","surface","aeroway","diameter","network")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")

### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_helipad<-download_osm_process(features_list, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

# remove non-helipad objects
osm_helipad <- osm_helipad %>%
  filter(aeroway=="helipad" | aeroway=="heliport")

# report inconsistency if icoa is empty & no:network is not empty and when not:network is not empty but icoa is also not empty
helipad_test <- osm_helipad %>%
  filter((is.na(icao) & (is.na(no_network) & is.na(not_network))) | 
         (!is.na(icao) & (!is.na(no_network) | !is.na(no_network))))
cat(paste("Inconsistencies in helipad data: ",nrow(helipad_test),"\n"))





### Create SQL for proper ingestion table ----

ingestion_table_hospital_sql <- c("
DROP TABLE IF EXISTS ingestion.hospitals CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.hospitals
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
  CONSTRAINT hospitals_pkey PRIMARY KEY (id)
);
",paste0("
WITH simplified as (
  SELECT
  CONCAT('https://osm.org/',osm_id) AS osm_id,
  CASE WHEN addr_street IS NULL THEN NULL 
	  ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END
	  AS address,
  CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	  ELSE CONCAT_WS('; ',contact_email, email,operator_email) END AS email,
  operator_email,
  CONCAT_WS('; ',emergency_phone, contact_mobile, mobile, contact_phone, phone) AS phone,
  CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL AND emergency_phone IS NULL AND contact_mobile IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2, emergency_phone, contact_mobile) END AS local_phone,
  emergency AS has_emergency_ward,
  healthcare_speciality,
  image as image_url,
jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN 'hospital' ELSE name END,
              'fre', name_fr,
              'ger', name_de,
              'dut', name_nl)) as name,
  CONCAT_WS('; ',short_name, official_name, alt_name, full_name, loc_name) AS other_names,
  CONCAT_WS('; ',opening_hours, opening_hours_visitors) as opening_hours,
  operator,
  CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS website,
	wikidata,
  geometry
  FROM raw_data.osm_hospitals)

INSERT INTO ingestion.hospitals (original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT
osm_id as original_id,
name,
JSONB_BUILD_OBJECT(
  'dut', 'ziekenhuis',
  'fre', 'hôpital',
  'ger', 'krankenhaus',
  'eng', 'hospital'
) as legend_item,
'",data_list_id_osm,"'::uuid as data_list_id,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'address', address,
  'email', email,
  'phone', phone,
  'has emergency ward', has_emergency_ward,
  'healthcare speciality', healthcare_speciality,
  'image url', image_url,
  'opening hours', opening_hours,
  'operator', operator,
  'website', website,
  'wikidata', wikidata,
  'other_names',other_names)) as properties,
geometry,
CURRENT_DATE as created_at
FROM simplified;
"))
  
                         
                         
ingestion_table_helipad_sql <- c("
DROP TABLE IF EXISTS ingestion.hospital_helipads CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.hospital_helipads
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
  CONSTRAINT hospital_helipads_pkey PRIMARY KEY (id)
);
",paste0("
WITH simplified as (
  SELECT
  CONCAT('https://osm.org/',osm_id) AS original_id,
  jsonb_strip_nulls(jsonb_build_object(
    'und', CASE WHEN name IS NULL THEN 'hospital helipad' ELSE concat(name,' (' || icao || ')') END,
    'fre', CASE WHEN name_fr IS NULL THEN NULL else concat(name_fr,' (' || icao || ')') END,
    'ger', CASE WHEN name_de IS NULL THEN NULL else concat(name_de,' (' || icao || ')') END,
    'dut', CASE WHEN name_nl IS NULL THEN NULL else concat(name_nl,' (' || icao || ')') END)) as name,
  CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
  ELSE CONCAT_WS('; ',contact_email, email,operator_email) END AS email,
  operator_email,
  CONCAT_WS('; ', contact_mobile, mobile, contact_phone, phone) AS phone,
  CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL AND contact_mobile IS NULL THEN NULL
  ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2, contact_mobile) END AS local_phone,
  opening_hours,
  operator,
  CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
  ELSE CONCAT_WS('; ',website, contact_website) END AS website,
  operator_wikidata,
  wikidata,
  icao,
  maxweight,
  reservation,
  surface,
  description,
  aeroway,diameter,network,
  0 as risk_level,
  geometry
  FROM raw_data.osm_helipad)

INSERT INTO ingestion.hospital_helipads (original_id, name, legend_item, risk_level, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
JSONB_BUILD_OBJECT(
  'dut', 'ziekenhuis helikopterplatform',
  'fre', 'héliport hospitalier',
  'ger', 'krankenhaus hubschrauberlandeplatz',
  'eng', 'hospital helipad'
) as legend_item,
risk_level,
'",data_list_id_helipad,"'::uuid as data_list_id,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'email', email,
  'operator email', operator_email,
  'phone', phone,
  'opening hours', opening_hours,
  'operator', operator,
  'website', website,
  'wikidata', wikidata,
  'icao', icao,
  'maxweight', maxweight,
  'reservation', reservation,
  'surface', surface,
  'description', description,
  'aeroway',aeroway,
  'diameter',diameter,
  'network',network
  )) as properties,
geometry,
CURRENT_DATE as created_at
FROM simplified;"))

                                 
### Create transformation table ----
transformation_table_hospitals_sql <- c("
DROP TABLE IF EXISTS transformation.hospitals CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.hospitals
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
    CONSTRAINT hospitals_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.hospitals
(id, original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id, properties, geometry, created_at FROM ingestion.hospitals;
")

transformation_table_helipads_sql <- c("
DROP TABLE IF EXISTS transformation.hospital_helipads CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.hospital_helipads
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
    CONSTRAINT hospital_helipads_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.hospital_helipads
(id, original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id, properties, geometry, created_at FROM ingestion.hospital_helipads;
")


### Create fdw views ----
fdw_views_hospitals_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_hospitals CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_hospitals
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
st_reduceprecision(geometry, 0.000001::double precision) AS geometry,
st_reduceprecision(st_pointonsurface(geometry), 0.000001::double precision) AS geometry_pt,
CASE
  WHEN st_geometrytype(geometry) = ANY (ARRAY['ST_Point'::text, 'ST_LineString'::text]) THEN st_reduceprecision(st_transform(st_buffer(st_transform(geometry, 31370), 20::double precision), 4326), 0.000001::double precision)
  ELSE geometry
  END AS geometry_pg
FROM transformation.hospitals;
","
GRANT ALL ON TABLE fdw.fdw_hospitals TO paragon;
")

fdw_views_hospital_helipads_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_hospital_helipads CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_hospital_helipads
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
st_reduceprecision(geometry, 0.000001::double precision) AS geometry,
st_reduceprecision(st_pointonsurface(geometry), 0.000001::double precision) AS geometry_pt,
CASE
  WHEN st_geometrytype(geometry) = ANY (ARRAY['ST_Point'::text, 'ST_LineString'::text]) THEN st_reduceprecision(st_transform(st_buffer(st_transform(geometry, 31370), 20::double precision), 4326), 0.000001::double precision)
  ELSE geometry
  END AS geometry_pg
FROM transformation.hospital_helipads;
","
GRANT ALL ON TABLE fdw.fdw_hospital_helipads TO paragon;")

# LOAD ----
# """""""""""""""""" ----


### Execute the SQL commands ----

### Execute the SQL commands ----
execute_sql_commands <- function(sql_commands, task_name) {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in sql_commands) {
        dbExecute(con_pg, sql_command)
      }
      print(paste(task_name, "SQL ran without error"))
    },
    error = function(err) {
      message(paste("The SQL functions for", task_name, "failed"))
      message(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}

# Now you can call this function for different tasks:

create_ingestion_table_hospitals <- function() {execute_sql_commands(ingestion_table_hospital_sql, "Hospital Ingestion table")}
create_ingestion_table_helipads <- function() {execute_sql_commands(ingestion_table_helipad_sql, "Helipad Ingestion table")}
create_transformation_table_hospitals <- function() {execute_sql_commands(transformation_table_hospitals_sql, "Hospital Transformation table")}
create_transformation_table_helipads <- function() {execute_sql_commands(transformation_table_helipads_sql, "Helipad Transformation table")}
create_fdw_views_hospitals <- function() {execute_sql_commands(fdw_views_hospitals_sql, "Hospital FDW view")}
create_fdw_views_helipads <- function() {execute_sql_commands(fdw_views_hospital_helipads_sql, "Helipad FDW view")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("hospitals", 50, 200, 100, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
  smart_update_process("hospital_helipads", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_hospitals")
  CreateImportTable(dataset = osm_helipad, schema = "raw_data", table_name = "osm_helipad")
  create_ingestion_table_hospitals()
  create_ingestion_table_helipads()
  run_smart_update()
  #create_transformation_table_hospitals()
  #create_transformation_table_helipads()
  #create_fdw_views_hospitals()
  #create_fdw_views_helipads()
}


if(F){
  main_function()
}






