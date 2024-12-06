## ---------------------------
##
## Script name: Import civil protection from OSM
##
## Purpose of script: Load OSM civil protection sites data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-11-13
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

data_list_id<-"0154689a-078a-4b65-9b07-3e2476a31e12"
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
features_list <- list("emergency"="disaster_response")

# Define extra tags to use as columns for properties
extra_columns <- c("ref:thw")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")



### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE)
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

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.emergency_response CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.emergency_response
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
    CONSTRAINT emergency_response_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
'https://osm.org/' || osm_id AS original_id,
jsonb_build_object(
      'und', CASE WHEN name IS NULL THEN 'civil protection' ELSE name END,
    	'fre', name_fr,
      'ger', name_de::text,
      'dut', name_nl) as name,
jsonb_build_object(
    	'fre', 'protection civile',
    	'ger', 'Zivilschutz',
    	'dut', 'civiele bescherming',
    	'eng', 'civil protection') as legend_item,
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
geometry
FROM raw_data.osm_emergency_response)

INSERT INTO ingestion.emergency_response 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",data_list_id,"' as data_list_id,
0 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'other_names', other_names,
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
DROP TABLE IF EXISTS transformation.emergency_response CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.emergency_response
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
    CONSTRAINT emergency_response_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.emergency_response
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT original_id, name, legend_item, data_list_id::uuid, risk_level, properties, geometry, created_at FROM ingestion.emergency_response;
")



### Create fdw views ----
fdw_views_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_emergency_response;",
"CREATE OR REPLACE VIEW fdw.fdw_emergency_response
 AS
 SELECT id,
    row_number() OVER () AS gid,
    original_id,
    name,
    legend_item,
    NULL::uuid AS best_address_id,
    NULL::uuid AS capakey_id,
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
        END AS geometry_pg,
    st_reduceprecision(st_simplifypreservetopology(geometry, ln(st_area(geometry) + 1::double precision) * 0.0025::double precision), 0.000001::double precision) AS geometry_s
   FROM transformation.emergency_response
  WHERE st_within(geometry, ( SELECT anchor_spatial_filter.geometry
           FROM raw_data.anchor_spatial_filter));",
"ALTER TABLE fdw.fdw_emergency_response
    OWNER TO pgn_group_data_team_w;",
"GRANT SELECT ON TABLE fdw.fdw_emergency_response TO pgn_group_acces2curation;",
"GRANT ALL ON TABLE fdw.fdw_emergency_response TO pgn_group_data_team_w;",
"GRANT SELECT ON TABLE fdw.fdw_emergency_response TO pgn_user_vectortiles;"
)

### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}
create_fdw_views <- function() {execute_sql_commands(fdw_views_sql, "FDW view")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("emergency_response", 50, 200, 100, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_emergency_response")  
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
  #create_fdw_views()
}


if(F){
  main_function()
}

