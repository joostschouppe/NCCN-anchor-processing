## ---------------------------
##
## Script name: Import railway & metro stations from OSM
##
## Purpose of script: Load OSM railway & metro stations data & transform into proto-anchors for Paragon
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

data_list_id<-"cd9bd5a4-f635-4c3d-b835-dc481945d4fa"
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
features_list <- list("railway" = "station")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("network:wikidata", "train", "station", "subway", 
                   "ref:STIB_MIVB","network","railway:ref","uic_ref",
                   "highspeed","ref", "railway:ref:DB", "usage")
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


ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.stations CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.stations
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
  CONSTRAINT stations_pkey PRIMARY KEY (id)
);
",paste0("WITH cleaned as (SELECT
            'https://osm.org/' || osm_id as original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', name,
                            'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
            jsonb_build_object(
              'dut', CASE WHEN station='subway' THEN 'metrostation' 
                     WHEN station IS NULL or station='railway' THEN 'treinstation' 
                     ELSE 'ander spoorstation' END,
              'fre', CASE WHEN station='subway' THEN 'gare de métro' 
                     WHEN station IS NULL or station='railway' THEN 'gare' 
                     ELSE 'autre gare' END,
              'ger', CASE WHEN station='subway' THEN 'U-Bahnstation' 
                     WHEN station IS NULL or station='railway' THEN 'Bahnhof' 
                     ELSE 'andere Bahnhof' END,
              'eng', CASE WHEN station='subway' THEN 'metro station' 
                     WHEN station IS NULL or station='railway' THEN 'train station' 
                     ELSE 'other station' END) as legend_item,
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
operator_website,operator_wikidata,
			network_wikidata, train, station, subway, ref_stib_mivb,network,railway_ref,uic_ref,highspeed,ref,railway_ref_db,
            geometry
            FROM raw_data.osm_stations
			WHERE (name IS NOT NULL)
			AND (usage IS NULL OR (usage!= 'tourism' AND usage!='leisure'))
			AND (station IS NULL OR station!= 'miniature')
		)
				
INSERT INTO ingestion.stations
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
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
'network_wikidata', network_wikidata,
'train', train,
'station', station,
'subway', subway,
'ref_stib_mivb', ref_stib_mivb,
'network', network,
'railway_ref', railway_ref,
'uic_ref', uic_ref,
'highspeed', highspeed,
'ref', ref,
'railway_ref_db', railway_ref_db
)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))
                            
### Create SQL for transformation table ----                            
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.stations CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.stations
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
    CONSTRAINT stations_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.stations 
(id, original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id::uuid, properties, geometry, created_at FROM ingestion.stations;
")
                            

### Create fdw views ----
fdw_views_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_stations CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_stations
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
FROM transformation.stations;
","
ALTER TABLE fdw.fdw_stations
OWNER TO paragon;
","
GRANT SELECT ON TABLE fdw.fdw_stations TO fdw4dev;
","
GRANT ALL ON TABLE fdw.fdw_stations TO paragon;
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
  smart_update_process("stations", 100, 150, 100, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_stations")  
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
  #create_fdw_views()
}
                            
                            
if(F){
  main_function()
}