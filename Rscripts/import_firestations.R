## ---------------------------
##
## Script name: ETL flow for firestations
##
## Purpose of script: Load OSM firestation data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-20
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

data_list_id<-"b3f833af-1c7a-4d03-8ceb-ffd55bfea5f8"
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

## Wikidata library
library(WikidataQueryServiceR)


# EXTRACT ----
# """""""""""""""""" ----


# Download wikidata ----
### make a query (start at https://query.wikidata.org/querybuilder/?uselang=nl and use the "show query in the query service" interface)
sparql_query <- "SELECT DISTINCT ?zone ?nl ?fr ?de ?website ?phone ?email ?kbo_bce WHERE {
  ?zone wdt:P31 wd:Q3575878.
  OPTIONAL { ?zone wdt:P856 ?website. }
  OPTIONAL { ?zone wdt:P1329 ?phone. }
  OPTIONAL { ?zone wdt:P968 ?email. }
  OPTIONAL { ?zone wdt:P3376 ?kbo_bce. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"nl\". ?zone rdfs:label ?nl. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"fr\". ?zone rdfs:label ?fr. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"de\". ?zone rdfs:label ?de. }
  FILTER NOT EXISTS { ?zone wdt:P576 ?dissolvedDate. }
}"

### load the actual data
zones <- query_wikidata(sparql_query, format = c("simple", "smart"))
### give nice names
zones_cleaned <- zones %>% rename(operator_wikidata=zone,zone_name_nl=nl,zone_name_fr=fr,zone_name_de=de)
### create a simple wikidata number variable
zones_cleaned$operator_wikidata <- gsub("^http://www.wikidata.org/entity/", "", zones_cleaned$operator_wikidata)
zones_cleaned$email <- gsub("^mailto:", "", zones_cleaned$email)
### remove fake names
zones_cleaned <- zones_cleaned %>%
  mutate(
    zone_name_nl = ifelse(grepl("^Q[0-9]+$", zone_name_nl), NA, zone_name_nl),
    zone_name_fr = ifelse(grepl("^Q[0-9]+$", zone_name_fr), NA, zone_name_fr),
    zone_name_de = ifelse(grepl("^Q[0-9]+$", zone_name_de), NA, zone_name_de)
  )

### de-duplicate (caused by the website, which can have multiple values)
zones_cleaned <- zones_cleaned %>%
  group_by(operator_wikidata) %>%
  summarize(
    zone_name_nl = first(zone_name_nl),
    zone_name_fr = first(zone_name_fr),
    zone_name_de = first(zone_name_de),
    phone=first(phone),
    email=first(email),
    kbo_bce=first(kbo_bce),
    website = paste(website, collapse = "; ")
  )

zones_cleaned <- zones_cleaned %>%
  rename(w_phone=phone,w_email=email,w_website=website,w_kbo_bce=kbo_bce)





# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list <- list("amenity" = "fire_station")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("emergency_phone", "fire_station:type", 
                   "fire_station:type:FR","emergency","operator:phone")
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




### Function to upload raw data from OSM & wikidata ----
# CreateImportTable is loaded via utils and called in the main funcion





# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper firestation ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.firestations CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.firestations
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
    CONSTRAINT firestations_pkey PRIMARY KEY (id)
  );
",paste0("WITH mergewiki AS(
SELECT f.*, s.zone_name_nl, s.zone_name_fr, s.zone_name_de, s.w_phone, s.w_email, s.w_kbo_bce, s.w_website FROM raw_data.osm_firestations f
LEFT JOIN raw_data.wikidata_be_safetyzones s ON f.operator_wikidata=s.operator_wikidata),

cleaned as (SELECT
'https://osm.org/' || osm_id as original_id,
jsonb_strip_nulls(jsonb_build_object(
    'und', CASE WHEN name IS NULL AND name_nl IS NULL AND name_fr IS NULL AND name_de IS NULL AND operator_type='industrial' then 'industrial firestation'
				WHEN name IS NULL AND name_nl IS NULL AND name_fr IS NULL AND name_de IS NULL AND operator_type='industrial' then 'airport firestation'
				WHEN name IS NULL AND name_nl IS NULL AND name_fr IS NULL AND name_de IS NULL AND operator_type='military' then 'military firestation'
				WHEN name IS NULL AND name_nl IS NULL AND name_fr IS NULL AND name_de IS NULL then 'firestation'
				ELSE name END,
    'dut', CASE WHEN name IS NULL AND name_nl IS NULL AND operator_type='industrial' then 'brandweerkazerne industriezone'
				WHEN name IS NULL AND name_nl IS NULL AND operator_type='industrial' then 'brandweerkazerne luchthaven'
				WHEN name IS NULL AND name_nl IS NULL AND operator_type='military' then 'militaire brandweerkazerne'
				WHEN name IS NULL AND name_nl IS NULL then 'brandweerkazerne' 
				ELSE name_nl END,
	    'fre', CASE WHEN name IS NULL AND name_fr IS NULL AND operator_type='industrial' then 'caserne de pompiers zone industrielle'
				WHEN name IS NULL AND name_fr IS NULL AND operator_type='industrial' then 'caserne de pompiers aéroport'
				WHEN name IS NULL AND name_fr IS NULL AND operator_type='military' then 'caserne de pompiers militaire'
				WHEN name IS NULL AND name_fr IS NULL then 'caserne de pompiers' 
				ELSE name_fr END,
	    'ger', CASE WHEN name IS NULL AND name_de IS NULL AND operator_type='industrial' then 'Feuerwache Industriegebiet'
				WHEN name IS NULL AND name_de IS NULL AND operator_type='industrial' then 'Flughafenfeuerwache'
				WHEN name IS NULL AND name_de IS NULL AND operator_type='military' then 'Militärische Feuerwache'
				WHEN name IS NULL AND name_de IS NULL then 'Feuerwache' 
				ELSE name_de END
)) as name,
jsonb_build_object(
	'dut', 	CASE WHEN operator_type='emergency_zone' THEN 'brandweerpost'
			ELSE 'brandweerkazerne (niet van hulpverleningszone)' END,
	'eng', 	CASE WHEN operator_type='emergency_zone' THEN 'fire station'
			ELSE 'fire station (non-emergency zone)' END,
	'fre', CASE WHEN operator_type='emergency_zone' THEN 'poste de pompiers'
	ELSE 'caserne de pompiers (pas d''une zone de secours)' END,
	'ger', CASE WHEN operator_type='emergency_zone' THEN 'Feuerwachen'
	ELSE 'Feuerwachen (nicht von Hilfeleistungszone)' END) as legend_item,
CASE WHEN short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL 
	ELSE CONCAT_WS('; ',short_name, official_name, alt_name, old_name) END AS other_names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END
	AS address,
CASE WHEN fire_station_type IS NULL AND mergewiki.fire_station_type_fr IS NULL THEN NULL
	ELSE CONCAT_WS('; ',fire_station_type, mergewiki.fire_station_type_fr) END AS firestation_type,
CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_email, email) END AS local_email,
CASE WHEN operator_email IS NULL AND w_email IS NULL THEN NULL 
  ELSE CONCAT_WS('; ',operator_email, w_email) END AS operator_email,
CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS local_phone,
CASE WHEN operator_phone IS NULL AND w_phone IS NULL THEN NULL
  ELSE CONCAT_WS('; ',operator_phone, w_phone) END AS operator_phone,
CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS local_website,
CASE WHEN operator_website IS NULL AND w_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',operator_website, w_website) END AS operator_website,
operator_wikidata, operator, operator_type, emergency, image, geometry
FROM mergewiki)


INSERT INTO ingestion.firestations 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",data_list_id,"' as data_list_id,
0 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'other_names', other_names,
	'address', address,
	'firestation_type', firestation_type,
	'local_email',local_email,
	'local_phone',local_phone,
	'operator_phone',operator_phone,
	'local_website',local_website,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'ambulance_station', CASE WHEN emergency='ambulance_station' THEN 'yes' ELSE NULL END,
	'image', image)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))






### Create SQL for transformation table ----

# Only use this code if there is no existing transformation table
# Note: don't worry, it's just a function that you can't accidentally run

transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.firestations CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.firestations
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
    CONSTRAINT firestations_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.firestations 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT original_id, name, legend_item, data_list_id::uuid, risk_level, properties, geometry, created_at FROM ingestion.firestations;
")


### Create fdw views ----
fdw_views_sql <- c("
CREATE OR REPLACE VIEW fdw.fdw_firestations
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
FROM transformation.firestations;
","
ALTER TABLE fdw.fdw_firestations
OWNER TO paragon;
","
GRANT SELECT ON TABLE fdw.fdw_firestations TO fdw4dev;
","
GRANT ALL ON TABLE fdw.fdw_firestations TO paragon;
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



# Main function -----------------------------------------------------------
# """"""""""""""""""""----




# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("firestations", 50, 250, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_firestations")  
  CreateImportTable(dataset = zones_cleaned, schema = "raw_data", table_name = "wikidata_be_safetyzones")
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
  #create_fdw_views()
}


if(F){
  main_function()
}


