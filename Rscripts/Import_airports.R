## ---------------------------
##
## Script name: Import airports from OSM
##
## Purpose of script: Load OSM international airport and military airbase data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-24
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

data_list_id<-"484a4303-9a16-4a01-a63c-e2f50025a095"
legend_item_id_international <- "348540ec-6c03-4a97-9112-01d8453726b0"
legend_item_id_military <- "a0c704ab-a2d6-4e48-8873-a8089b3dc582"



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



log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))




# Libraries -------------------------------
# """""""""""""""""" ----------------------

# loaded in utils



# EXTRACT ----
# """""""""""""""""" ----

process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    

# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features

# international airports
features_international <- list("aerodrome:type" = "international",
                               "aerodrome" = "international")

# military airports
features_military <- list("military"="airfield")



# Define extra tags to use as columns for properties
extra_columns <- c("aerodrome:type", "aeroway", "military", "landuse", "iata", "icao")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----

# download international airports
tryCatch({
  # Call the large function
  osm_international<-download_osm_process(features_international, datatypes, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processed succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

osm_international <- osm_international %>% mutate(object_type = "international airport")

# download military airports
tryCatch({
  # Call the large function
  osm_military<-download_osm_process(features_military, datatypes, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processed succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

osm_military <- osm_military %>% mutate(object_type = "military airbase")

# Keep only full-time operational military airbases

osm_military_operational <- osm_military %>%
  filter(aeroway == "aerodrome" | is.na(aeroway)) %>% # removes heliports/helipads
  filter(! str_detect(name, "modélisme")) %>% # removes model airplane clubs
  filter(! str_detect(name, "Saint-Hubert|Wevelsmoer|Brasschaat|Moorsele")) # removes reserve airbases in Belgium (SHAPE and other)

# other NATO SHAPE airbases and runways on military domains are under general tags aeroway=aerodrome or aeroway=runway
# query results for Dutch, German, French military airbases within bounding box + buffer seem to be accurate and complete

# Is there overlap between civil and military airports?

compare_types <- st_join(osm_military_operational, osm_international, join = st_intersects, left = FALSE) # inner spatial join

# airports with well-defined public and military parts can exist as two separate objects

# Is there overlap between nodes and ways in military airports?

osm_military_merged <- osm_military_operational %>%
  group_by(group = st_intersects(geometry)) %>%
  arrange(desc(osm_id)) %>% # prioritizes ways over nodes
  summarize(across(-geometry, ~first(na.omit(.)))) %>% # adds value of node where value of way is NA
  ungroup() %>%
  select(- group)


osm_all <<- rbind(osm_international, osm_military_merged)

  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function


# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function



# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.airports CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.airports
  (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    original_id text,    
    name jsonb,
    legend_item jsonb,
    legend_item_id uuid,
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
    CONSTRAINT airports_pkey PRIMARY KEY (id)
  );
",paste0("WITH 
cleaned as (SELECT
'https://osm.org/' || osm_id AS original_id,
jsonb_strip_nulls(jsonb_build_object('und', CASE WHEN name IS NULL THEN 'airport' ELSE name END,
    	'fre', name_fr,
    	'ger', name_de,
    	'dut', name_nl)) as name,
jsonb_build_object(
      'dut', CASE WHEN object_type='international airport' THEN 'internationale luchthaven' ELSE 'militaire luchtmachtbasis' END,
      'fre', CASE WHEN object_type='international airport' THEN 'aéroport international' ELSE 'base aérienne militaire' END,
      'ger', CASE WHEN object_type='international airport' THEN 'internationaler Flughafen' ELSE 'Militärflughafen' END,
      'eng', CASE WHEN object_type='international airport' THEN 'international airport' ELSE 'military airbase' END) as legend_item,
     CASE WHEN object_type='international airport' THEN '",legend_item_id_international,"'::uuid
     ELSE '",legend_item_id_military,"'::uuid END
     as legend_item_id,
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
operator_wikidata, operator, operator_type, image, 
iata,	icao, wikidata,
CASE WHEN object_type='international airport' THEN 3 ELSE 2 END AS risk_level,
geometry
FROM raw_data.osm_airports)


INSERT INTO ingestion.airports 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'iata', iata,
	'iaco', icao,
	'other_names', other_names,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image,
	'wikidata', wikidata))
END,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.airports OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.airports TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.airports TO pgn_user_airflow;")


### Execute the SQL commands ----
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}



# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("airports", 1000, 2000, 1000, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_airports")  
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}
