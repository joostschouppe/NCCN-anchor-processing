## ---------------------------
##
## Script name: Import dams & locks from OSM
##
## Purpose of script: Load OSM water infrastructure data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-24
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# Load external IDs
data_list_id<-"17d9f06d-9eb2-4cf9-86e6-5921a9edd3f3"
li_dam <- "9b94748f-2e95-443f-a5e4-b07d523cea9f"
li_lock <- "086dae30-25e0-492c-b59e-26dfa7c6353e"
li_boatlift <- "aa7c9f05-10f8-49bc-933a-b7418b4e89eb"


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



# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {

# Download OSM data ----


### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_lock <- list("lock" = "yes")
features_boatlift <- list("waterway" = "boat_lift")
features_dam <- list("waterway" = "dam")


# Define extra tags to use as columns for properties
extra_columns <- c("CEMT", "lock", "lock_name", "lock_name:nl", "lock_name:fr", "lock_name:de", "lock_ref", "lock:height", "length", "maxdraft", "maxdraught", "maxheight", "maxlength", "maxwidth", "seamark:name", "waterway","boat_lift")

# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes_lock <- c("lines")
datatypes_boatlift <- c("points", "lines", "mpolygon")
datatypes_dam <- c("lines", "mpolygon")


### Actual OSM download & transformation ----



tryCatch({
  # Call the OSM function
  osm_lock<-download_osm_process(features_lock, datatypes_lock, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})
tryCatch({
  # Call the OSM function
  osm_boatlift<-download_osm_process(features_boatlift, datatypes_boatlift, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})
tryCatch({
  # Call the OSM function
  osm_dam<-download_osm_process(features_dam, datatypes_dam, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})



#LOCK:only if line & NOT also boat-lift
osm_lock <- osm_lock %>%
  filter(lock == "yes" & !grepl("boat_lift", waterway))
osm_lock <- osm_lock %>%
  mutate(
    legend_item = "lock")


# BOAT LIFT: keep only if it has a name. Point, line or polygon
osm_boatlift <- osm_boatlift %>%
  mutate(
    legend_item = case_when(
      waterway == "boat_lift" & !is.na(name) ~ "boat_lift",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(legend_item))

# DAM: keep only if it is line or polygon, if it has a name

osm_dam <- osm_dam %>%
  filter(!is.na(name)) %>%
  mutate(
    legend_item = case_when(
      waterway == "dam" ~ "dam",
      TRUE ~ NA_character_
    )
  )





# merge points and polygons
osm_all <- bind_rows(osm_lock, osm_boatlift, osm_dam)

CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_locks_dams")  

  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function


# Upload to raw data ----




# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.locks_dams CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.locks_dams
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
    CONSTRAINT osm_locks_dams_pkey PRIMARY KEY (id)
  );
",paste0("
WITH 
cleaned as (SELECT
'https://osm.org/' || osm_id AS original_id,
legend_item AS legend_item_orig,
CASE WHEN legend_item = 'lock' THEN
    	jsonb_strip_nulls(jsonb_build_object(
			'und', CASE WHEN lock_name IS NULL THEN 'lock' ELSE lock_name END, --we dont use name because thats the name of the river not the lock
    		'fre', CONCAT_WS(lock_name_fr,' (' || lock_ref || ')'),
    		'ger', CONCAT_WS(lock_name_de::text,' (' || lock_ref || ')'),
    		'dut', CONCAT_WS(lock_name_nl,' (' || lock_ref || ')')))
	WHEN legend_item = 'dam' THEN
    	jsonb_strip_nulls(jsonb_build_object('und', CASE WHEN name IS NULL THEN 'dam' ELSE name END,
    	'fre', name_fr,
    	'ger', name_de::text,
    	'dut', name_nl))
	ELSE
    	jsonb_strip_nulls(jsonb_build_object('und', CASE WHEN name IS NULL THEN 'boat lift' ELSE name END,
    	'fre', name_fr,
    	'ger', name_de::text,
    	'dut', name_nl))
	END as name,
CASE WHEN legend_item = 'lock' THEN
	jsonb_build_object(
	'dut', 'sluis',
	'fre', 'écluse',
	'ger', 'Schleuse')
	WHEN legend_item = 'boat_lift' THEN
	jsonb_build_object(
	'dut', 'scheepslift',
	'fre', 'ascenseur à bateaux',
	'ger', 'Schiffshebewerk')
	ELSE
	jsonb_build_object(
	'dut', 'dam',
	'fre', 'barrage',
	'ger', 'Talsperre')	END as legend_item,
CASE WHEN legend_item = 'lock' THEN '",li_lock,"'::uuid
  WHEN legend_item = 'boat_lift' THEN '",li_boatlift,"'::uuid
	ELSE '",li_dam,"'::uuid END AS legend_item_id,
CASE WHEN seamark_name IS NULL AND short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL
ELSE CONCAT_WS('; ',seamark_name, short_name, official_name, alt_name, old_name) END AS other_names,
CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_email, email) END AS local_email,
operator_email,
CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS local_phone,
CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS local_website,
operator_website,
operator_wikidata, operator, operator_type, image, 
cemt, lock_height, length, 
CASE WHEN maxdraft IS NULL AND maxdraught IS NULL THEN NULL
ELSE CONCAT_WS('; ',maxdraught, maxdraft) END AS maxdraught,
maxheight, maxlength, maxwidth,		
geometry
FROM raw_data.osm_locks_dams)


INSERT INTO ingestion.locks_dams 
(original_id, name, legend_item, legend_item_id, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
CASE WHEN legend_item_orig = 'lock' THEN
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'CEMT', cemt,
	'lock_height', lock_height, 
	'lock_length', length, 
	'lock_maxdraft', maxdraught,
	'lock_maxheight', maxheight, 
	'lock_maxlength', maxlength, 
	'lock_maxwidth', maxwidth,
	'other_names', other_names,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image))
ELSE
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'other_names', other_names,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image))
END as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.locks_dams OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.locks_dams TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.locks_dams TO pgn_user_airflow;"
)

### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-overrule_checks


run_smart_update = function() {
  smart_update_process("locks_dams", 50, 250, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}

