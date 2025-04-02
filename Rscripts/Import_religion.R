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

# Docs: https://dev.azure.com/NCCN-Paragon/Paragon/_wiki/wikis/Paragon.wiki/581/Religious-buildings

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

data_list_id<-"45f9f964-4fe7-4966-8140-b622cb69d224"

log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))

# Libraries -------------------------------
# """""""""""""""""" ----------------------

# everything loaded via utils



# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {

# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list <- list("amenity" = "place_of_worship")
# Define extra tags to use as columns for properties
extra_columns <- c("building","place_of_worship","place_of_worship:type",
                   "religion","denomination",
                   "services","service_times",
                   "basilica","deanery","diocese","parish","historic", "building:part")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})


  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function



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
	AND (building_part IS NULL OR (building_part != 'wayside_chapel' AND building_part != 'wayside_cross' AND building_part != 'wayside_shrine' AND building_part != 'chapel'))
	AND NOT (name IS NULL and religion IS NULL)
	AND (place_of_worship IS NULL OR (place_of_worship!= 'wayside_chapel' AND place_of_worship!='wayside_shrine' AND place_of_worship!='lourdes_grotto'))
	AND (place_of_worship_type IS NULL OR (place_of_worship_type != 'wayside_chapel' AND place_of_worship_type!= 'wayside_shrine' AND place_of_worship_type!= 'wayside_cross'))
	AND (historic IS NULL OR (historic != 'wayside_shrine' AND historic != 'wayside_cross' AND historic != 'wayside_chapel'))
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






### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}





# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-overrule_checks


run_smart_update = function() {
  smart_update_process("place_of_worship", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_religion")  
    create_ingestion_table()
  }
  run_smart_update()
  #create_transformation_table()
}


if(run_status){
  main_function()
}

