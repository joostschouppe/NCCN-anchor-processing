## ---------------------------
##
## Script name: Import shopping malls
##
## Purpose of script: Load OSM shopping malls & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2025-03-20
##
##
## ---------------------------

# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

data_list_id <- "236626f3-af57-42f9-8113-2d7c383bdfa2"
legend_item_id <- "12f749c3-77c6-44ea-8792-052236e57aa3"

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

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


data_list_id<-"236626f3-af57-42f9-8113-2d7c383bdfa2"

temporary_folder <-Sys.getenv("TEMPORARY_STORAGE")
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")


# Libraries -------------------------------
# """""""""""""""""" ----------------------

# no extra libraries needed

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))




# EXTRACT ----
# """""""""""""""""" ----


DownloadOSM <- function(){
  tryCatch({

# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list <- list(or_group(
 tag_filter("shop", "mall"),
 tag_filter("shop", "department_store")
 ))

# Define extra tags to use as columns for properties
extra_columns <- c("shop", "max_level", "min_level", "brand:wikidata")

# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----

osm_all <- run_process(
  download_osm_process(features_list, datatypes, extra_columns),
  paste0("OSM download & processing for ", describe_features_list(features_list))
)

# remove if no name
# only keep relevant department stores:
## INNO Q300632, La Fayette Q3094686 , Galeria (Karstadt of Kaufhof) Q80220059 (brand:wikidata), De Bijenkorf Q2063937
osm_all <- osm_all %>%
  filter(!is.na(name)) %>%
  filter(!(shop == "department_store" & !grepl("Q300632|Q3094686|Q80220059|Q2063937", brand_wikidata)))


# detect objects on top of other objects in the dataset
result <- osmspatial_deduplication(osm_all, c("shop", "min_level", "max_level"))
join_filtered <- result$data
#osm_diagnostic <- result$diagnostic

# standardize the usual OSM columns
join_filtered <- build_osm_derived_columns(join_filtered)


CreateImportTable(dataset = join_filtered, schema = "raw_data", table_name = "osm_mall")

  }, 
error = function(e) {
  print(paste("Error downloading OSM:", e))
  return(NULL)  # Return NULL on failure
})
}


# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function




# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.mall CASCADE;
",paste0("
CREATE TABLE IF NOT EXISTS ingestion.mall
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
  CONSTRAINT mall_pkey PRIMARY KEY (id)
);"),
paste0("WITH 
cleaned as (SELECT
'https://osm.org/' || osm_id as original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', name,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
            jsonb_build_object(
              'dut', 'winkelcentrum',
              'fre', 'centre commercial',
              'ger', 'Einkaufszentrum',
              'eng', 'shopping mall') as legend_item,
            other_names,address,local_email,operator_email,local_phone,local_website,
operator_website,operator_wikidata, operator, operator_type, image, 
min_level,max_level,
	geometry FROM raw_data.osm_mall)
INSERT INTO ingestion.mall 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",legend_item_id,"'::uuid as legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
1 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'other_names', other_names,
  'address', address,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
	'operator',operator,
	'building_lowest_level',min_level,
	'building_highest_level',max_level
)),
ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.mall OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.mall TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.mall TO pgn_user_airflow;")



### Execute the SQL commands ----


create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}

run_smart_update = function() {
  smart_update_process("mall", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    DownloadOSM()
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}

