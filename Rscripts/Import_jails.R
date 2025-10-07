## ---------------------------
##
## Script name: Import jails from OSM
##
## Purpose of script: Load OSM jails data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-24
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

data_list_id_jails <- "e3277b09-8cb1-491e-8fc7-3ab9ef8dbb2a"
data_list_id_asylum <- "fbc8ea91-872d-4d6d-b4dc-5eadd1e5f5a1"
legend_item_id_jail_fed <- "2db8b300-1c09-47d8-bcb3-fb393d44f6c2"
legend_item_id_jail_oth <- "fc2b2f55-6ea2-4e73-a899-e286b6463fad"
legend_item_id_asylum_detention <- "f8b40bd7-7b2c-41a7-a1cd-4ceb5b16af9f"


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



# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    

# Define the list of features
features_list <- list("amenity" = "prison")

# Define extra tags to use as columns for properties
extra_columns <- c("start_date", "capacity","capacity:planned","capacity:female","capacity:theoretically","prison",
                   "prison:for", "prison:age", "prison:classification")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----
osm_all <<- run_process(
  download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
  paste0("OSM download & processing for ", paste(paste(names(features_list), unlist(features_list), sep = "="), collapse = ", "), collapse = ", ")
)

# if it is in Belgium, it must have a name, and it should have one of the below:
## operator:wikidata to indicate it as a federal prison
## prison='rejected_asylum_seekers' to indicate it as a closed detention centre for refugees
## prison=pre_release or prison:for=juvenile to indicate "other prisons"


osm_all_problems <- osm_all %>%
  filter(!is.na(language)
    & (
    is.na(name) | 
    (
      (operator_wikidata != 'Q1469956' | is.na(operator_wikidata)) &
      ((prison!='rejected_asylum_seekers' & prison!='pre_release') | is.na(prison)) &
      (prison_for!='juvenile' | is.na(prison_for))
    )
  ))

# if osm_all_problems has records, save them to log as geojson
if (nrow(osm_all_problems)>0){
  filename_visualization<-paste0(log_folder,"jails_problems", format(Sys.time(), "%Y%m%d_%H%M%S"), ".geojson")
  st_write(osm_all_problems, filename_visualization, driver = "GeoJSON")
  print(paste0("OSM data issues need to be fixed first, check them at ", filename_visualization))
} else {
  print("OSM data quality check passed")
}

# add a stop if there are problems
if (nrow(osm_all_problems)>0){
  stop("OSM data quality check failed")
}



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

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.jails CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.jails
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
    CONSTRAINT jails_pkey PRIMARY KEY (id)
  );
",paste0("
WITH 
cleaned as (SELECT
'https://osm.org/' || osm_id as original_id,
jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL AND prison='rejected_asylum_seekers' THEN 'closed centre for rejected asylum seekers'
						WHEN name IS NULL THEN 'jail' ELSE name END,
              'fre', name_fr,
              'ger', name_de,
              'dut', name_nl)) as name,
CASE 
WHEN prison='rejected_asylum_seekers' THEN
jsonb_build_object(
	'dut', 'gesloten centrum voor uitgeprocedeerde asielzoekers',
	'fre', 'centre fermé pour demandeurs d''asile ayant épuisé tous les recours légaux',
	'ger', 'geschlossenes Zentrum für Asylbewerber, die alle Rechtsmittel ausgeschöpft haben',
	'eng', 'closed centre for rejected asylum seekers') 
WHEN operator_wikidata='Q1469956' THEN
  jsonb_build_object(
	'dut', 'Belgische federale gevangenis',
	'fre', 'prison Belge federal',
	'ger', 'Belgisches Bundesgefängnis',
	'eng', 'Belgian federal prison') 
ELSE jsonb_build_object(
	'dut', 'gevangenis (overige)',
	'fre', 'prison (autre)',
	'ger', 'Gefängnis (andere)',
	'eng', 'prison (other)') END
			as legend_item,
CASE WHEN prison='rejected_asylum_seekers' THEN '",legend_item_id_asylum_detention,"'::uuid
  WHEN operator_wikidata='Q1469956' THEN '",legend_item_id_jail_fed,"'::uuid
  ELSE '",legend_item_id_jail_oth,"'::uuid
  END as legend_item_id,	
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
start_date,capacity,capacity_planned,capacity_female,capacity_theoretically,prison,		
geometry
FROM raw_data.osm_jails)

INSERT INTO ingestion.jails 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
legend_item_id,
CASE WHEN prison='rejected_asylum_seekers' THEN '",data_list_id_asylum,"'::uuid
ELSE '",data_list_id_jails,"'::uuid END as data_list_id,
2 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'other_names', other_names,
	'address', address,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image,
	'start_date',start_date,
	'capacity',capacity,
	'capacity_planned',capacity_planned,
	'capacity_female',capacity_female,
	'capacity_theoretically',capacity_theoretically,
	'prison_type',prison
	)) as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.jails OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.jails TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.jails TO pgn_user_airflow;")




### Execute the SQL commands ----


create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data


run_smart_update = function() {
  smart_update_process("jails", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}

# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_jails")  
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}