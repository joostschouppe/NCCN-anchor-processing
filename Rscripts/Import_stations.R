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

# External IDs
data_list_id<-"cd9bd5a4-f635-4c3d-b835-dc481945d4fa"
li_station_metro <- "1fc2b8f0-f0cb-4d97-ad7b-d71a34ce84c2"
li_station_other <- "7a67afbc-e228-4d86-baee-bf870752e949"
li_station_train <- "89845afd-bafc-4396-a362-ce68a76760a0"

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



# Set log folder
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------
rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))

# Extra libraries -------------------------------
# """""""""""""""""" ----------------------
# all are loaded via the utils




# EXTRACT ----
# """""""""""""""""" ----
# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {

    
# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list <- list("railway" = "station", "railway"="halt")

# Define extra tags to use as columns for properties
extra_columns <- c("network:wikidata", "train", "station", "subway", 
                   "ref:STIB_MIVB","network","railway:ref","uic_ref",
                   "highspeed","ref", "railway:ref:DB", "usage")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----
osm_all <- run_process(
  download_osm_process(features_list, datatypes, extra_columns, postgres=TRUE),
  paste0("OSM download & processing for ", paste(paste(names(features_list), unlist(features_list), sep = "="), collapse = ", "), collapse = ", ")
)


# Upload to raw data ----
CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_stations") 

  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function






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
            CASE WHEN station='subway' THEN '",li_station_metro,"'::uuid
                WHEN station IS NULL or station='railway' THEN '",li_station_train,"'::uuid
                ELSE '",li_station_other,"'::uuid END
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
operator_website,operator_wikidata,
			network_wikidata, train, station, subway, ref_stib_mivb,network,railway_ref,uic_ref,highspeed,ref,railway_ref_db,
            geometry
            FROM raw_data.osm_stations
			WHERE (name IS NOT NULL)
			AND (usage IS NULL OR (usage!= 'tourism' AND usage!='leisure'))
			AND (station IS NULL OR station!= 'miniature')
		)
				
INSERT INTO ingestion.stations
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
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
FROM cleaned;"),
"ALTER TABLE ingestion.stations OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.stations TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.stations TO pgn_user_airflow;")


### Execute the SQL commands ----
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}



run_smart_update = function() {
  smart_update_process("stations", 100, 150, 100, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
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


if (run_status) {
  main_function()
}


