## ---------------------------
##
## Script name: Import railways from OSM and kilometer markers from Infrabel
##
## Purpose of script: Load OSM railway infrastructure & Infrabel kilometer markers and transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2026-07-08
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

data_list_id_osm <- "2a04d94d-40cc-4342-ae00-8f73e70e95ce"
data_list_id_infrabel <- "e3c86442-d1b6-4785-a1d1-91b46a375cb8"

li_train_main           <- "f47ac10b-58cc-4372-a567-0e02b2c3d479"
li_train_secondary      <- "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
li_tram_main            <- "550e8400-e29b-41d4-a716-446655440000"
li_tram_secondary       <- "7d793037-a076-4f21-87b5-76c6a8c9a9e0"
li_subway_main          <- "c56a4180-65aa-42ec-a945-5fd21dec0538"
li_subway_secondary     <- "9f8b4e23-1a2b-4c5d-8e6f-7a8b9c0d1e2f"
li_light_rail_main      <- "3d594650-3079-41d5-aa58-e4f18c40d60e"
li_light_rail_secondary <- "b3d1c2e4-5f6a-7b8c-9d0e-1f2a3b4c5d6e"

li_rail_milemarker <- "29c2b0d1-d992-49c8-956d-ba50f67d73c8"

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

# Set location for temporary files
temporary_folder <-Sys.getenv("TEMPORARY_STORAGE")


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
features_list <- list(or_group(tag_filter("railway")))

# Define extra tags to use as columns for properties
extra_columns <- c("usage", "railway", "electrified", "railway:traffic_mode", "gauge", "highway", "service", "railway:preserved", 
                   "layer", "bridge", "tunnel", "location", "ref", "frequency", "railway:maximum_current", "voltage", "maxspeed",
                   "passenger_lines", "railway:etcs", "railway:track_class")

# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("lines")

### Download OSM rail ----
osm_railways <- run_process(
  download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE),
  paste0("OSM download & processing for ", describe_features_list(features_list))
)

# remove relations: in this dataset, these are objects bundling services on railway lines - but we are interested in the infra, not the service
osm_railways <- osm_railways %>%
  filter(!grepl('relation',osm_id))



osm_railways <- osm_railways %>%
  mutate(
    rail_class_description = case_when(
      # train – main lines
      railway == 'rail' &
        (is.na(usage) | usage %in% c('branch', 'main', 'industrial')) &
        (is.na(railway_preserved) | railway_preserved == 'no') ~ 'train_main',
      # train – secondary lines
      railway == 'rail' &
        usage %in% c('freight', 'military', 'siding', 'yard', 'tourism') ~ 'train_secondary',
      # tram / light rail / subway – main lines
      railway %in% c('light_rail', 'tram', 'subway') &
        (is.na(usage) | usage %in% c('branch', 'main', 'transportation')) ~ paste0(railway, '_main'),
      # tram / light rail / subway – secondary lines
      railway %in% c('light_rail', 'tram', 'subway') &
        !is.na(usage) &
        !usage %in% c('branch', 'main', 'transportation') ~ paste0(railway, '_secondary'),
      .default = NA
    )
  ) %>%
  filter(!is.na(rail_class_description))



osm_railways <- build_osm_derived_columns(osm_railways)


osm_railways <- osm_railways %>%
  mutate(rail_electrified_type=case_when(
    grepl('rail',electrified) & grepl('contact_line',electrified) ~ 'both ground-level power supply and overhead contact line',
    grepl('contact_line',electrified) ~ 'overhead contact line',
    grepl('rail',electrified) | grepl('ground-level_power_supply',electrified) | grepl('4th_rail',electrified) ~ 'ground-level power supply',
    .default = 'no'
  ))

osm_railways <- osm_railways %>%
  mutate(rail_service=case_when(
    grepl('siding',service) ~ 'siding',
    grepl('spur',service) ~ 'spur',
    grepl('yard',service) ~ 'yard',
    .default = NA
  ))

osm_railways <- osm_railways %>%
  mutate(bridge=ifelse(bridge=='no' | is.na(bridge), NA, bridge))

osm_railways <- osm_railways %>%
  mutate(tunnel=ifelse((tunnel!='no' & !is.na(tunnel)) | location=='underground', 'yes', NA))

osm_railways <- osm_railways %>%
  mutate(rail_electrified_acdc=ifelse(frequency=='0', 'DC', paste0(frequency, ' Hz')))

osm_railways <- osm_railways %>%
  mutate(rail_electrified_max_current=ifelse(!is.na(railway_maximum_current),paste0(railway_maximum_current, ' A'), NA))

osm_railways <- osm_railways %>%
  mutate(rail_electrified_voltage = sapply(voltage, function(v) {
    if (is.na(v)) return(NA)
    values <- strsplit(v, ";")[[1]]
    values <- trimws(values)
    values <- values[values != "0" & values != ""]
    if (length(values) == 0) return(NA)
    paste(paste0(values, " V"), collapse = "; ")
  }))


osm_railways <- osm_railways %>%
  rename(rail_parallel_main_tracks=passenger_lines) %>%
  rename(rail_track_class=railway_track_class)

osm_railways <- osm_railways %>%
  mutate(rail_etcs=case_when(
    railway_etcs=='1' ~ 1,
    railway_etcs=='2' | railway_etcs=='1;2' ~ 2,
    .default=NA))

# add unit to gauge if it's a single, unitless number
osm_railways <- osm_railways %>%
  mutate(gauge = ifelse(!is.na(gauge) & grepl("^[0-9]+$", gauge), paste0(gauge, ' mm'), gauge))



# Download Infrabel milemarkers ----
url_infrabel <- 'https://infrabel.opendatasoft.com/api/explore/v2.1/catalog/datasets/relatie-tussen-kilometerpalen-en-lijnen/exports/geojson?lang=nl&timezone=Europe%2FBerlin'
response <- GET(url_infrabel)
milemarkers <- st_read(rawToChar(response$content),quiet=TRUE)

# markers without an actual number seem to be generally not relevant
milemarkers <- milemarkers %>%
  filter(!is.na(name))

# use clearer names
milemarkers <- milemarkers %>%
  rename(
    original_id=kpp_id,
    rail_segment_id = track_id,
    rail_ref = linecalfa)

# Upload to raw data ----
CreateImportTable(dataset = osm_railways, schema = "raw_data", table_name = "osm_railways")
CreateImportTable(dataset = milemarkers, schema = "raw_data", table_name = "infrabel_milemarkers")


  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function



# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql_osm <- c("
DROP TABLE IF EXISTS ingestion.railway_segment CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.railway_segment
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
    CONSTRAINT railway_segment_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
'https://osm.org/' || osm_id AS original_id,
jsonb_strip_nulls(jsonb_build_object(
      'und', CASE WHEN name IS NOT NULL AND ref IS NOT NULL THEN concat(name, ' (' || ref || ')') 
	WHEN name IS NOT NULL THEN name
	WHEN ref IS NOT NULL then ref	  
	  WHEN rail_class_description='train_main' THEN 'train line'
	  WHEN rail_class_description='tram_main' THEN 'tram line'
	  WHEN rail_class_description='subway_main' THEN 'subway line'
	  WHEN rail_class_description='light_rail_main' THEN 'light rail line'
	  WHEN rail_class_description='train_secondary' THEN 'special train line'
	  WHEN rail_class_description='tram_secondary' THEN 'special tram line'
	  WHEN rail_class_description='subway_secondary' THEN 'special subway line'
	  WHEN rail_class_description='light_rail_secondary' THEN 'special light rail line'
	  END,
      'fre', COALESCE(name_fr, CASE
	  WHEN name IS NOT NULL OR ref IS NOT NULL THEN NULL
	  WHEN rail_class_description='train_main' THEN 'ligne de train'
	  WHEN rail_class_description='tram_main' THEN 'ligne de tram'
	  WHEN rail_class_description='subway_main' THEN 'ligne de métro'
	  WHEN rail_class_description='light_rail_main' THEN 'ligne de métro léger'
	  WHEN rail_class_description='train_secondary' THEN 'ligne de train spéciale'
	  WHEN rail_class_description='tram_secondary' THEN 'ligne de tram spéciale'
	  WHEN rail_class_description='subway_secondary' THEN 'ligne de métro spéciale'
	  WHEN rail_class_description='light_rail_secondary' THEN 'ligne de métro léger spéciale'
	  END),
      'ger', COALESCE(name_de, CASE
	  WHEN name IS NOT NULL OR ref IS NOT NULL THEN NULL
	  WHEN rail_class_description='train_main' THEN 'Zugstrecke'
	  WHEN rail_class_description='tram_main' THEN 'Straßenbahnlinie'
	  WHEN rail_class_description='subway_main' THEN 'U-Bahn-Linie'
	  WHEN rail_class_description='light_rail_main' THEN 'Stadtbahnlinie'
	  WHEN rail_class_description='train_secondary' THEN 'besondere Zugstrecke'
	  WHEN rail_class_description='tram_secondary' THEN 'besondere Straßenbahnlinie'
	  WHEN rail_class_description='subway_secondary' THEN 'besondere U-Bahn-Linie'
	  WHEN rail_class_description='light_rail_secondary' THEN 'besondere Stadtbahnlinie'
	  END),
      'dut', COALESCE(name_nl, CASE
	  WHEN name IS NOT NULL OR ref IS NOT NULL THEN NULL
	  WHEN rail_class_description='train_main' THEN 'treinlijn'
	  WHEN rail_class_description='tram_main' THEN 'tramlijn'
	  WHEN rail_class_description='subway_main' THEN 'metrolijn'
	  WHEN rail_class_description='light_rail_main' THEN 'lightraillijn'
	  WHEN rail_class_description='train_secondary' THEN 'bijzondere treinlijn'
	  WHEN rail_class_description='tram_secondary' THEN 'bijzondere tramlijn'
	  WHEN rail_class_description='subway_secondary' THEN 'bijzondere metrolijn'
	  WHEN rail_class_description='light_rail_secondary' THEN 'bijzondere lightraillijn'
	  END))) as name,
CASE
  WHEN rail_class_description = 'train_main'           THEN '",li_train_main,"'::uuid
  WHEN rail_class_description = 'train_secondary'      THEN '",li_train_secondary,"'::uuid
  WHEN rail_class_description = 'tram_main'            THEN '",li_tram_main,"'::uuid
  WHEN rail_class_description = 'tram_secondary'       THEN '",li_tram_secondary,"'::uuid
  WHEN rail_class_description = 'subway_main'          THEN '",li_subway_main,"'::uuid
  WHEN rail_class_description = 'subway_secondary'     THEN '",li_subway_secondary,"'::uuid
  WHEN rail_class_description = 'light_rail_main'      THEN '",li_light_rail_main,"'::uuid
  WHEN rail_class_description = 'light_rail_secondary' THEN '",li_light_rail_secondary,"'::uuid
END::uuid AS legend_item_id,
1 as risk_level,
other_names,local_email,operator_email,local_phone,local_website,
operator_website,
operator_wikidata, operator, operator_type, image,
wikidata,
gauge, railway_traffic_mode as rail_traffic_mode, rail_service, bridge, tunnel, ref,
rail_electrified_type, rail_electrified_acdc, rail_electrified_max_current, rail_electrified_voltage, 
geometry
FROM raw_data.osm_railways)

INSERT INTO ingestion.railway_segment
(original_id, name, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item_id,
'",data_list_id_osm,"'::uuid as data_list_id,
risk_level,
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
	'image',image,
	'wikidata',wikidata,
	'gauge',gauge,
	'rail_traffic_mode',rail_traffic_mode,
	'rail_service',rail_service,
	'bridge',bridge,
	'tunnel',tunnel,
	'ref',ref,
	'rail_electrified_type',rail_electrified_type,
	'rail_electrified_acdc',rail_electrified_acdc,
	'rail_electrified_max_current',rail_electrified_max_current,
	'rail_electrified_voltage',rail_electrified_voltage
)) as properties,
ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.railway_segment OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.railway_segment TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.railway_segment TO pgn_user_airflow;")




ingestion_table_sql_infrabel <- c("
DROP TABLE IF EXISTS ingestion.railway_km_marker CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.railway_km_marker
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
    CONSTRAINT railway_km_marker_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
original_id,
jsonb_strip_nulls(jsonb_build_object(
      'und', CASE WHEN name is null THEN 'unmarked railway KM-marker' ELSE name END
    	)) as name,
1 as risk_level,
rail_segment_id, rail_ref,
geometry
FROM raw_data.infrabel_milemarkers)

INSERT INTO ingestion.railway_km_marker 
(original_id, name, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
'",li_rail_milemarker,"'::uuid as legend_item_id,
'",data_list_id_infrabel,"'::uuid as data_list_id,
risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'rail_segment_id',rail_segment_id,
	'rail_ref',rail_ref
)) as properties,
ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.railway_km_marker OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.railway_km_marker TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.railway_km_marker TO pgn_user_airflow;"
)



### Execute the SQL commands ----

create_ingestion_table_osm <- function() {execute_sql_commands(ingestion_table_sql_osm, "Ingestion table OSM")}
create_ingestion_table_infrabel <- function() {execute_sql_commands(ingestion_table_sql_infrabel, "Ingestion table Infrabel")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
#overrule_checks<-TRUE


run_smart_update = function() {
  smart_update_process("railway_km_marker", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
  smart_update_process("railway_segment", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data, same_id_size_factor = 1.25,different_id_size_factor = 1.25)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    create_ingestion_table_osm()
    create_ingestion_table_infrabel()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}
