## ---------------------------
##
## Script name: Import cultural centers
##
## Purpose of script: Load OSM cultural centers & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-07-02
##
##
## ---------------------------



# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

data_list_id<-"b8fca5e6-fa2b-465e-b124-bb3b81fefff6"
legend_item_id_cultural_centre <- "b009f733-8f0e-4c17-9b6a-bb1fe33f93d6"
legend_item_id_cinema <- "0e5fdfbc-ce61-4119-865a-e9aaf7b19393"
legend_item_id_concert_hall <- "e82946d3-0977-48be-9009-7ec42b38b97b"
legend_item_id_theatre <- "f8d76589-001a-4b94-8b03-dbf8402d5ca1"
legend_item_id_museum <- "3b565ddc-0d72-4c5f-8a32-c1329ab2323b"

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
features_list <- list(or_group(
  tag_filter("amenity", "theatre"),
  tag_filter("amenity", "community_centre"),
  tag_filter("amenity", "arts_centre"),
  tag_filter("amenity", "concert_hall"),
  tag_filter("amenity", "cinema"),
  tag_filter("amenity", "music_venue"),
  tag_filter("tourism", "museum")
))


# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")

extra_columns <- c("amenity", "community_centre", "community_centre:for", "capacity:persons", "capacity:seats", "operator", "theatre:genre", "theatre:type", "training", "description", "museum", "level", "tourism")

osm_all <- run_process(
  download_osm_process(features_list, datatypes, extra_columns, keep_region = TRUE),
  paste0("OSM download & processing for ", describe_features_list(features_list))
)

# remove objects already in "higher" layers
## get the relevant object ids
con_pg <- get_con()
remove_objects <- dbGetQuery(con_pg, paste0("WITH presel AS (SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.embassy WHERE deleted_at IS NULL UNION ALL
                                     SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.nursing_homes WHERE deleted_at IS NULL)
                                     SELECT distinct(osm_id) AS osm_id from presel"))
dbDisconnect(con_pg)
## anti-join to throw them out
osm_all <- osm_all %>%
  anti_join(remove_objects, by = "osm_id")

# flag open air theatres for removal, unless also a museum
osm_all <- osm_all %>%
  mutate(remove = amenity == "theatre" & theatre_type %in% c("open_air", "amphi") & (is.na(tourism) | tourism != "museum")) %>%
  filter(!remove) %>%
  select(-remove)

# flag unneeded community centres for removal, unless also a museum
osm_all <- osm_all %>%
  mutate(remove = amenity == "community_centre" & !community_centre %in% "cultural_centre" & (is.na(tourism) | tourism != "museum")) %>%
  filter(!remove) %>%
  select(-remove)


# Data quality review: No name in OSM
osm_all_problems<-osm_all %>% 
  filter(is.na(name) & !is.na(amenity) & !is.na(language)) %>%
  select(osm_id,amenity)
# save as geojson
if (nrow(osm_all_problems)>0){
  filename_visualization<-paste0(log_folder,"/cc_no_name_osm", format(Sys.time(), "%Y%m%d_%H%M%S"), ".geojson")
  st_write(osm_all_problems, filename_visualization, driver = "GeoJSON")
  print(paste0("OSM data issues should be reviewed (CC without a name), check them at ", filename_visualization))
} else {
  print("OSM data quality check passed")
}

# remove if no name & remove all training centres
osm_all<-osm_all %>%
  filter(!is.na(name) & is.na(training))


# add object type
osm_all<-osm_all %>%
  mutate(object_type=case_when(
    (amenity=='theatre' & theatre_type=='concert_hall') | amenity=='concert_hall' | amenity=='music_venue' ~ 'concert_hall',
    amenity=='theatre' ~ 'theatre',
    community_centre=='cultural_centre' | amenity=='arts_centre' ~ 'cultural_centre',
    amenity=='cinema' ~ 'cinema',
    tourism=='museum' ~ 'museum',
    TRUE  ~ 'other'
  )) %>%
  mutate(object_type_orig=object_type)

# remove other, if any
osm_all<-osm_all %>%
  filter(object_type!='other')

# detect objects on top of other objects in the dataset
result <- osmspatial_deduplication(osm_all, c("capacity_persons", "capacity_seats", "theatre_type", "theatre_genre", "community_centre_for", "description", "museum", "level", "object_type"))
join_filtered <- result$data
#osm_diagnostic <- result$diagnostic


# standardize the usual OSM columns
join_filtered <- build_osm_derived_columns(join_filtered)


# if object_type became a list, then make it a cultural_centre
join_filtered <- join_filtered %>%
  mutate(object_type = ifelse(object_type_orig != object_type, "cultural_centre", object_type_orig))

# upload to postgres
CreateImportTable(dataset = join_filtered, schema = "raw_data", table_name = "osm_cultural_centre")

  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function

# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function




# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.cultural_centre CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.cultural_centre
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
  CONSTRAINT cultural_centre_pkey PRIMARY KEY (id)
);",paste0("WITH 
cleaned as (SELECT
            'https://osm.org/' || osm_id as original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN object_type
                          ELSE name END,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
            jsonb_build_object(
              'dut', CASE WHEN object_type='concert_hall' THEN 'concertzaal' WHEN object_type='theatre' THEN 'theater' WHEN object_type='cinema' THEN 'bioscoop' ELSE 'cultureel centrum' END,
              'fre', CASE WHEN object_type='concert_hall' THEN 'salle de concert' WHEN object_type='theatre' THEN 'théâtre' WHEN object_type='cinema' THEN 'cinéma' ELSE 'centre culturel' END,
              'ger', CASE WHEN object_type='concert_hall' THEN 'Konzertsaal' WHEN object_type='theatre' THEN 'Theater' WHEN object_type='cinema' THEN 'Kino' ELSE 'Kulturzentrum' END,
              'eng', CASE WHEN object_type='concert_hall' THEN 'concert hall' WHEN object_type='theatre' THEN 'theatre' WHEN object_type='cinema' THEN 'cinema' ELSE 'cultural centre' END) 
              as legend_item,
              CASE WHEN object_type='concert_hall' THEN '",legend_item_id_concert_hall,"'::uuid
              WHEN object_type='theatre' THEN '",legend_item_id_theatre,"'::uuid
              WHEN object_type='cinema' THEN '",legend_item_id_cinema,"'::uuid
              WHEN object_type='museum' THEN '",legend_item_id_museum,"'::uuid
              ELSE '",legend_item_id_cultural_centre,"'::uuid END
              as legend_item_id,
           other_names,address,local_email,operator_email,local_phone,local_website,
operator_website,operator_wikidata, operator, operator_type, image, 
	community_centre_for,
	CASE WHEN capacity_persons IS NULL AND capacity_seats IS NULL THEN NULL
	ELSE CONCAT_WS('; ',capacity_persons, capacity_seats) END AS capacity,
	theatre_genre,
	theatre_type,
	description,museum,level,
	geometry FROM raw_data.osm_cultural_centre)
INSERT INTO ingestion.cultural_centre
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
	'operator_email',operator_email,
	'operator_website',operator_website,
	'operator',operator,
	'community_centre_for',community_centre_for,
	'capacity',capacity,
	'theatre_genre',theatre_genre,
	'theatre_type',theatre_type,
	'description',description,
	'museum_subject',museum,
	'building_level',level
)),
ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.cultural_centre OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.cultural_centre TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.cultural_centre TO pgn_user_airflow;")




### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}

# set to TRUE if you want to update the transformation table even if the checks fail. 
#overrule_checks<-TRUE
#do_dry_run<-TRUE

run_smart_update = function() {
  smart_update_process("cultural_centre", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
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











