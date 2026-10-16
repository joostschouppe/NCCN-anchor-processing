## ---------------------------
##
## Script name: Import sports centers
##
## Purpose of script: Load OSM sports centers & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-07-11
##
##
## ---------------------------




# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# External IDs
data_list_id<-"d883a115-05f7-45c1-8398-d555334102ae"

li_sport_centre <- "be58439a-45c0-4610-a4ae-080b908c9e24"
li_sport_golf <- "e1be3094-1b92-4000-8a6a-2beae0ffe6f3"
li_sport_horse <- "dbfdfb29-e88a-4553-bb4f-7e63d729bf4d"
li_sport_pitch <- "dcf3a220-a84b-4d8f-aac5-be8fb96f052d"
li_sport_pool <- "6b88c984-7d6f-4361-8731-124deb7c04b1"
li_sport_stadium <- "dc36d8d9-3774-4df3-934a-fbad4b8f86ec"
li_sport_winter <- "95c1165b-a70f-4647-a82c-e9b766c6e35c"
  


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

# everything loaded via utils





# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {

# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----
features_list_pp <- list(or_group(
  tag_filter("leisure", "sports_centre"),
  tag_filter("leisure", "ice_rink"),
  tag_filter("leisure", "horse_riding"),
  tag_filter("leisure", "stadium"),
  tag_filter("leisure", "golf_course"),
  tag_filter("leisure", "sports_hall"),
  tag_filter("leisure", "pitch"),
  tag_filter("leisure", "swimming_pool"),
  tag_filter("leisure", "swimming_area"),
  tag_filter("landuse", "winter_sports"),
  tag_filter("building", "sports_centre"),
  tag_filter("building", "sports_hall")
))


features_list_plp <- list("leisure" = "track")

# Define extra tags to use as columns for properties
extra_columns <- c("amenity", "sport", "access", "capacity", "community_centre", "description", "leisure", "building", "landuse", "tourism")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")
datatypes_lines_too <- c("points", "lines", "mpolygon")


### Actual OSM download & transformation ----

osm_pp <- run_process(
  download_osm_process(features_list_pp, datatypes, extra_columns, keep_region=TRUE),
  paste0("OSM download & processing for ", describe_features_list(features_list_pp))
)

osm_plp <- run_process(
  download_osm_process(features_list_plp, datatypes, extra_columns, keep_region=TRUE),
  paste0("OSM download & processing for ", describe_features_list(features_list_plp))
)


# we can consider throwing away all objects with a private tag, however often they do seem to be relevant features

  
# merge all layers
osm_all <- bind_rows(osm_pp, osm_plp)

# remove objects already in "higher" layers
## get the relevant object ids
con_pg <- get_con()
remove_objects <- dbGetQuery(con_pg, paste0("WITH presel AS (SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.embassy WHERE deleted_at IS NULL
                                     UNION ALL
                                     SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.schools WHERE deleted_at IS NULL
                                     UNION ALL
                                     SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.cultural_centre WHERE deleted_at IS NULL
                                     UNION ALL
                                     SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.nursing_homes WHERE deleted_at IS NULL)
                                     SELECT distinct(osm_id) AS osm_id from presel"))
dbDisconnect(con_pg)
## anti-join to throw them out
osm_all <- osm_all %>%
  anti_join(remove_objects, by = "osm_id")



osm_all <- osm_all %>%
# remove private swimming pools
  filter(!(leisure=="swimming_pool" & access=="private") | is.na(access)) %>%
# remove things with no name if track or pitch
  filter(!((leisure=="track" | leisure=="pitch") & is.na(name))) %>%
# if building and no leisure, remove if no name
  filter(!(!is.na(building) & is.na(leisure) & is.na(name))) %>%
# remove objects already included in accommodations script
  filter(is.na(tourism) | !tourism %in% c("hotel", "hostel", "motel", "camp_site", "chalet"))


# select only if amenity is null and not some things that make no sense or are already an anchor
osm_all <- osm_all %>%
  filter(is.na(amenity) | (amenity!="animal_boarding" & amenity!="animal_breeding" & amenity!="animal_shelter" & amenity!="animal_training" & amenity!="arts_centre" & amenity!="events_venue" & amenity!="school" & amenity!="university" & amenity!="theatre" & !(amenity=="community_centre" & community_centre=="cultural_centre")))

# add object type
osm_all<-osm_all %>%
  mutate(object_type=case_when(
    leisure=='swimming_pool' | sport=='swimming' | leisure=='swimming_area' ~ 'swimming_pool',
    leisure=='sports_centre' | leisure=='sports_hall'  ~ 'sports_centre',
    leisure=='ice_rink' | landuse=='winter_sports'  ~ 'winter_sports',
    leisure=='horse_riding'  ~ 'horse_riding',
    leisure=='stadium' ~ 'stadium',
    leisure=='golf_course' ~ 'golf_course',
    leisure=='pitch' | leisure=='track' ~ 'sports_field',
    building=='sports_centre' | building == 'sports_hall' ~ 'incomplete sports centre',
    TRUE  ~ 'other'
  ))
#table(osm_all$object_type)

# select things that are not 'other' (mostly artefacts from site relations)
osm_all <- osm_all %>%
  filter(object_type!="other")

# remove duplicate osm_id's
osm_all <- osm_all %>%
  distinct(osm_id, .keep_all = TRUE)

# keep the original version of object_type
osm_all <- osm_all %>%
  mutate(object_type_orig=object_type)

# detect objects on top of other objects in the dataset
## with extra hook: golf course elements in a large group should lose their name if they are not the outer
result <- osmspatial_deduplication(
  osm_dataset = osm_all,
  custom_columns_to_aggregate = c("sport", "capacity", "description", "object_type"),
  pre_aggregation_hook = function(join) {
    name_cols <- c("name", "name_nl", "name_fr", "name_de")
    join %>%
      mutate(across(
        any_of(name_cols),
        ~ ifelse(object_type == "golf_course" & count > 5 & outer_id != osm_id, NA, .x)
      ))
  }
)
join_filtered <- result$data
#osm_diagnostic <- result$diagnostic

# if object_type (aggregated) contains stadium, then use stadium. If it contains sports_centre, then sports_centre. Otherwise, simply keep the value of the original object type (of the outer)
join_filtered <- join_filtered %>%
  mutate(object_type = case_when(
    grepl("stadium", object_type, ignore.case = TRUE) ~ "stadium",
    grepl("sports_centre", object_type, ignore.case = TRUE) ~ "sports_centre",
    TRUE ~ object_type_orig
  ))

# remove line geometries
join_filtered <- join_filtered %>%
  filter(!st_geometry_type(geometry) %in% c("LINESTRING", "MULTILINESTRING"))

# remove results without a name
join_filtered <- join_filtered %>%
  filter(!is.na(name))


# extract a dataset for use in MapRoulette with incomplete swimming pools
#maproulette <- join_filtered %>%
#  filter(leisure == "swimming_pool" & !is.na(language)) %>%
#  select(osm_id,name)
#st_write(maproulette, paste0(log_folder,"maproulette_swimming_pools.geojson"))
#occasionally have a look at these: most are the swimming areas that belong to a larger facility. This tag is ONLY for the water area itself. The facility should be mapped with leisure=sports_centre/sports_hall + sport=swimming

# remove object_type='incomplete sports_centre' (just a building with a name) if they still exist and "lose" swimming pools
#table(join_filtered$object_type)
join_filtered <- join_filtered %>%
  filter(object_type_orig != "incomplete sports centre") %>%
  filter(leisure!="swimming_pool" | is.na(leisure))

# standardize the usual OSM columns
join_filtered <- build_osm_derived_columns(join_filtered)


# Upload to raw data ----
CreateImportTable(dataset = join_filtered, schema = "raw_data", table_name = "osm_sports") 

  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function



# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.sports CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.sports
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
  CONSTRAINT sports_pkey PRIMARY KEY (id)
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
              'dut', CASE WHEN object_type='golf_course' THEN 'golfterrein' WHEN object_type='horse_riding' THEN 'paardrijden' WHEN object_type='sports_centre' THEN 'sportcentrum' 
              WHEN object_type='sports_field' THEN 'sportveld' WHEN object_type='stadium' THEN 'stadium' WHEN object_type='swimming_pool' THEN 'zwembad' WHEN object_type='winter_sports' THEN 'wintersport' END,
              'fre', CASE WHEN object_type='golf_course' THEN 'terrain de golf' WHEN object_type='horse_riding' THEN 'équitation' WHEN object_type='sports_centre' THEN 'centre sportif' 
              WHEN object_type='sports_field' THEN 'terrain de sport' WHEN object_type='stadium' THEN 'stade' WHEN object_type='swimming_pool' THEN 'piscine' WHEN object_type='winter_sports' THEN 'sports d''hiver' END,
              'ger', CASE WHEN object_type='golf_course' THEN 'Golfplatz' WHEN object_type='horse_riding' THEN 'Reiten' WHEN object_type='sports_centre' THEN 'Sportzentrum' 
              WHEN object_type='sports_field' THEN 'Sportplatz' WHEN object_type='stadium' THEN 'Stadion' WHEN object_type='swimming_pool' THEN 'Schwimmbad' WHEN object_type='winter_sports' THEN 'Wintersport' END,
              'eng', CASE WHEN object_type='golf_course' THEN 'golf course' WHEN object_type='horse_riding' THEN 'horse riding' WHEN object_type='sports_centre' THEN 'sports centre' 
              WHEN object_type='sports_field' THEN 'sports field' WHEN object_type='stadium' THEN 'stadium' WHEN object_type='swimming_pool' THEN 'swimming pool' WHEN object_type='winter_sports' THEN 'winter sports' END)
              as legend_item,
            CASE WHEN object_type='golf_course' THEN '",li_sport_golf,"'::uuid
              WHEN object_type='horse_riding' THEN '",li_sport_horse,"'::uuid
              WHEN object_type='sports_centre' THEN '",li_sport_centre,"'::uuid
              WHEN object_type='sports_field' THEN '",li_sport_pitch,"'::uuid
              WHEN object_type='stadium' THEN '",li_sport_stadium,"'::uuid
              WHEN object_type='swimming_pool' THEN '",li_sport_pool,"'::uuid
              WHEN object_type='winter_sports' THEN '",li_sport_winter,"'::uuid END
              as legend_item_id,
           other_names,address,local_email,operator_email,local_phone,local_website,
operator_website,operator_wikidata, operator, operator_type, image, 
	sport, access, capacity,description,
	geometry FROM raw_data.osm_sports)
INSERT INTO ingestion.sports 
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
	'sport',sport,
	'access',access,
	'capacity',capacity,
	'description',description
)),
ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.sports OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.sports TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.sports TO pgn_user_airflow;")

### Execute the SQL commands ----


create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
#overrule_checks<-TRUE

#do_dry_run <- TRUE

run_smart_update = function() {
  smart_update_process("sports", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
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


