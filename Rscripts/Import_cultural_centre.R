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
#features_list <- list("amenity" = "theatre", "amenity" = "community_centre", "amenity" = "arts_centre" )
features_list_1 <- list("amenity" = "theatre")
features_list_2 <- list("amenity" = "community_centre")
features_list_3 <- list("amenity" = "arts_centre")
features_list_4 <- list("amenity" = "concert_hall")
features_list_5 <- list("amenity" = "cinema")
features_list_6 <- list("amenity" = "music_venue")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("amenity", "community_centre", "community_centre:for", "capacity:persons", "capacity:seats", "operator", "theatre:genre", "theatre:type", "training")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----

osm_1 <- run_process(
  download_osm_process(features_list_1, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
  paste0("OSM download & processing for ", paste(paste(names(features_list_1), unlist(features_list_1), sep = "="), collapse = ", "), collapse = ", ")
)

osm_2 <- run_process(
  download_osm_process(features_list_2, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
  paste0("OSM download & processing for ", paste(paste(names(features_list_2), unlist(features_list_2), sep = "="), collapse = ", "), collapse = ", ")
)

osm_3 <- run_process(
  download_osm_process(features_list_3, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
  paste0("OSM download & processing for ", paste(paste(names(features_list_3), unlist(features_list_3), sep = "="), collapse = ", "), collapse = ", ")
)

osm_4 <- run_process(
  download_osm_process(features_list_4, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
  paste0("OSM download & processing for ", paste(paste(names(features_list_4), unlist(features_list_4), sep = "="), collapse = ", "), collapse = ", ")
)

osm_5 <- run_process(
  download_osm_process(features_list_5, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
  paste0("OSM download & processing for ", paste(paste(names(features_list_5), unlist(features_list_5), sep = "="), collapse = ", "), collapse = ", ")
)

osm_6 <- run_process(
  download_osm_process(features_list_6, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
  paste0("OSM download & processing for ", paste(paste(names(features_list_6), unlist(features_list_6), sep = "="), collapse = ", "), collapse = ", ")
)



# from osm_1, throw away if theatre_type=open_air
osm_1<-osm_1 %>%
  filter(is.na(theatre_type) | (theatre_type!="open_air" & theatre_type!="amphi"))

# from osm_2, keep only if community_centre=cultural_centre
osm_2<-osm_2 %>%
  filter(community_centre=="cultural_centre")

# from osm_3 keep if training not arts
osm_3<-osm_3 %>%
  filter(is.na(training) | training!="art")

# merge all
osm_all<-rbind(osm_1,osm_2,osm_3,osm_4,osm_5,osm_6)

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

# remove if no name
osm_all<-osm_all %>%
  filter(!is.na(name) & is.na(training))

# add object type
osm_all<-osm_all %>%
  mutate(object_type=case_when(
    (amenity=='theatre' & theatre_type=='concert_hall') | amenity=='concert_hall' ~ 'concert_hall',
    amenity=='theatre' ~ 'theatre',
    community_centre=='cultural_centre' | amenity=='arts_centre' ~ 'cultural_centre',
    amenity=='cinema' ~ 'cinema',
    TRUE  ~ 'other'
  ))

# remove other
osm_all<-osm_all %>%
  filter(object_type!='other')


# detect objects on top of other objects in the dataset
## first define a set of potential outers
osm_outers<-osm_all %>%
  select(outer_id=osm_id) %>%
  filter(!st_is(geometry, "POINT"))
## then join them to the objects
join <- st_join(osm_all, osm_outers, join = st_within)


# count the number of times an inner is used (to later find objects that are linked to itself and something else too, or to more than one outer)
join<-join %>%
  mutate(outer_id=case_when(!is.na(outer_id)~outer_id, TRUE~osm_id)) %>%
  group_by(osm_id) %>%
  mutate(count_inner=case_when(!is.na(outer_id) ~n(), TRUE~NA))
# remove if the inner is used by an outer but still refers to itself as well
join<-join %>%
  filter(count_inner==1 | (count_inner>1 & osm_id!=outer_id))


# add a count with the number of times an outer is used
join<-join %>%
  mutate(outer_id=case_when(!is.na(outer_id)~outer_id, TRUE~osm_id)) %>%
  group_by(outer_id) %>%
  mutate(count=case_when(!is.na(outer_id) ~n(), TRUE~NA))

# count inners again and give them an order_inner number, where 1 is for the case linked to the biggest count (outer)
join<-join %>%
  group_by(osm_id) %>%
  mutate(count_inner=case_when(!is.na(outer_id) ~n(), TRUE~NA)) %>%
  arrange(desc(count)) %>%
  mutate(order_inner = row_number())

# if count_inner>1, take the first one (i.e. the one linked to the biggest polygon)
join<-join %>%
  filter(count_inner==1 | (count_inner>1 & order_inner==1))



# create aggregated variables
join <- join %>%
  group_by(outer_id) %>%
  mutate(object_type_agg = ifelse(count > 1, paste(unique(object_type), collapse = "; "), object_type)) %>%
  mutate(operator= ifelse(count > 1, paste(unique(na.omit(operator)), collapse = "; "), operator)) %>%
  mutate(addr_city= ifelse(count > 1, paste(unique(na.omit(addr_city)), collapse = "; "), addr_city)) %>%
  mutate(addr_housenumber= ifelse(count > 1, paste(unique(na.omit(addr_housenumber)), collapse = "; "), addr_housenumber)) %>%
  mutate(addr_street= ifelse(count > 1, paste(unique(na.omit(addr_street)), collapse = "; "), addr_street)) %>%
  mutate(addr_postcode= ifelse(count > 1, paste(unique(na.omit(addr_postcode)), collapse = "; "), addr_postcode)) %>%
  mutate(contact_email= ifelse(count > 1, paste(unique(na.omit(contact_email)), collapse = "; "), contact_email)) %>%
  mutate(email= ifelse(count > 1, paste(unique(na.omit(email)), collapse = "; "), email)) %>%
  mutate(website= ifelse(count > 1, paste(unique(na.omit(website)), collapse = "; "), website)) %>%
  mutate(contact_website= ifelse(count > 1, paste(unique(na.omit(contact_website)), collapse = "; "), contact_website)) %>%
  mutate(phone= ifelse(count > 1, paste(unique(na.omit(phone)), collapse = "; "), phone)) %>%
  mutate(contact_phone= ifelse(count > 1, paste(unique(na.omit(contact_phone)), collapse = "; "), contact_phone)) %>%
  mutate(opening_hours= ifelse(count > 1, paste(unique(na.omit(opening_hours)), collapse = "; "), opening_hours)) %>%
  mutate(mobile= ifelse(count > 1, paste(unique(na.omit(mobile)), collapse = "; "), mobile)) %>%
  mutate(contact_mobile= ifelse(count > 1, paste(unique(na.omit(contact_mobile)), collapse = "; "), contact_mobile)) %>%
  mutate(check_date= ifelse(count > 1, paste(unique(na.omit(check_date)), collapse = "; "), check_date)) %>%
  mutate(wikidata= ifelse(count > 1, paste(unique(na.omit(wikidata)), collapse = "; "), wikidata)) %>%
  mutate(capacity_persons= ifelse(count > 1, paste(unique(na.omit(capacity_persons)), collapse = "; "), capacity_persons)) %>%
  mutate(capacity_seats= ifelse(count > 1, paste(unique(na.omit(capacity_seats)), collapse = "; "), capacity_seats)) %>%
  mutate(theatre_type= ifelse(count > 1, paste(unique(na.omit(theatre_type)), collapse = "; "), theatre_type)) %>%
  mutate(theatre_genre= ifelse(count > 1, paste(unique(na.omit(theatre_genre)), collapse = "; "), theatre_genre)) %>%
  mutate(operator_wikidata= ifelse(count > 1, paste(unique(na.omit(operator_wikidata)), collapse = "; "), operator_wikidata)) %>%
  mutate(phone_2= ifelse(count > 1, paste(unique(na.omit(phone_2)), collapse = "; "), phone_2)) %>%
  mutate(alt_website= ifelse(count > 1, paste(unique(na.omit(alt_website)), collapse = "; "), alt_website)) %>%
  mutate(operator_website= ifelse(count > 1, paste(unique(na.omit(operator_website)), collapse = "; "), operator_website)) %>%
  mutate(community_centre_for= ifelse(count > 1, paste(unique(na.omit(community_centre_for)), collapse = "; "), community_centre_for))
  
  
# aggregate names
aggregate_names <- function(df, names_to_aggregate) {
  for (col in names_to_aggregate) {
    df <- df %>%
      group_by(outer_id) %>%
      mutate(
        primary_name = ifelse(!is.na(get(col))[osm_id == outer_id][1], get(col)[osm_id == outer_id][1], NA),
        other_names = paste(unique(na.omit(get(col)[osm_id != outer_id & get(col) != primary_name])), collapse = ", "),
        across(col, ~ ifelse(!is.na(.), case_when(
          count > 1 & !is.na(other_names) & other_names != "" ~ ifelse(is.na(primary_name) | primary_name == "", other_names, paste0(primary_name, " (", other_names, ")")),
          TRUE ~ .
        ), .))
      ) %>%
      ungroup() %>%
      select(-primary_name, -other_names)  # Remove temporary columns
  }
  
  return(df)
}


# Define your list of columns to aggregate
names_to_aggregate <- c("name", "name_nl", "name_fr", "name_de", "alt_name", "short_name", "official_name", "old_name")

# Call the function to aggregate names
join <- aggregate_names(join, names_to_aggregate)



# Filter records based on the count and outer_id conditions
join_filtered <- join %>%
  filter(count == 1 | (count > 1 & outer_id == osm_id))

#if count>1 & object_type_agg!=object_type then object_type:cultural_centre
join_filtered <- join_filtered %>%
  mutate(object_type = ifelse(count > 1 & object_type_agg != object_type, "cultural_centre", object_type))

# make available outside the function
join_filtered <<- join_filtered

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
              ELSE '",legend_item_id_cultural_centre,"'::uuid END
              as legend_item_id,
           CASE WHEN short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL 
	ELSE CONCAT_WS('; ',short_name, official_name, alt_name, old_name) END AS other_names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || addr_postcode, ' ' || addr_city),', ') END
	AS address,
CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_email, email) END AS local_email,
operator_email,
CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS local_phone,
CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS local_website,
	operator_website,
	operator,
	community_centre_for,
	CASE WHEN capacity_persons IS NULL AND capacity_seats IS NULL THEN NULL
	ELSE CONCAT_WS('; ',capacity_persons, capacity_seats) END AS capacity,
	theatre_genre,
	theatre_type,
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
	'phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
	'operator',operator,
	'community_centre_for',community_centre_for,
	'capacity',capacity,
	'theatre_genre',theatre_genre,
	'theatre_type',theatre_type
)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.cultural_centre OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.cultural_centre TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.cultural_centre TO pgn_user_airflow;")




### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}

# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-overrule_checks

#do_dry_run<-TRUE
run_smart_update = function() {
  smart_update_process("cultural_centre", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = join_filtered, schema = "raw_data", table_name = "osm_cultural_centre")
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}











