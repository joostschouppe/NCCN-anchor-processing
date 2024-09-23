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

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

data_list_id<-"d883a115-05f7-45c1-8398-d555334102ae"
log_folder <- "C:/temp/logs/"

### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))

# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(sf)
library(RPostgres)
library(DBI)

## Data processing libraries
library(dplyr)


## OSM library
library(osmdata)





# EXTRACT ----
# """""""""""""""""" ----


# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list_1 <- list("leisure" = "sports_centre")
features_list_2 <- list("leisure" = "ice_rink", "leisure" = "horse_riding", "leisure" = "stadium", "landuse"="winter_sports", "leisure"="golf_course")
features_list_3 <- list("leisure" = "sports_hall")
features_list_4 <- list(
  list(key = "leisure", value = "track"),
  list(key = "name")
)
features_list_5 <- list(
  list(key = "leisure", value = "pitch"),
  list(key = "name")
)
features_list_6 <- list(
  list(key = "leisure", value = "swimming_pool"),
  list(key = "access", value = "private", negate = TRUE)
)

features_list_7 <- list(
  list(key = "building", value = "sports_centre"),
  list(key = "name"),
  list(key = "leisure", key_does_not_exist = TRUE)
)
features_list_8 <- list(
  list(key = "building", value = "sports_hall"),
  list(key = "name"),
  list(key = "leisure", key_does_not_exist = TRUE)
)


# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("amenity", "sport", "access", "capacity", "community_centre", "description", "leisure", "building", "landuse")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")
datatypes_lines_too <- c("points", "lines", "mpolygon")


### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_1<-download_osm_process(features_list_1, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE )
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_2<-download_osm_process(features_list_2, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_3<-download_osm_process(features_list_3, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})


tryCatch({
  # Call the large function
  osm_4<-download_osm_process(features_list_4, datatypes_lines_too, extra_columns, alternative_overpass_server, keep_region=TRUE, feature_tag_list=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_5<-download_osm_process(features_list_5, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE, feature_tag_list=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_6<-download_osm_process(features_list_6, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE, feature_tag_list=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_7<-download_osm_process(features_list_7, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE, feature_tag_list=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_8<-download_osm_process(features_list_8, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE, feature_tag_list=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

# we can consider throwing away all objects with a private tag, however often they do seem to be relevant features

# merge all layers
osm_all <- bind_rows(osm_1, osm_2, osm_3, osm_4, osm_5, osm_6, osm_7, osm_8)


# select only if amenity is null and not some things that make no sense or are already an anchor
osm_all <- osm_all %>%
  filter(is.na(amenity) | (amenity!="animal_boarding" & amenity!="animal_breeding" & amenity!="animal_shelter" & amenity!="animal_training" & amenity!="arts_centre" & amenity!="events_venue" & amenity!="school" & amenity!="university" & amenity!="theatre" & !(amenity=="community_centre" & community_centre=="cultural_centre")))

# add object type
osm_all<-osm_all %>%
  mutate(object_type=case_when(
    leisure=='swimming_pool' | sport=='swimming' ~ 'swimming_pool',
    leisure=='sports_centre' | leisure=='sports_hall'  ~ 'sports_centre',
    leisure=='ice_rink' | landuse=='winter_sports'  ~ 'winter_sports',
    leisure=='horse_riding'  ~ 'horse_riding',
    leisure=='stadium' ~ 'stadium',
    leisure=='golf_course' ~ 'golf_course',
    leisure=='pitch' | leisure=='track' ~ 'sports_field',
    building=='sports_centre' | building == 'sports_hall' ~ 'incomplete sports_centre',
    TRUE  ~ 'other'
  ))
table(osm_all$object_type)

# select things that are not 'other' (mostly artefacts from site relations)
osm_all <- osm_all %>%
  filter(object_type!="other")

# remove duplicate osm_id's
osm_all <- osm_all %>%
  distinct(osm_id, .keep_all = TRUE)


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




# golf course elements in a large group should lose their name if they are not the outer
join <- join %>%
  mutate(name = ifelse(object_type == "golf_course" & count > 5 & outer_id != osm_id, NA, name)) %>%
  mutate(name_nl = ifelse(object_type == "golf_course" & count > 5 & outer_id != osm_id, NA, name_nl)) %>%
  mutate(name_fr = ifelse(object_type == "golf_course" & count > 5 & outer_id != osm_id, NA, name_fr)) %>%
  mutate(name_de = ifelse(object_type == "golf_course" & count > 5 & outer_id != osm_id, NA, name_de))


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
  mutate(operator_website= ifelse(count > 1, paste(unique(na.omit(operator_website)), collapse = "; "), operator_website)) %>%
  mutate(sport= ifelse(count > 1, paste(unique(na.omit(sport)), collapse = "; "), sport)) %>%
  mutate(capacity= ifelse(count > 1, paste(unique(na.omit(capacity)), collapse = "; "), capacity)) %>%
  mutate(description= ifelse(count > 1, paste(unique(na.omit(description)), collapse = "; "), description))
  
# aggregate names
#if osm_id=outer_id and name is missing, fill it in with "sports"
join<-join %>%
  mutate(name=ifelse(osm_id==outer_id & is.na(name) & count>1, "sports", name))

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
      select(-primary_name, -other_names)  # Remove temporary columns if not needed
  }
  
  return(df)
}


# Define your list of columns for name aggregation
names_to_aggregate <- c("name", "name_nl", "name_fr", "name_de", "alt_name", "short_name", "official_name", "old_name")

# Call the function to aggregate names
join <- aggregate_names(join, names_to_aggregate)



# Filter records based on the count and outer_id conditions
join_filtered <- join %>%
  filter(count == 1 | (count > 1 & outer_id == osm_id))


# if object_type_agg contains stadium, then stadium. If it contains sports_centre, then sports_centre. Otherwise, simply keep the value of the outer
join_filtered <- join_filtered %>%
  mutate(object_type = case_when(
    grepl("stadium", object_type_agg, ignore.case = TRUE) ~ "stadium",
    grepl("sports_centre", object_type_agg, ignore.case = TRUE) ~ "sports_centre",
    TRUE ~ object_type
  ))

# remove line geometries
join_filtered <- join_filtered %>%
  filter(!st_geometry_type(geometry) %in% c("LINESTRING", "MULTILINESTRING"))

# remove results without a name
join_filtered <- join_filtered %>%
  filter(!is.na(name))



# find weird cases:
# don't just merge overlapping features, but also look at very close together features > check how many are close together after merging!
# find objects near to each other
join_filtered_expanded <- join_filtered %>%
  mutate(geometry = st_sfc(geometry)) %>%
  st_set_crs(4326) %>%
  st_transform(31370) %>%
  mutate(geometry = st_buffer(geometry, dist = 20)) %>%
  st_transform(4326) %>%
  mutate(geometry = st_sfc(geometry)) %>%
  st_set_crs(4326)

## first define a set of potential outers
osm_outers2<-join_filtered_expanded %>%
  select(outer_id=osm_id) %>%
  filter(!st_is(geometry, "POINT"))
## then join them to the objects
join2 <- join_filtered_expanded%>%
  select(-outer_id, count)

join2 <- st_join(join2, osm_outers2, join = st_within)

join2<-join2 %>%
  mutate(outer_id=case_when(!is.na(outer_id)~outer_id, TRUE~osm_id)) %>%
  group_by(outer_id) %>%
  mutate(count=case_when(!is.na(outer_id) ~n(), TRUE~NA))


# remove object_type='incomplete sports_centre' (just a building with a name) if they still exist
table(join_filtered$object_type)
join_filtered <- join_filtered %>%
  filter(object_type != "incomplete sports_centre")



# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function




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
	sport, access, capacity,description,
	geometry FROM raw_data.osm_sports)
INSERT INTO ingestion.sports 
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
	'operator',operator,
	'sport',sport,
	'access',access,
	'capacity',capacity,
	'description',description
)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))
                            
### Create transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.sports CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.sports
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
    CONSTRAINT sports_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.sports
(id, original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id::uuid, properties, geometry, created_at FROM ingestion.sports;
")



### Create fdw views ----
fdw_views_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_sports CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_sports
AS
SELECT id,
original_id,
name,
legend_item,
NULL::uuid as best_address_id,
NULL::uuid as capakey_id,
data_list_id,
risk_level,
properties,
properties_secondary,
imported_at,
tags,
deleted_at,
updated_at,
created_at,
created_by,
updated_by,
st_reduceprecision(geometry, 0.000001::double precision) AS geometry,
st_reduceprecision(st_pointonsurface(geometry), 0.000001::double precision) AS geometry_pt,
CASE
  WHEN st_geometrytype(geometry) = ANY (ARRAY['ST_Point'::text, 'ST_LineString'::text]) THEN st_reduceprecision(st_transform(st_buffer(st_transform(geometry, 31370), 20::double precision), 4326), 0.000001::double precision)
  ELSE geometry
  END AS geometry_pg
FROM transformation.sports;
","
GRANT ALL ON TABLE fdw.fdw_sports TO paragon;
")





### Execute the SQL commands ----

create_ingestion_table <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in ingestion_table_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("Ingestion table SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the ingestion table failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


create_transformation_table <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in transformation_table_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("Transformation table SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the transformation table failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


create_fdw_views <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in fdw_views_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("FDW table SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the FDW views failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}



# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("museum", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = join_filtered, schema = "raw_data", table_name = "osm_sports") 
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
  #create_fdw_views()
}

if(F){
  main_function()
}
