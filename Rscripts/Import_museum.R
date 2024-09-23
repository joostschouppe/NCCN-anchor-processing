## ---------------------------
##
## Script name: Import musea
##
## Purpose of script: Load OSM musea & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-07-04
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

data_list_id<-"09faf6c9-fb36-44ff-9ea3-3643cca17df6"
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
features_list <- list("tourism" = "museum")

# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("description", "museum", "level", "tourism")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----



tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE )
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})


# remove if no name
osm_all<-osm_all %>%
  filter(!is.na(name) & tourism=="museum")


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
  mutate(operator_wikidata= ifelse(count > 1, paste(unique(na.omit(operator_wikidata)), collapse = "; "), operator_wikidata)) %>%
  mutate(phone_2= ifelse(count > 1, paste(unique(na.omit(phone_2)), collapse = "; "), phone_2)) %>%
  mutate(alt_website= ifelse(count > 1, paste(unique(na.omit(alt_website)), collapse = "; "), alt_website)) %>%
  mutate(operator_website= ifelse(count > 1, paste(unique(na.omit(operator_website)), collapse = "; "), operator_website)) %>%
  mutate(description= ifelse(count > 1, paste(unique(na.omit(description)), collapse = "; "), description)) %>%
  mutate(museum= ifelse(count > 1, paste(unique(na.omit(museum)), collapse = "; "), museum)) %>%
  mutate(level= ifelse(count > 1, paste(unique(na.omit(level)), collapse = "; "), level)) %>%
  mutate(tourism= ifelse(count > 1, paste(unique(na.omit(tourism)), collapse = "; "), tourism))

  
  
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
      select(-primary_name, -other_names)  # Remove temporary columns if not needed
  }
  
  return(df)
}


# Define your list of columns to aggregate
names_to_aggregate <- c("name", "name_nl", "name_fr", "name_de", "alt_name", "short_name", "official_name", "old_name")

# Call the function to aggregate names
join <- aggregate_names(join, names_to_aggregate)



# Filter records based on the count and outer_id conditions
join_filtered <- join %>%
  filter(count == 1 | (count > 1 & outer_id == osm_id)) %>%
  select(-count,-outer_id)

# confirm as SF dataset
join_filtered <- st_as_sf(join_filtered, "geometry")


# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function




# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.museum CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.museum
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
  CONSTRAINT museum_pkey PRIMARY KEY (id)
);",paste0("WITH 
cleaned as (SELECT
'https://osm.org/' || osm_id as original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', name,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
            jsonb_build_object(
              'dut', 'museum',
              'fre', 'musée',
              'ger', 'Museum',
              'eng', 'museum') as legend_item,
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
operator_website,operator_wikidata,operator,
description,museum,level,
	geometry FROM raw_data.osm_museum)
INSERT INTO ingestion.museum 
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
	'description',description,
	'museum_subject',museum,
	'building_level',level
)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))
                            
### Create transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.museum CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.museum
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
    CONSTRAINT museum_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.museum
(id, original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id::uuid, properties, geometry, created_at FROM ingestion.museum;
")


### Create fdw views ----
fdw_views_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_museum CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_museum
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
FROM transformation.museum;
","
GRANT ALL ON TABLE fdw.fdw_museum TO paragon;
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
  CreateImportTable(dataset = join_filtered, schema = "raw_data", table_name = "osm_museum") 
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
  #create_fdw_views()
}

if(F){
  main_function()
}

