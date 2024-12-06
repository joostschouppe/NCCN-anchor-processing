## ---------------------------
##
## Script name: Import dams & locks from OSM
##
## Purpose of script: Load OSM water infrastructure data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-24
##
##
## ---------------------------

# todo: undo delete of transformation was id = ebef328a-36b7-42b1-94fb-e0945c28c2b3 & relation https://osm.org/relation/13409923 and check geometry
#risk level

# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

data_list_id<-"b3f833af-1c7a-4d03-8ceb-ffd55bfea5f8"
log_folder <- "C:/temp/logs/"

### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))



# EXTRACT ----
# """""""""""""""""" ----



# Download OSM data ----


### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_lock <- list("lock" = "yes")
features_boatlift <- list("waterway" = "boat_lift")
features_dam <- list("waterway" = "dam")


# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("CEMT", "lock", "lock_name", "lock_name:nl", "lock_name:fr", "lock_name:de", "lock_ref", "lock:height", "length", "maxdraft", "maxdraught", "maxheight", "maxlength", "maxwidth", "seamark:name", "waterway","boat_lift")

# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes_lock <- c("lines")
datatypes_boatlift <- c("points", "lines", "mpolygon")
datatypes_dam <- c("lines", "mpolygon")


### Actual OSM download & transformation ----



tryCatch({
  # Call the OSM function
  osm_lock<-download_osm_process(features_lock, datatypes_lock, extra_columns, alternative_overpass_server)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})
tryCatch({
  # Call the OSM function
  osm_boatlift<-download_osm_process(features_boatlift, datatypes_boatlift, extra_columns, alternative_overpass_server)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})
tryCatch({
  # Call the OSM function
  osm_dam<-download_osm_process(features_dam, datatypes_dam, extra_columns, alternative_overpass_server)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})



#LOCK:only if line & NOT also boat-lift
osm_lock <- osm_lock %>%
  filter(lock == "yes" & !grepl("boat_lift", waterway))
osm_lock <- osm_lock %>%
  mutate(
    legend_item = "lock")


# BOAT LIFT: keep only if it has a name. Point, line or polygon
osm_boatlift <- osm_boatlift %>%
  mutate(
    legend_item = case_when(
      waterway == "boat_lift" & !is.na(name) ~ "boat_lift",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(legend_item))

# DAM: keep only if it is line or polygon, if it has a name

osm_dam <- osm_dam %>%
  filter(!is.na(name)) %>%
  mutate(
    legend_item = case_when(
      waterway == "dam" ~ "dam",
      TRUE ~ NA_character_
    )
  )





# merge points and polygons
osm_all <- bind_rows(osm_lock, osm_boatlift, osm_dam)




# Upload to raw data ----

## Prepare connection
get_con<-function(){
  con_pg <- dbConnect(Postgres(),
                      user=postgres_user, 
                      password=postgres_password,
                      host=db_host_name,
                      dbname=db_name,
                      port=5432, 
                      sslmode = 'prefer')
  return(con_pg)
}


### Upload raw data data ----

CreateImportTable<-function(dataset, schema, table_name){
  if(exists("dataset")){
    con_pg<-get_con()
    table_id <- DBI::Id(
      schema  = schema,
      table   = table_name
    )
    table_id_t <- paste0(schema,".",table_name)
    start<-Sys.time()
    print(paste0("Start :",format(Sys.time(), "%a %b %d %X %Y")))
    print(paste0("Import data into postgresql table ", table_id_t))
    dbWriteTable(con_pg, table_id, dataset, overwrite = TRUE, row.names = FALSE )
    
    print("ID primary key")
    query <- paste("ALTER TABLE ", table_id_t,
                   "ADD COLUMN ogc_fid SERIAL;")
    dbExecute(con_pg, query)
    query <- paste("ALTER TABLE ", table_id_t,
                   "ADD PRIMARY KEY (ogc_fid);")
    dbExecute(con_pg, query)
    
    ## Close connection --------------------------------------------------------
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
    
  }else{
    print(paste0("Error, the geojson you wanted to import into ", table_id_t, "does not exist, try again"))
  }
}




# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.locks_dams CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.locks_dams
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
    CONSTRAINT osm_locks_dams_pkey PRIMARY KEY (id)
  );
",paste0("
WITH 
cleaned as (SELECT
'https://osm.org/' || osm_id AS original_id,
legend_item AS legend_item_orig,
CASE WHEN legend_item = 'lock' THEN
    	jsonb_strip_nulls(jsonb_build_object(
			'und', CASE WHEN lock_name IS NULL THEN 'lock' ELSE lock_name END, --we dont use name because thats the name of the river not the lock
    		'fre', CONCAT_WS(lock_name_fr,' (' || lock_ref || ')'),
    		'ger', CONCAT_WS(lock_name_de::text,' (' || lock_ref || ')'),
    		'dut', CONCAT_WS(lock_name_nl,' (' || lock_ref || ')')))
	WHEN legend_item = 'dam' THEN
    	jsonb_strip_nulls(jsonb_build_object('und', CASE WHEN name IS NULL THEN 'dam' ELSE name END,
    	'fre', name_fr,
    	'ger', name_de::text,
    	'dut', name_nl))
	ELSE
    	jsonb_strip_nulls(jsonb_build_object('und', CASE WHEN name IS NULL THEN 'boat lift' ELSE name END,
    	'fre', name_fr,
    	'ger', name_de::text,
    	'dut', name_nl))
	END as name,
CASE WHEN legend_item = 'lock' THEN
	jsonb_build_object(
	'dut', 'sluis',
	'fre', 'écluse',
	'ger', 'Schleuse')
	WHEN legend_item = 'boat_lift' THEN
	jsonb_build_object(
	'dut', 'scheepslift',
	'fre', 'ascenseur à bateaux',
	'ger', 'Schiffshebewerk')
	ELSE
	jsonb_build_object(
	'dut', 'dam',
	'fre', 'barrage',
	'ger', 'Talsperre')	END as legend_item,
CASE WHEN seamark_name IS NULL AND short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL
ELSE CONCAT_WS('; ',seamark_name, short_name, official_name, alt_name, old_name) END AS other_names,
CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_email, email) END AS local_email,
operator_email,
CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS local_phone,
CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS local_website,
operator_website,
operator_wikidata, operator, operator_type, image, 
cemt, lock_height, length, 
CASE WHEN maxdraft IS NULL AND maxdraught IS NULL THEN NULL
ELSE CONCAT_WS('; ',maxdraught, maxdraft) END AS maxdraught,
maxheight, maxlength, maxwidth,		
geometry
FROM raw_data.osm_locks_dams)


INSERT INTO ingestion.locks_dams 
(original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",data_list_id,"' as data_list_id,
CASE WHEN legend_item_orig = 'lock' THEN
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'CEMT', cemt,
	'lock_height', lock_height, 
	'lock_length', length, 
	'lock_maxdraft', maxdraught,
	'lock_maxheight', maxheight, 
	'lock_maxlength', maxlength, 
	'lock_maxwidth', maxwidth,
	'other_names', other_names,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image))
ELSE
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'other_names', other_names,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image))
END as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))

### Create SQL for transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.locks_dams CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.locks_dams
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
    CONSTRAINT locks_dams_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.locks_dams 
(original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT original_id, name, legend_item, data_list_id::uuid, properties, geometry, created_at FROM ingestion.osm_locks_dams;
")



### Create fdw views ----
fdw_views_sql <- c("
CREATE OR REPLACE VIEW fdw.fdw_locks_dams
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
geometry,
st_pointonsurface(geometry) AS geometry_pt
FROM transformation.locks_dams;
","
ALTER TABLE fdw.fdw_locks_dams
OWNER TO paragon;
","
GRANT SELECT ON TABLE fdw.fdw_locks_dams TO fdw4dev;
","
GRANT ALL ON TABLE fdw.fdw_locks_dams TO paragon;
")

### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}
create_fdw_views <- function() {execute_sql_commands(fdw_views_sql, "FDW view")}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----


# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("locks_dams", 50, 250, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



main_function = function() {
  CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_locks_dams")  
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table() should not be needed
  #create_fdw_views() should not be needed
}


if(F){
  main_function()
}


