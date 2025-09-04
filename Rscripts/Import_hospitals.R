## ---------------------------
##
## Script name: OSM hospitals
##
## Purpose of script: load OSM hospitals to proto-anchors
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-16
##
##
## ---------------------------




# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# External IDs
data_list_id_osm<-"bd623971-34e3-4518-a220-eb8d26d757ad"
data_list_id_helipad<-"14d5bc60-ffb0-4565-b132-fc4bd9dd156d"
data_list_id_emergency_entrance<-"6f0fbe5c-7731-40e7-b50b-33c4aecd16a5"

legend_item_hospital <- "9018a8e4-d69b-4447-8012-760999f68f68"
legend_item_hospital_helipad <- "8d3feb4d-be49-4760-abb9-ac0741481e25"
legend_item_hospital_emergency <- "926d3794-da66-4708-883e-7dce4997db4b"


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





# Goals
## Load hospitals from OSM
## Keep interesting attributes
## Check for hospital within hospital
## add emergency wards, but without creating a full anchor (pass info on to Marc to add as vector tile layer)
## add helipads as a separate layer

# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {

# Load & transform hospital data ----

### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list <- list("amenity"="hospital")
# Define extra tags to use as columns for properties
extra_columns <- c("amenity","description","email","emergency","emergency:phone","fax","full_name","healthcare", "healthcare:speciality","loc_name","opening_hours:visitors","start_date", "ref:fps_health:recognition", "ref:fps_health:campus","emergency_ward")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")

### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

# remove objects that do NOT actually have a hospital tag (this is an artifact from the site relation)
osm_all <- osm_all %>%
  filter(amenity=="hospital")

# remove emergency ward entrances (these are interesting, but not actual hospitals)
osm_all <- osm_all %>%
  filter(emergency!="emergency_ward_entrance" | is.na(emergency))

# Find all hospitals within other hospitals
joined_all <- st_join(osm_all, osm_all, join = st_within)

# there is now a row for every hospital where it intersects with itself, and a row for every time it intersects with another one.
# Filter out just the ones that are within another one, in Belgium
joined_all <- joined_all %>%
  filter(osm_id.x != osm_id.y & !is.na(language.x))

# this list was reviewed at the start of the processing for Paragon. We accept that there may be hospitals within hospitals, but they should have a name in this case. Otherwise they are likely to be errors.
# stop if in Belgium there are any hospitals within hospitals without a name
if (any(is.na(joined_all$name.x))) {
  test <- joined_all %>%
    filter(is.na(name.x))
  stop(paste("There are hospitals within hospitals without a name:", test$osm_id.x))
}

# possible st_within makes more sense
#write.csv(st_drop_geometry(joined_all), file = "c:/temp/joined_data_poly.csv")
# reviewed 17/5/2024: quite a few sites had duplicate geometries, with info spread out over them; these were remapped with most info on the outer and deletion of the inner if it wasn't a "subhospital"

# if healthcare_speciality contains psychiatry, child_psychiatry or neuropsychiatry, set risk_level=3, else risk_level=2
osm_all <- osm_all %>%
  mutate(risk_level = case_when(
    grepl("psychiatry|child_psychiatry|neuropsychiatry", healthcare_speciality) ~ 3,
    str_detect(name, "universit") |
      str_detect(name_nl, "universit") |
      str_detect(name_fr, "universit") |
      str_detect(name_de, "universit") | operator_type == "university" ~ 3,
    TRUE ~ 2  # Keep other values as 2
  ))

# make available outside the function
osm_all <<- osm_all


# Load & transform helipads ----

# Define the list of features
features_list <-list("operator:type"="hospital")
# Define extra tags to use as columns for properties
extra_columns <- c("description","no:network","not:network","icao","maxweight","reservation","surface","aeroway","diameter","network")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")

### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_helipad<-download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

# remove non-helipad objects
osm_helipad <- osm_helipad %>%
  filter(aeroway=="helipad" | aeroway=="heliport")

# report inconsistency if 
## a helipad that did not get an ICOA code and also wasn't flagged as being outside of the network (i.e. unreviewed): icoa is empty & no:network is not empty
## or when it has an ICAO code but also has a tag saying it doesn't have a network (not:network is not empty but icoa is also not empty)
helipad_test <- osm_helipad %>%
  filter((is.na(icao) & (is.na(no_network) & is.na(not_network))) | 
         (!is.na(icao) & (!is.na(no_network) | !is.na(no_network))))
cat(paste("Inconsistencies in helipad data: ",nrow(helipad_test),"\n"))


# replace the geometry of heliports with the geometry of the helipad within it (if there's only one)
## select heliports
osm_heliport <- osm_helipad %>%
  filter(aeroway=="heliport")

## calculate bbox for each row
osm_heliport_bbox <- osm_helipad %>%
  filter(aeroway == "heliport") %>%
  rowwise() %>%
  mutate(
    minx = as.numeric(st_bbox(geometry)["xmin"]),
    miny = as.numeric(st_bbox(geometry)["ymin"]),
    maxx = as.numeric(st_bbox(geometry)["xmax"]),
    maxy = as.numeric(st_bbox(geometry)["ymax"])
  ) %>%
  ungroup()

### Create a numeric vector from the four columns in each row
osm_heliport_bbox <- osm_heliport_bbox %>%
  mutate(bbox = pmap(list(minx, miny, maxx, maxy), ~ c(...)))
bbox_numeric <- unlist(osm_heliport_bbox$bbox[1])

## add a stop if this is more than one row
if (nrow(osm_heliport_bbox) > 1) {
  stop("More than one row in osm_heliport_bbox - please adapt script to deal with this")
}

## download any helipad within the bbox
### do NOT set postgres to TRUE, becuase we're using a very small BBOX here - and that gets overruled by utils.R for now
features_list <-list("aeroway"="helipad")
tryCatch({
  # Call the large function
  osm_helipad_within<-download_osm_process(features_list, datatypes, extra_columns, bbox=bbox_numeric, keep_region=TRUE, postgres=FALSE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

if (nrow(osm_helipad_within) > 1) {
  stop("More than one row in osm_helipad_within - please adapt script to deal with this")
}

# replace the geometry
# Assuming both datasets are in sf format and each contains only one record
# Extract the geometry from the first (and only) record in osm_helipad_within
new_geometry <- osm_helipad_within$geometry[1]

# Replace the geometry in osm_heliport
osm_heliport$geometry[1] <- new_geometry

# replace the record
osm_helipad <- osm_helipad %>%
  filter(aeroway=="helipad")
osm_helipad <- rbind(osm_helipad, osm_heliport)


# make available outside the function
osm_helipad <<- osm_helipad

# Load & transform emergency ward entrance data ----

### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list <- list("emergency"="emergency_ward_entrance")
# Define extra tags to use as columns for properties
extra_columns <- c("emergency_ward_entrance")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points")

### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_emergency_entrance<-download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})


# Select core hospital data (multipolygon only)
hospitals <- osm_all %>%
  mutate(geometry_type = st_geometry_type(geometry)) %>%
  filter(geometry_type == "MULTIPOLYGON" | geometry_type == "POLYGON") %>%
  select(hospital_osm_id=osm_id, hospital_name=name)

# Join hospital data as point in polygon
osm_emergency_entrance <- osm_emergency_entrance %>%
  st_join(hospitals, join = st_within)

# keep only the first record if there are now several records for a single entrance
osm_emergency_entrance <- osm_emergency_entrance %>%
  group_by(osm_id) %>%
  slice(1) %>%
  ungroup()

# If there are emergency ward entrances that are not in a hospital, then they should be reviewed. There's only one case within Belgium though (18/10/2024); it's case we can throw out
# stop if there is more than one case with language not NA and hospital_osm_id is NA
if (nrow(osm_emergency_entrance %>% filter(!is.na(language) & is.na(hospital_osm_id))) > 1) {
  stop("More than one case in Belgium (language not NA) and hospital_osm_id is NA - please review OSM data at this point manually, we only want emergency entrances within hospitals")
}


# select only wards within hospitals
osm_emergency_entrance <- osm_emergency_entrance %>%
  filter(!is.na(hospital_osm_id))

# make available outside the function
osm_emergency_entrance <<- osm_emergency_entrance

  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function  

### Create SQL for proper ingestion table ----

ingestion_table_hospital_sql <- c("
DROP TABLE IF EXISTS ingestion.hospitals CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.hospitals
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
  CONSTRAINT hospitals_pkey PRIMARY KEY (id)
);
",paste0("
WITH simplified as (
  SELECT
  CONCAT('https://osm.org/',osm_id) AS osm_id,
  CASE WHEN addr_street IS NULL THEN NULL 
	  ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END
	  AS address,
  CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	  ELSE CONCAT_WS('; ',contact_email, email,operator_email) END AS email,
  operator_email,
  CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL AND emergency_phone IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2, emergency_phone) END AS phone,
  emergency AS has_emergency_ward,
  CASE WHEN emergency_ward='complete' THEN 'complete' 
  WHEN emergency_ward='limited' THEN 'limited' 
  WHEN emergency='yes' then 'yes'
  WHEN emergency='no' then 'no'
  ELSE 'unknown' END AS emergency_ward_type,
  ref_fps_health_recognition AS fps_health_recognition,
  ref_fps_health_campus AS fps_health_campus,
  healthcare_speciality,
  image as image_url,
jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN 'hospital' ELSE name END,
              'fre', name_fr,
              'ger', name_de,
              'dut', name_nl)) as name,
  CASE WHEN short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND full_name IS NULL AND loc_name IS NULL THEN NULL
     ELSE CONCAT_WS('; ', short_name, official_name, alt_name, full_name, loc_name)
	 END AS other_names,
	CASE WHEN opening_hours IS NULL AND opening_hours_visitors IS NULL THEN NULL
	  ELSE CONCAT_WS('; ',opening_hours, opening_hours_visitors) END as opening_hours,
  operator,
  CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS website,
	wikidata,
	risk_level,
  geometry
  FROM raw_data.osm_hospitals)

INSERT INTO ingestion.hospitals (original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
osm_id as original_id,
name,
JSONB_BUILD_OBJECT(
  'dut', 'ziekenhuis',
  'fre', 'hôpital',
  'ger', 'krankenhaus',
  'eng', 'hospital'
) as legend_item,
'",legend_item_hospital,"'::uuid as legend_item_id,
'",data_list_id_osm,"'::uuid as data_list_id,
risk_level as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'address', address,
  'email', email,
  'phone', phone,
  'emergency_ward_type', emergency_ward_type,
  'fps_health_recognition', fps_health_recognition,
  'fps_health_campus', fps_health_campus,
  'healthcare_speciality', healthcare_speciality,
  'image', image_url,
  'opening_hours', opening_hours,
  'operator', operator,
  'website', website,
  'wikidata', wikidata,
  'other_names',other_names)) as properties,
geometry,
CURRENT_DATE as created_at
FROM simplified;"),
"ALTER TABLE IF EXISTS ingestion.hospitals OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hospitals TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hospitals TO pgn_user_airflow;")


ingestion_table_helipad_sql <- c("
DROP TABLE IF EXISTS ingestion.hospital_helipads CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.hospital_helipads
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
  CONSTRAINT hospital_helipads_pkey PRIMARY KEY (id)
);
",paste0("
WITH simplified as (
  SELECT
  CONCAT('https://osm.org/',osm_id) AS original_id,
  jsonb_strip_nulls(jsonb_build_object(
    'und', CASE WHEN name IS NULL THEN 'hospital helipad' ELSE concat(name,' (' || icao || ')') END,
    'fre', CASE WHEN name_fr IS NULL THEN NULL else concat(name_fr,' (' || icao || ')') END,
    'ger', CASE WHEN name_de IS NULL THEN NULL else concat(name_de,' (' || icao || ')') END,
    'dut', CASE WHEN name_nl IS NULL THEN NULL else concat(name_nl,' (' || icao || ')') END)) as name,
  CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
  ELSE CONCAT_WS('; ',contact_email, email,operator_email) END AS email,
  operator_email,
  CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
  ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS phone,
  opening_hours,
  operator,
  CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
  ELSE CONCAT_WS('; ',website, contact_website) END AS website,
  operator_wikidata,
  wikidata,
  icao,
  maxweight,
  reservation,
  surface,
  description,
  aeroway,diameter,network,
  0 as risk_level,
  geometry
  FROM raw_data.osm_helipad)

INSERT INTO ingestion.hospital_helipads (original_id, name, legend_item, legend_item_id, risk_level, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
JSONB_BUILD_OBJECT(
  'dut', 'ziekenhuis helikopterplatform',
  'fre', 'héliport hospitalier',
  'ger', 'krankenhaus hubschrauberlandeplatz',
  'eng', 'hospital helipad'
) as legend_item,
'",legend_item_hospital_helipad,"'::uuid as legend_item_id,
risk_level,
'",data_list_id_helipad,"'::uuid as data_list_id,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'email', email,
  'operator email', operator_email,
  'phone', phone,
  'opening_hours', opening_hours,
  'operator', operator,
  'website', website,
  'wikidata', wikidata,
  'icao', icao,
  'maxweight', maxweight,
  'reservation', reservation,
  'surface', surface,
  'description', description,
  'aeroway',aeroway,
  'diameter',diameter
  )) as properties,
geometry,
CURRENT_DATE as created_at
FROM simplified;"),
"ALTER TABLE IF EXISTS ingestion.hospital_helipads OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hospital_helipads TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hospital_helipads TO pgn_user_airflow;")


ingestion_table_emergency_sql <- c("
DROP TABLE IF EXISTS ingestion.hospital_emergency CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.hospital_emergency
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
  CONSTRAINT hospital_emergency_pkey PRIMARY KEY (id)
);
",paste0("
WITH simplified as (
  SELECT
  CONCAT('https://osm.org/',osm_id) AS original_id,
  jsonb_strip_nulls(jsonb_build_object(
    'und', CASE WHEN language IS NULL THEN 'emergency'
            WHEN language='fre' THEN 'urgences'
            WHEN language='ger' THEN 'Notaufnahme'
            WHEN language='dut' THEN 'spoed'
            WHEN language='brussels' THEN 'emergency'
            ELSE 'emergency' END,
    'fre', 'urgences',
    'ger', 'Notaufnahme',
    'dut', 'spoed')) as name,
  CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
  ELSE CONCAT_WS('; ',contact_email, email,operator_email) END AS email,
  operator_email,
  CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
  ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS phone,
  opening_hours,
  hospital_name as operator,
  CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
  ELSE CONCAT_WS('; ',website, contact_website) END AS website,
  operator_wikidata,
  wikidata,
  0 as risk_level,
  geometry
  FROM raw_data.osm_emergency_entrance)

INSERT INTO ingestion.hospital_emergency (original_id, name, legend_item, legend_item_id, risk_level, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
JSONB_BUILD_OBJECT(
  'dut', 'spoedingang',
  'fre', 'entrée des urgences',
  'ger', 'Notaufnahme',
  'eng', 'emergencies entrance'
) as legend_item,
'",legend_item_hospital_emergency,"'::uuid as legend_item_id,
risk_level,
'",data_list_id_emergency_entrance,"'::uuid as data_list_id,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'email', email,
  'operator email', operator_email,
  'phone', phone,
  'opening_hours', opening_hours,
  'operator', operator,
  'website', website,
  'wikidata', wikidata
  )) as properties,
geometry,
CURRENT_DATE as created_at
FROM simplified;"),
"ALTER TABLE IF EXISTS ingestion.hospital_emergency OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hospital_emergency TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hospital_emergency TO pgn_user_airflow;")
 


# LOAD ----
# """""""""""""""""" ----


### Execute the SQL commands ----

create_ingestion_table_hospitals <- function() {execute_sql_commands(ingestion_table_hospital_sql, "Hospital Ingestion table")}
create_ingestion_table_helipads <- function() {execute_sql_commands(ingestion_table_helipad_sql, "Helipad Ingestion table")}
create_ingestion_table_emergencies <- function() {execute_sql_commands(ingestion_table_emergency_sql, "Emergency Ingestion table")}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----

run_smart_update = function() {
  smart_update_process("hospitals", 50, 200, 100, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
  smart_update_process("hospital_helipads", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
  smart_update_process("hospital_emergency", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_hospitals")
    CreateImportTable(dataset = osm_helipad, schema = "raw_data", table_name = "osm_helipad")
    CreateImportTable(dataset = osm_emergency_entrance, schema = "raw_data", table_name = "osm_emergency_entrance")
    create_ingestion_table_hospitals()
    create_ingestion_table_helipads()
    create_ingestion_table_emergencies()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}



