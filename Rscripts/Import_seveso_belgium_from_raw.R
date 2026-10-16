## ---------------------------
##
## Script name: Compile sources about BE Seveso and update Paragon
##
## Purpose of script: Merge data about seveso sites from regions and federal sources and transform into proto-anchors for Paragon + update seveso.be
##
## Author: Joost Schouppe
##
## Date Created: 2025-10-14 (total rewrite of the 2023-12-14 version)
##
##
## ---------------------------


# NOTE: the resulting data is published as a geojson that is used for the maps on seveso.be . This map depends on name language 'und' existing, as well as the properties 'address', 'activity_fre','activity_dut' and 'federal_id' having some text. 'risk_ecotoxic', 'risk_explosion', 'risk_fire' and 'risk_toxic' should be 0 or 1. original_id should be filled with the regional ID. The risk_level is expected to be 3 or 4.
## Any change in the process that affects anything of the above should be discussed with COMM and lead to adjustments in the setting of the 4 umaps that they use.

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

# Set dynamic parameters ------

# Update if new files received. These should be added to github with a PR. Add them in the data/seveso/be folder and update the name here if needed
flanders_link_filename <- "20250904_Seveso_exploitant-vergunning.xlsx"
brussels_polygon_filename <- "polygons-brussels-lambert72.geojson"

# normal parameters
legend_item_id <- "70088593-ec60-42c0-952d-81968e3b273f"
data_list_id <- "ccd8ec1f-d624-47dd-b24b-1df1e371c4b2"


# Set location for files received by email
offline_storage <-Sys.getenv("OFFLINE_STORAGE")

# Location in that folder
flanders_link_path <- paste0(offline_storage,"/seveso/be/",flanders_link_filename)
brussels_polygons_path <- paste0(offline_storage,"/seveso/be/",brussels_polygon_filename)
# if new files received, put them in pgn-data-airflow\data\seveso\be\ and use the upload script to get them in Airflow

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



# Library -----------------------------------------------------------------
# """""""""""""""""" ----------------------
library(openxlsx)
# other required libraries loaded via the utils scripts





# EXTRACT ----
# """""""""""""""""" ----------------------

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {

# Get the month we are working on    
get_current_month <- function() {
con_pg <- get_con()
batch_state <- dbGetQuery(con_pg, "
  SELECT 
    month_id,
    COUNT(*) FILTER (WHERE processing_status = 'ok') AS ok_count,
    MAX(batch_status) AS batch_status
  FROM raw_data.seveso_email_intake_archive
  GROUP BY month_id
  ORDER BY month_id DESC
  LIMIT 1
")
dbDisconnect(con_pg)

current_month_id<-NA
current_month_id <- if (nrow(batch_state) == 0) {
  stop("Getting the import status failed")
} else if (batch_state$batch_status[1] != "import_triggered") {
  stop(paste0("The import status of batch ", batch_state$month_id[1], " is '",batch_state$batch_status[1], "', not the expected 'import_triggered" ))
} else {  
  batch_state$month_id[1]
}

return(current_month_id)
}
current_month_id <- get_current_month()
message("Working on month_id: ", current_month_id)
    
# Load main datasets ----
### Prepare loading functions ----

## Get link/inspection

## Get Flanders linkfile
LoadVLpolylink <- function(){  
  tryCatch({
    VL_polylink <-readxl::read_xlsx(flanders_link_path)
    names(VL_polylink) <- tolower(names(VL_polylink))
    colnames(VL_polylink) <- gsub(" ", "", colnames(VL_polylink))
    print("Loading VL polylinks is done")
    return(VL_polylink)
  }, 
  error = function(e) {
    print(paste("Error loading VL_polylink:", e))
    return(NULL)
  })
}

### Run data loading functions ------
con_pg <- get_con()
# main data files
fedlink <- dbGetQuery(con_pg, "SELECT * FROM raw_data.seveso_fod")
VL <- dbGetQuery(con_pg, "SELECT * FROM raw_data.seveso_flanders")
WAL <- dbGetQuery(con_pg, "SELECT * FROM raw_data.seveso_wallonia")
BRU <- dbGetQuery(con_pg, "SELECT * FROM raw_data.seveso_brussels")
# get our Own Links (the records where the federal id is not yet known)
OL <- dbGetQuery(con_pg, "SELECT fed_id, reference FROM raw_data.seveso_temporary_links")
dbDisconnect(con_pg)

# special data delivery to help with link between Flemish & Federal level
VL_polylink <- LoadVLpolylink()

# self-made dataset with the Brussels polygons
brussels_pg<-st_read(brussels_polygons_path)

# TRANSFORM ----
# """""""""""""""""" ----------------------

# Merge regional datasets ----

# set all column names lowercase and remove spaces
VL <- VL %>% 
  rename_with(~ tolower(gsub(" ", "_", .x)))
WAL <- WAL %>% 
  rename_with(~ tolower(gsub(" ", "_", .x)))
BRU <- BRU %>% 
  rename_with(~ tolower(gsub(" ", "_", .x)))

# harmonize column names
VL <- VL %>% rename(activity_dut=description)
BRU <- BRU %>% rename(activity_dut=description)
WAL <- WAL %>% rename(activity_fre=description)



#  harmonize column types

# QA: check for strange values in the type column. Should always be 1 (low risk) or 2 (high risk). If it's a value like "in review", it should be removed (which will happen in a next step). If it's value like "high risk" or "low risk", it should be changed to 1 or 2. Consult with source to avoid the problem re-occurring.
table(BRU$sevesostatus)
table(WAL$sevesostatus)
table(VL$sevesostatus)

BRU$sevesostatus <- as.numeric(BRU$sevesostatus)
VL$sevesostatus <- as.numeric(VL$sevesostatus)
WAL$sevesostatus <- as.numeric(WAL$sevesostatus)
WAL$zipcode  <- as.numeric(WAL$zipcode)
VL$lambertx <- as.numeric(VL$lambertx)
VL$lamberty <- as.numeric(VL$lamberty)
BRU$housenr <- as.character(BRU$housenr)
WAL$riskecotoxic <- as.logical(WAL$riskecotoxic)
WAL$risktoxic <- as.logical(WAL$risktoxic)
WAL$riskfire <- as.logical(WAL$riskfire)
WAL$riskexplosion <- as.logical(WAL$riskexplosion)
VL$riskecotoxic <- as.logical(VL$riskecotoxic)
VL$risktoxic <- as.logical(VL$risktoxic)
VL$riskfire <- as.logical(VL$riskfire)
VL$riskexplosion <- as.logical(VL$riskexplosion)
VL$productecotoxic <- as.logical(VL$productecotoxic)
VL$producttoxic <- as.logical(VL$producttoxic)
VL$productfire <- as.logical(VL$productfire)
VL$productexplosion <- as.logical(VL$productexplosion)
VL$productoxidising <- as.logical(VL$productoxidising)

# merge the regional datasets
new_merge <- bind_rows(VL,WAL,BRU) %>%
  filter(sevesostatus==1 | sevesostatus==2) %>%
  mutate(sevesostatus = as.integer(sevesostatus))



# Test for missing or empty names ----
missing_name <- new_merge %>%
  filter(is.na(name) | name == "")
if (nrow(missing_name) > 0) {
  print(nrow(missing_name)," had no name, trying to fix with nickname", "\n")
  new_merge <- new_merge %>%
    mutate(name=ifelse(is.na(name) | name == "",nickname,name))
  missing_name <- new_merge %>%
    filter(is.na(name) | name == "")

    if (nrow(missing_name) > 0) {
      stop(nrow(missing_name)," records have no name, even after trying to fix with nickname", "\n")
    }
}
## Note: Flanders regularly adds cases that have not been fully reviewed yet without the seveso status and/or without the main name field. If this happens, simply throw out the cases without a normal seveso status, and manually fill in the name based on the nickname.


# Add federal data ----

fedlink <- fedlink %>%
  select(reference, fed_id, date_inspection=inspectiondate)

# Add the known missing links (fake federal_id previously generated temporary numbers (9XX))
fedlink <- bind_rows(fedlink, OL)

# Remove the missing links that by now have a real federal ID
fedlink <- fedlink %>%
  group_by(reference) %>%
  filter(fed_id == min(fed_id)) %>%
  ungroup()


# add the federal data to the new merge
new_merge <- new_merge %>%
  left_join(fedlink, by = "reference")





# QA: Check duplicates (N should always be 0)
dup_check_id <- new_merge %>%
  group_by(fed_id) %>%
  summarise(frequency = n()) %>%
  filter(frequency > 1 & !is.na(fed_id))
dup_check_reference <- new_merge %>%
  group_by(reference) %>%
  summarise(frequency = n()) %>%
  filter(frequency > 1 & !is.na(reference))

# cat the number of duplicates if there are any records in the datasets
# the process will stop automatically if there are any
if (nrow(dup_check_id) > 0) {
  print(dup_check_id)
  stop("Duplicates in federal id column: ", nrow(dup_check_id), "\n")
}
if (nrow(dup_check_reference) > 0) {
  print(dup_check_reference)
  stop("Duplicates in reference (regional_id) column: ", nrow(dup_check_reference), "\n")
}




### Give a temporary fed_id to new objects not in fedlink yet ----
# if there are missing fed_id's, give them a number lower than the lowest in OL and update OL.csv afterwards
new_records_OL <- NA

if (any(is.na(new_merge$fed_id))) {
  # Step 1: Find the lowest number in the 'fed_id' column in dataframe OL which is above 900
  lowest_fed_id <- min(OL$fed_id[OL$fed_id >= 900], na.rm = TRUE)
  
  # Step 2: Identify rows in seveso0 where fed_id is NULL
  new_merge_with_null_id <- new_merge %>%
    filter(is.na(fed_id))
  
  # Step 3: Fill in consecutive lower numbers in the 'id' column
  new_ids <- seq(lowest_fed_id-1, by = -1, length.out = nrow(new_merge_with_null_id))
  
  new_merge_with_null_id <- new_merge_with_null_id %>%
    mutate(fed_id = new_ids)
  
  # Step 4: Create new records in OL for filled id values
  new_records_OL <- new_merge_with_null_id %>%
    select(fed_id, reference) %>%
    distinct() %>%
    bind_rows(OL, .)
  
  new_records_OL <- new_records_OL %>%
    select(fed_id, reference)
  
  # visual inspection of the new records, to avoid a previous issue where the file was messed up
  print("Showing the 'own links' content that will be uploaded to raw_data.seveso_temporary_links later")
  print(new_records_OL)
  
  
  # Step 5: update new_merge with the new ID numbers
  
  new_merge <- bind_rows(new_merge,new_merge_with_null_id)
  # Filter out rows where 'id' is NULL
  new_merge <- new_merge %>%
    filter(!is.na(fed_id))
  
} else {
  cat("No NULL values found in 'fed_id' column.")
}



# standardize columns
new_merge <- new_merge %>%
  rename(risk_toxic=risktoxic, risk_ecotoxic=riskecotoxic, risk_fire=riskfire, risk_explosion=riskexplosion) %>%
  mutate(
    risk_toxic = ifelse(is.na(risk_toxic), FALSE, risk_toxic),
    risk_ecotoxic = ifelse(is.na(risk_ecotoxic), FALSE, risk_ecotoxic),
    risk_fire = ifelse(is.na(risk_fire), FALSE, risk_fire),
    risk_explosion = ifelse(is.na(risk_explosion), FALSE, risk_explosion)
  ) %>%
  rename(
    product_ecotoxic=productecotoxic,
    product_toxic=producttoxic,
    product_fire=productfire,
    product_explosion=productexplosion,
    product_oxidising=productoxidising
  ) %>%
  mutate(
    product_ecotoxic = ifelse(is.na(product_ecotoxic), FALSE, product_ecotoxic),
    product_toxic = ifelse(is.na(product_toxic), FALSE, product_toxic),
    product_fire = ifelse(is.na(product_fire), FALSE, product_fire),
    product_explosion = ifelse(is.na(product_explosion), FALSE, product_explosion),
    product_oxidising = ifelse(is.na(product_oxidising), FALSE, product_oxidising)
  )



# Get the x-y from the existing data in Paragon ----
new_merge <- new_merge %>%
  rename(regional_id=reference)

con_pg<-get_con()
seveso0<- dbGetQuery(con_pg,
                     "SELECT 
  TRIM(regexp_split_to_table(original_id, ';')) AS regional_id,
  ROUND(ST_X(ST_Transform(geometry_pt, 31370))) AS x,
  ROUND(ST_Y(ST_Transform(geometry_pt, 31370))) AS y
  FROM transformation.seveso 
  WHERE deleted_at IS NULL
  ORDER BY regional_id")
dbDisconnect(con_pg)

# overwrite non-missing cases with the values from curation
new_merge <- new_merge %>%
  left_join(seveso0, by = "regional_id") %>%
  mutate(
    lambertx=ifelse(is.na(x),lambertx,x),
    lamberty=ifelse(is.na(y),lamberty,y)
  ) %>%
  select(-x,-y)

# Check for missing coördinates ----
# please note: perfect geocoding is needed, if there are missing coordinates, add them manually! 
geocode_input <- new_merge %>%
  filter(is.na(lambertx)) %>%
  select(regional_id,address,housenr,zipcode)

# stop if there are cases
if(nrow(geocode_input) > 0){
  print(geocode_input)
  stop(nrow(geocode_input)," records have no Lambert coordinates. Please find them (e.g. by looking up the address on Geopunt) and add them to the excel where the data is sourced from / straight in the database in raw data.", "\n", "When doing so, make sure the selected point is inside the regional polygon (if already available)")
}


# create geometry based on the x and y columns
new_merge <- st_as_sf(new_merge, coords = c("lambertx", "lamberty"), crs = st_crs(31370))


# if the name is not unique, use the nickname instead. Keep the original name in the nickname field.
new_merge <- new_merge %>%
  group_by(name) %>%
  mutate(
    is_duplicate = n() > 1,
    backup_name = name
  ) %>%
  ungroup() %>%
  mutate(
    name = if_else(is_duplicate, nickname, name)
  ) %>%
  mutate(  
    nickname = if_else(is_duplicate, backup_name, nickname)
  ) %>%
  select(-backup_name, -is_duplicate)


# set as date variable
new_merge <- new_merge %>%
  mutate(date_inspection = as.Date(date_inspection, format = "%Y-%m-%d"))



## Enrich with polygon data ----

### Add Flemish polygon data ----

# if source breaks, refer to https://www.vlaanderen.be/datavindplaats/catalogus/terreinen-van-seveso-inrichtingen-in-vlaanderen
gpkg_vla <- "https://dataplatform.omgeving.vlaanderen.be/public/mercatornet/pf_seveso_ter/pf_seveso_ter.lb72?format=gpkg"

# Define the local file path to save the downloaded GPKG
local_file <- file.path(offline_storage,"pf_seveso_ter.gpkg")

# Download the GPKG file
GET(gpkg_vla, write_disk(local_file, overwrite = TRUE))

# Read the GPKG file using sf
gpkg_vla <- st_read(local_file, layer = "pf_seveso_ter")
unlink(local_file)




# in Flanders, one "exploitant" (federal logic) can have more than one "vergunningshouder" (Flemish logic). To deal with this, we merge polygons that have the same "exploitant".
# in Flanders, one "vergunningshouder" can have more than one exploitant. We "ignore" this. That means we only find one polygon. The other exploitants on the same area will simply retain a point on that polygon. There's only a single case: Campine. Maybe it makes sense to follow the Flemish logic in Paragon, which means we could also merge the organizations 
VL_polylink <- VL_polylink %>%
  select(refpermit="vl-nummervergunninghouder", refexpl="vl-nummerexploitant")
gpkg_vla <- gpkg_vla %>%
  left_join(VL_polylink, by=c("referentie"="refpermit")) %>%
  mutate(referentie = ifelse(!is.na(refexpl), refexpl, referentie)) %>%
  select(referentie, geom) %>%
  group_by(referentie) %>%
  summarise(geom = st_union(geom)) %>%
  ungroup()


gpkg_vla_df<- as.data.frame(gpkg_vla) %>%
  select(referentie, geom) %>%
  rename(geometry_pg = geom)





### Add Brussels polygon data ----

# rename geometry to geometry_pg
brussels_pg<-brussels_pg %>% rename(geometry_pg = geometry)

polygons <-bind_rows(gpkg_vla_df, brussels_pg)


### Add Wallonia polygon data ----
# download 
wallonia_pg <- st_read("https://geoservices.wallonie.be/arcgis/rest/services/INDUSTRIES_SERVICES/SEVESO/MapServer/1/query?where=1%3D1&outFields=*&returnGeometry=true&f=geojson")
wallonia_pg <- wallonia_pg %>% rename(geometry_pg = geometry)
wallonia_pg <- wallonia_pg %>% st_transform(31370)
wallonia_pg <- wallonia_pg %>%
  mutate(referentie = sprintf("WA%04d", REF_SEVESO)) %>%
  select(referentie, geometry_pg)

polygons <-bind_rows(polygons, wallonia_pg)


### Join polygon data to points ----
new_merge_pg <- left_join(new_merge, polygons, by = c("regional_id" = "referentie"))


new_merge_pg$distance <- mapply(calculate_distance_integrated, new_merge_pg$geometry, new_merge_pg$geometry_pg)


# if distance>0, make geometry_pg empty. You may review objects where this is the case. Sometimes a small move of the point can fix this.
new_merge_pg$geometry_pg[!is.na(new_merge_pg$distance) & new_merge_pg$distance > 0] <- NA
new_merge_pg<-new_merge_pg %>% 
  select(-distance) %>%
  rename(geometry_pt = geometry)



# select just the records of new_merge_pg that have an invalid geometry_pg
invalid_geometries <- new_merge_pg %>% filter(st_is_valid(geometry_pg) == FALSE)
print(paste0("Invalid geometries: ", nrow(invalid_geometries)))
if (nrow(invalid_geometries)>0) {
  new_merge_pg$geometry_pg <- st_make_valid(new_merge_pg$geometry_pg)
} 
invalid_geometries <- new_merge_pg %>% filter(st_is_valid(geometry_pg) == FALSE)
print(paste0("Invalid geometries after fix: ", nrow(invalid_geometries)))
if (nrow(invalid_geometries)>0) {
  stop("ERROR: There are still invalid geometries after fix")
}


# Post processing steps ----

## make a backup of our Own Links ----
OL_backup <- OL %>%
  mutate(current_month_id=current_month_id,
         run_timestamp=Sys.time())

con_pg <- get_con()
timestamp_backup <- dbGetQuery(con_pg, "
  SELECT MAX(run_timestamp) as max_run_timestamp, max(current_month_id) as max_current_month_id FROM raw_data.seveso_temporary_links_backup")
dbDisconnect(con_pg)

if (timestamp_backup$max_run_timestamp[1] == OL_backup$run_timestamp[1]) {
  stop("We already made this exact backup of our Own Links") 
} else if (timestamp_backup$max_current_month_id[1] == OL_backup$current_month_id[1]) {
  stop("We already made a backup of our Own Links") 
} else {
  AppendImportTable(OL_backup,"raw_data","seveso_temporary_links_backup")
}

## update the current version of our own links ----
if (is.data.frame(new_records_OL)) {
  CreateImportTable(new_records_OL,"raw_data","seveso_temporary_links")
}

## update processed raw data ----
### minor cleanup
new_merge_pg <- new_merge_pg %>%
  select(-ogc_fid) %>%
  mutate(sevesostatus = as.integer(sevesostatus))
CreateImportTable(dataset = new_merge_pg, schema = "raw_data", table_name = "seveso_processed")


# update seveso_email_intake_archive ----
# mark complete
con_pg <- get_con()
dbExecute(con_pg, sprintf("
    UPDATE raw_data.seveso_email_intake_archive
    SET batch_status = 'raw_data_processed'
    WHERE month_id = %d", current_month_id))
dbDisconnect(con_pg)




} else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function  


### Import to raw data ----



### Create SQL for proper ingestion table ----

# note: during this step we merge identical geometries together (to fit with general Paragon anchor logic). Strings are combined, for risks, the highest risk is kept.
ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.seveso CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.seveso
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
  	geometry_pt geometry(geometry, 4326),
  	geometry_pg geometry(geometry, 4326),
    CONSTRAINT seveso_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
regional_id AS original_id,
name, CONCAT(nickname,' (',fed_id,')') as other_names,
LTRIM(CONCAT(address,' ', housenr, ', ', zipcode,' ',city)) AS address,
fed_id as federal_id, sevesostatus, 
risk_toxic, risk_fire, risk_explosion, risk_ecotoxic, 
product_ecotoxic,product_toxic,product_fire,product_explosion,product_oxidising,
NULLIF(activity_dut,'') as activity_dut, NULLIF(activity_fre,'') as activity_fre, date_inspection,
CASE WHEN ST_IsEmpty(geometry_pg) THEN ST_Transform(geometry_pt,4326)
	    ELSE ST_Transform(geometry_pg,4326) END as geometry,
ST_Transform(geometry_pt,4326) as geometry_pt,
ST_Transform(geometry_pg,4326) as geometry_pg
FROM raw_data.seveso_processed),
				 
aggregated as (SELECT string_agg(original_id, ';' ORDER BY original_id) as original_id,
string_agg(name,'; ' ORDER BY name) as name,
string_agg(other_names,'; ' ORDER BY other_names) as other_names,
jsonb_build_object(
	'dut', 'Sevesobedrijf',
	'fre', 'entreprise Seveso',
	'ger', 'Seveso-betriebs',
	'eng', 'Seveso company') as legend_item,
	min(address) as address,
	string_agg(federal_id::text, ';' ORDER BY federal_id) as federal_id,
	max(sevesostatus) as sevesostatus,
	bool_or(risk_toxic) as risk_toxic,
	bool_or(risk_fire) as risk_fire,
	bool_or(risk_explosion) as risk_explosion,
	bool_or(risk_ecotoxic) as risk_ecotoxic,
	bool_or(product_ecotoxic) as product_ecotoxic,
  bool_or(product_toxic) as product_toxic,
  bool_or(product_fire) as product_fire,
  bool_or(product_explosion) as product_explosion,
  bool_or(product_oxidising) as product_oxidising,
	string_agg(DISTINCT activity_dut, ';') as activity_dut,
	string_agg(DISTINCT activity_fre, ';') as activity_fre,
	max(date_inspection) as date_inspection,
	geometry,
	min(geometry_pt)::geometry as geometry_pt,
	min(geometry_pg)::geometry as geometry_pg
FROM cleaned
GROUP by geometry)

INSERT INTO ingestion.seveso 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, geometry_pt, geometry_pg, created_at)
SELECT
original_id,
jsonb_build_object('und', CASE WHEN name IS NULL THEN 'seveso company' ELSE name END) as name,
legend_item,
'",legend_item_id,"'::uuid as legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
CASE WHEN sevesostatus=2 THEN 4
  WHEN sevesostatus=1 THEN 3 ELSE null END as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'federal_id',federal_id,
	'address', address,
	'risk_toxic',risk_toxic,
	'risk_fire',risk_fire,
	'risk_explosion',risk_explosion,
	'risk_ecotoxic',risk_ecotoxic,
	'product_ecotoxic', product_ecotoxic,
  'product_toxic', product_toxic,
  'product_fire', product_fire,
  'product_explosion', product_explosion,
  'product_oxidising', product_oxidising,
	'activity_dut',activity_dut,
	'activity_fre',activity_fre,
	'date_inspection',date_inspection,
	'other_names',other_names,
	'seveso_risk', CASE WHEN sevesostatus=2 THEN 'upper limit'
  WHEN sevesostatus=1 THEN 'lower limit' ELSE null END)),
ST_ReducePrecision(geometry, 1e-8) as geometry,
ST_ReducePrecision(geometry_pt, 1e-8) as geometry_pt,
ST_ReducePrecision(geometry_pg, 1e-8) as geometry_pg,
CURRENT_DATE as created_at
FROM aggregated;"),
"ALTER TABLE IF EXISTS ingestion.seveso OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.seveso TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.seveso TO pgn_user_airflow;"
)


### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}




# set to TRUE if you want to update the transformation table even if the checks fail. 
#overrule_checks <- TRUE
#do_dry_run <- TRUE
run_smart_update = function() {
  smart_update_process("seveso", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
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


