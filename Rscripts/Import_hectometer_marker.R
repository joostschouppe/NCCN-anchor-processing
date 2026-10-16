## ---------------------------
##
## Script name: ETL flow for hectometer markers
##
## Purpose of script: Merge data for hectometer markers, enrich with nearest regional road info and transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2025-11-18
##
##
## ---------------------------

# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

# connection details
db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

#datalist ids
dl_hectometer_bru <- "7c9f27af-5193-4001-9ed8-130e766ab930"
dl_hectometer_wal <- "5bdd0648-43e3-4979-ac37-9ce96bc7a6f7"
dl_hectometer_vla <- "ef67d78c-4e09-4408-8467-48d3bf4183f2"

#legend item ids
li_milemarker_km <- "cf180191-9e0d-42e4-8c6d-c9366632ff68"
li_milemarker_hm <- "f88b5f24-661d-468f-851c-ea984e841cb2"

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


#data_list_id<-"cannot be added centrally, since several items are processed"

temporary_folder <-Sys.getenv("TEMPORARY_STORAGE")
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

# switch to get extra QA info
run_extra_qa<-FALSE

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))



# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(tidyverse) # a suite of packages for data wrangling, transformation, plotting, ...
library(ows4R) # interface for OGC webservices
library(arcgislayers) # for the Wallonia ArcGIS REST service

# Download data -----------------------------------------------------------
# """"""""""""""""""----------------------

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {

### Vlaanderen ----

# WFS download as explained on https://inbo.github.io/tutorials/tutorials/spatial_wfs_services/

# Define the base WFS URL
wfs <- "https://opendata.apps.mow.vlaanderen.be/opendata-geoserver/awv/wfs"


# Initialize variables
all_features <- list()
start_index <- 0
batch_size <- 20000
has_more_features <- TRUE

# Build the request URL
build_request_url <- function(start_index) {
  url <- parse_url(wfs)
  url$query <- list(
    service = "wfs",
    request = "GetFeature",
    typename = "awv:Referentiepunten",
    srsName = "EPSG:31370",
    startIndex = start_index,
    maxFeatures = batch_size,
    outputFormat = "application/json"
  )
  return(build_url(url))
}



# Loop to fetch data in batches
while (has_more_features) {
  # Build the request URL for the current batch
  request_url <- build_request_url(start_index)
  
  # Fetch the data
  batch <- read_sf(request_url)
  
  # Check if there are no more features to fetch
  if (nrow(batch) == 0) {
    has_more_features <- FALSE
  } else {
    # Append the fetched features to the list
    all_features <- append(all_features, list(batch))
    # Increment the start index for the next batch
    start_index <- start_index + batch_size
  }
}

# Combine all fetched features into a single data frame
vl <- do.call(rbind, all_features)

# remove useless 3D z-dimension
vl <- st_zm(vl, drop = TRUE)

# keep raw copy for raw_data schema
vl_raw <- vl

# set all columns to lowercase
vl <- vl %>% 
  rename_all(tolower)

# Print the number of features retrieved
cat("Total VL signs retrieved:", nrow(vl), "\n")


# prettier road number
vl <- vl %>%
  mutate(
    substr_part = substr(wegnummer, 2, 4),
    tmp = as.integer(substr_part),
    road = paste0(substr(wegnummer, 1, 1), if_else(is.na(tmp), substr_part, as.character(tmp)))
  ) %>%
  select(-substr_part, -tmp)

# extract segment identifier & clean up
vl <- vl %>%
  mutate(
    substr_part = substr(wegnummer, 5, nchar(wegnummer)),
    tmp = as.integer(substr_part),
    segment = if_else(is.na(tmp), substr_part, as.character(tmp))
  ) %>%
  select(-substr_part, -tmp) %>%
  rename(marker=opschrift) %>%
  select(id,road,segment,marker)

# a lot of objects are returned several times, here we simplify
vl_agg <- vl %>%
 group_by(id) %>%
 summarize(
   across(
     c(road, segment, marker),
     collapse_unique
   ),
   geometry = first(geometry) # Retain the geometry
   ) %>%
 ungroup()

# if needed, have a look at place where more than one sign shares the same location
if (run_extra_qa) {
geom_duplicates <- vl_agg %>%
  mutate(geom_wkt = st_as_text(geometry)) %>%
  group_by(geom_wkt) %>%
  filter(n() > 1) %>%
  ungroup() %>%
  arrange(geom_wkt) %>%
  mutate(casenum=row_number()) %>%
  select(-geom_wkt)
cat("Total VL markers where several IDs share a single location:", nrow(geom_duplicates), "\n")
}


vl_agg <- vl_agg %>%
  mutate(geom_wkt = st_as_text(geometry)) %>%
  group_by(geom_wkt) %>%
  summarize(
    across(
      c(id, road, segment, marker),
      collapse_unique
    ),
    geometry = first(geometry) # Retain the geometry
  ) %>%
  ungroup() %>%
  select(-geom_wkt)

vl_agg <- vl_agg %>%
  mutate(data_list_id=dl_hectometer_vla)



cat("Total VL markers retrieved:", nrow(vl), "\n")
cat("Total VL markers after deduplicating:", nrow(vl_agg), "\n")



### Brussels ----



# Define the base WFS URL
wfs <- "https://data.mobility.brussels/geoserver/bm_equipment/wfs"

# Build the request URL
build_request_url <- function(start_index) {
  url <- parse_url(wfs)
  url$query <- list(
    service = "wfs",
    request = "GetFeature",
    typename = "bm_equipment:esign_panels",
    srsName = "EPSG:31370",
    startIndex = start_index,
    count = 1000,
    outputFormat = "json"
  )
  return(build_url(url))
}

# Initialize variables
all_features <- list()
start_index <- 0
batch_size <- 1000
has_more_features <- TRUE

# Loop to fetch data in batches
while (has_more_features) {
  # Build the request URL for the current batch
  request_url <- build_request_url(start_index)
  if (run_extra_qa) {print(request_url)}
  # Fetch the data
  batch <- read_sf(request_url)
  
  # Check if there are no more features to fetch
  if (nrow(batch) == 0) {
    has_more_features <- FALSE
  } else {
    # Append the fetched features to the list
    all_features <- append(all_features, list(batch))
    # Increment the start index for the next batch
    start_index <- start_index + batch_size
  }
}

# Combine all fetched features into a single data frame
bru <- do.call(rbind, all_features)

bru_raw <- bru


#keep only hectometer
bru <- bru %>%
  filter(type_code=='KM')

bru$bru_marker<- gsub(" ", "", bru$gen_text_fr)
bru$bru_marker <- gsub("\\|\\|", "|", bru$bru_marker)

# make the marker prettier
bru <- bru %>%
  mutate(bru_marker = sub("^([0-9]{1,2})\\.[|]([0-9])[|]$",
                                "\\1.\\2",
                                bru_marker))

# extract the marker
bru$marker <- sub("\\|.*", "", bru$bru_marker)
# remove trailing .0
bru$marker <- sub("\\.0$", "", bru$marker)
# remove trailing .
bru$marker <- sub("\\.$", "", bru$marker)
# set empty to missing
bru$marker[bru$marker == ""] <- NA

# extract road info
bru <- bru %>%
  mutate(
    road = if_else(grepl("\\|", bru_marker),
                   sub("^[^|]*\\|", "", bru_marker),
                   NA_character_)
  )
# replace | with spaces
bru$road <- gsub("\\|", " ", bru$road)

# remove stray spaces
bru$road <- trimws(bru$road)

# set to NA if empty string
bru$road[bru$road == ""] <- NA

bru <- bru %>%
  filter(!(is.na(road) & is.na(marker)))


# remove geometric duplicates
bru_agg <- bru %>%
  mutate(geom_wkt = st_as_text(geometry)) %>%
  group_by(geom_wkt) %>%
  summarize(
    across(
      c(id, road, marker),
      collapse_unique
    ),
    geometry = first(geometry) # Retain the geometry
  ) %>%
  ungroup() %>%
  select(-geom_wkt)

bru_agg <- bru_agg %>%
  mutate(data_list_id=dl_hectometer_bru)


cat("BRU relevant signs:", nrow(bru), "\n")
cat("Total BRU stops after processing:", nrow(bru_agg), "\n")


### Wallonia ----



hm_url <- arc_open("https://geoservices.wallonie.be/arcgis/rest/services/MOBILITE/RES_ROUTIER_REGIONAL/MapServer/1/")
km_url <- arc_open("https://geoservices.wallonie.be/arcgis/rest/services/MOBILITE/RES_ROUTIER_REGIONAL/MapServer/2/")
wal_hm <- arc_select(hm_url)
wal_km <- arc_select(km_url)


wal <- rbind(wal_hm, wal_km)
wal_raw <- wal

# set all columns to lowercase
wal <- wal %>%
  rename_all(tolower)


# remove full duplicates
wal <- dplyr::distinct(wal)

# if needed, have a look at place where more than one sign shares the same location
if (run_extra_qa) {
  geom_duplicates <- wal %>%
    mutate(geom_wkt = st_as_text(geometry)) %>%
    group_by(geom_wkt) %>%
    filter(n() > 1) %>%
    ungroup() %>%
    arrange(geom_wkt) %>%
    mutate(casenum=row_number()) %>%
    select(-geom_wkt)
  cat("Total WAL markers where several IDs share a single location:", nrow(geom_duplicates), "\n")
}

# prettier road number
wal <- wal %>%
  mutate(
    substr_part = substr(code_route, 2, 4),
    tmp = as.integer(substr_part),
    road = paste0(substr(code_route, 1, 1), if_else(is.na(tmp), substr_part, as.character(tmp)))
  ) %>%
  select(-substr_part, -tmp)

# extract segment identifier
wal <- wal %>%
  mutate(
    substr_part = substr(code_route, 5, nchar(code_route)),
    tmp = as.integer(substr_part),
    segment = if_else(is.na(tmp), substr_part, as.character(tmp))
  ) %>%
  select(-substr_part, -tmp)

wal <- wal %>%
  mutate(id=paste0(code_route,"-",identifiant))

wal <- wal %>%
  select(id,road,segment,marker=identifiant_carto)


# remove geometric duplicates
wal_agg <- wal %>%
  mutate(geom_wkt = st_as_text(geometry)) %>%
  group_by(geom_wkt) %>%
  summarize(
    across(
      c(id, road, segment, marker),
      collapse_unique
    ),
    geometry = first(geometry) # Retain the geometry
  ) %>%
  ungroup() %>%
  select(-geom_wkt)

wal_agg <- wal_agg %>%
  mutate(data_list_id=dl_hectometer_wal)



cat("Total WAL signs:", nrow(wal_raw), "\n")
cat("Unique WAL signs:", nrow(wal), "\n")
cat("WAL unique geometries:", nrow(wal_agg), "\n")


### Unify the datasets ----

# reproject to wgs84

vl_agg <- st_transform(vl_agg, 4326)
bru_agg <- st_transform(bru_agg, 4326)
wal_agg <- st_transform(wal_agg, 4326)



# merge files
all_poles <- bind_rows(vl_agg, wal_agg, bru_agg)


# legend item ID
# If the pole is a round number, set as KM marker, else HM marker
all_poles <- all_poles %>%
  mutate(legend_item_id=ifelse(grepl("^[0-9]+$", marker),
                                   li_milemarker_km,
                               li_milemarker_hm))



# upload to raw data ----
CreateImportTable(dataset = vl_raw, schema = "raw_data", table_name = "vla_awv_hectometer")
CreateImportTable(dataset = bru_raw, schema = "raw_data", table_name = "bru_mob_traffic_signs")
CreateImportTable(dataset = wal_raw, schema = "raw_data", table_name = "spw_hectometer")
CreateImportTable(dataset = all_poles, schema = "raw_data", table_name = "hectometer_markers")

} else {
  print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function  


# LOAD ----
# """""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.hectometer_markers CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.hectometer_markers
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
  CONSTRAINT hectometer_markers_pkey PRIMARY KEY (id)
);",paste0("WITH cleaned as 
	(select id as original_id,
	LTRIM(concat_ws(' ',road,CASE WHEN segment IS NOT NULL AND segment!= '0' THEN '(' || segment || ')' END, marker)) as name,
	 road, segment, marker, legend_item_id, data_list_id,
	geometry from raw_data.hectometer_markers)
INSERT INTO ingestion.hectometer_markers 
(original_id, name, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
select original_id,
jsonb_build_object('und', CASE when name IS NULL THEN 'hectometer marker' else name end) as name,
legend_item_id::uuid as legend_item_id,
data_list_id::uuid as data_list_id,
0 as risk_level,
jsonb_strip_nulls(jsonb_build_object(
	'road_identifier', road,
	'subroad_identifier', segment,
	'road_distance', marker)) as properties,
ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.hectometer_markers OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hectometer_markers TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hectometer_markers TO pgn_user_airflow;"
)



### Execute the SQL commands ----
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}

run_smart_update = function() {
  smart_update_process("hectometer_markers", 2, 20, 10, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
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