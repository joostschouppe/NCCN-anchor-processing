## ---------------------------
##
## Script name: ETL flow for bus/tram/metro stops
##
## Purpose of script: Merge data about bus/tram/metro stops from the three official sources and transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-08-05
##
##
## ---------------------------

# Docs: https://dev.azure.com/NCCN-Paragon/Paragon/_wiki/wikis/Paragon.wiki/756/Bus-tram#


#  """""""""""""""""" ----------------------

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

#data_list_id<-"cannot be added centrally, since several items are processed"
log_folder <- "C:/temp/logs/"
local_folder <- "C:/temp/"

### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))

# Set conditions -----------------------------------------------------------
# threshold for deciding a stop is a tram stop
distance_threshold_tram <- 15
#threshold for merging a stop outside of the usual region of the operator with a nearby stop of the usual operator in that region
distance_threshold_merge_extraregional <- 15


# Libraries -------------------------------
# """""""""""""""""" ----------------------


# Download data -----------------------------------------------------------
# """"""""""""""""""----------------------
### Vlaanderen ----

# WFS download as explained on https://inbo.github.io/tutorials/tutorials/spatial_wfs_services/

# Define the base WFS URL
wfs_haltes <- "https://geo.api.vlaanderen.be/Haltes/wfs"

# Build the request URL
build_request_url <- function(start_index) {
  url <- parse_url(wfs_haltes)
  url$query <- list(
    service = "wfs",
    request = "GetFeature",
    typename = "Haltes:Halte",
    srsName = "EPSG:31370",
    startIndex = start_index,
    maxFeatures = 10000,
    outputFormat = "application/json"
  )
  return(build_url(url))
}

# Initialize variables
all_features <- list()
start_index <- 0
batch_size <- 10000
has_more_features <- TRUE

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
vl_haltes <- do.call(rbind, all_features)

vl_haltes_raw<-vl_haltes

# Print the number of features retrieved
cat("Total VL stops retrieved:", nrow(vl_haltes), "\n")




# Download Flanders tram lines
wfs_reiswegen <- "https://geo.api.vlaanderen.be/Reiswegen/wfs"
url <- parse_url(wfs_reiswegen)
url$query <- list(service = "wfs",
                  #version = "2.0.0", # optional
                  request = "GetFeature",
                  typename = "Reiswegen:Reisweg",
                  srsName = "EPSG:31370"
)
request <- build_url(url)
reiswegen <- read_sf(request)
cat("Total VL routes retrieved:", nrow(reiswegen), "\n")

reiswegen_raw<-reiswegen

# turn into SF dataset
reiswegen <- reiswegen %>% 
  st_cast(to = "MULTILINESTRING")

# select only if voertuig=Tram
reiswegen_tram <- reiswegen %>% 
  filter(VOERTUIG == "Tram")


# keep only voertuig column
reiswegen_tram <- reiswegen_tram %>% 
  select(VOERTUIG)

# add to haltes using st_within distance_matched_threshold
haltes_joined <- st_join(vl_haltes, reiswegen_tram, join = st_is_within_distance, dist = distance_threshold_tram)

# keep only a single record for each id
haltes_joined <- haltes_joined %>% 
  distinct()

# set all columns to lower case
haltes_joined <- haltes_joined %>% 
  rename_all(tolower)

#standardize data format
vl_haltes<-haltes_joined


#if voertuig=Tram, then set tram=yes
vl_haltes<-vl_haltes %>%
  mutate(tram=ifelse(voertuig=="Tram", 1, 0))


# aggregate
# TODO: replace with faster procedure
vl_haltes_agg <- vl_haltes %>%
  mutate(geom_wkt = st_as_text(geometry)) %>%
  group_by(geom_wkt) %>%
  summarize(
    original_id = paste(unique(id), collapse = '; '),
    source="De Lijn",
    tram = max(tram),
    name_dut = paste(unique(naamhalte), collapse = '; '),
    stop_type= paste(unique(lbltypehal), collapse = '; '),
    stopid = paste(unique(stopid), collapse = '; ')) %>%
  select(-geom_wkt)


cat("Total VL stops retrieved:", nrow(vl_haltes), "\n")
cat("Total VL routes retrieved:", nrow(reiswegen), "\n")
cat("Total VL stops after processing:", nrow(vl_haltes_agg), "\n")



### Brussels ----
# download zip
# set the source
zip_url<-"https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/shapefiles-production/alternative_exports/shapefileszip/"
# set the location to save it
local_zip_path <- paste0(local_folder,"shapefiles.zip")
# Download the ZIP file
GET(url = zip_url, write_disk(local_zip_path, overwrite = TRUE))
# Unzip the downloaded file
unzip(local_zip_path, exdir = local_folder)

# List the contents of the unzipped directory
unzipped_contents <- list.dirs(paste0(local_folder,"shapefiles"), recursive = FALSE)
unzipped_folder <- unzipped_contents[1]

# open the shapefile 
brussels_stops <- st_read(paste0(unzipped_folder,"/ACTU_STOPS.shp"))
brussels_stops_raw<-brussels_stops

brussels_lines <- st_read(paste0(unzipped_folder,"/ACTU_LIGNES_BRUTES.shp"))
brussels_lines_raw<-brussels_lines

cat("Total BRU stops:", nrow(brussels_stops), "\n")
cat("Total BRU lines:", nrow(brussels_lines), "\n")

# delete the unzipped folder
unlink(paste0(local_folder,"shapefiles"), recursive = TRUE)

# turn POINT Z into just POINT geometry
brussels_stops <- brussels_stops %>%
  mutate(
    x = st_coordinates(geometry)[, 1],
    y = st_coordinates(geometry)[, 2],
    geometry = st_sfc(map2(x, y, ~ st_point(c(.x, .y))), crs = st_crs(geometry))
  ) %>%
  select(-x, -y) # Optionally remove the temporary x and y columns


brussels_stops <- brussels_stops %>%
  mutate(
    metro=ifelse(mode=="M", 1, 0),
    tram=ifelse(mode=="T", 1, 0),
    bus=ifelse(mode=="B", 1, 0)
  )


brussels_stops_agg <- brussels_stops %>%
  mutate(geom_wkt = st_as_text(geometry)) %>%
  group_by(geom_wkt) %>%
  summarize(
    original_id = paste(unique(stop_id), collapse = '; '),
    source="STIB",
    tram = max(tram),
    metro = max(metro),
    bus = max(bus),
    name_dut = paste(unique(alpha_nl), collapse = '; '),
    name_fre = paste(unique(alpha_fr), collapse = '; ')
  ) %>%
  select(-geom_wkt)

cat("Total BRU stops:", nrow(brussels_stops), "\n")
cat("Total BRU lines:", nrow(brussels_lines), "\n")
cat("Total BRU stops after processing:", nrow(brussels_stops_agg), "\n")


### Wallonia ----

# download from WFS

# WFS download as explained on https://inbo.github.io/tutorials/tutorials/spatial_wfs_services/

# Define the base WFS URL
wfs_haltes <- "https://geodata.tec-wl.be/server/services/Poteaux/MapServer/WFSServer"

# Build the request URL
build_request_url <- function(start_index) {
  url <- parse_url(wfs_haltes)
  url$query <- list(
    service = "wfs",
    request = "GetFeature",
    typename = "Poteaux",
    srsName = "EPSG:31370",
    startIndex = start_index,
    maxFeatures = 10000,
    outputFormat = "GEOJSON"
  )
  return(build_url(url))
}

# Initialize variables
all_features <- list()
start_index <- 0
batch_size <- 10000
has_more_features <- TRUE

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
wal_poteaux <- do.call(rbind, all_features)

cat("Total WAL stops:", nrow(wal_poteaux), "\n")

wal_poteaux_raw <- wal_poteaux


# Download Wallonia lines
# Define the base WFS URL
wfs_haltes <- "https://geodata.tec-wl.be/server/services/Lignes/MapServer/WFSServer"

# Build the request URL
build_request_url <- function(start_index) {
  url <- parse_url(wfs_haltes)
  url$query <- list(
    service = "wfs",
    request = "GetFeature",
    typename = "Lignes",
    srsName = "EPSG:31370",
    startIndex = start_index,
    maxFeatures = 10000,
    outputFormat = "GEOJSON"
  )
  return(build_url(url))
}

# Initialize variables
all_features <- list()
start_index <- 0
batch_size <- 10000
has_more_features <- TRUE

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
wal_lines <- do.call(rbind, all_features)
cat("Total WAL lines:", nrow(wal_lines), "\n")

wal_lines_raw <- wal_lines

# metro lines: "LGN_NUM" = 'M1', M2', 'M3', 'M3AB', 'M4'
wal_lines <- wal_lines %>%
  mutate(tram=ifelse(LGN_NUM %in% c('M1','M1AB', 'M2', 'M3', 'M3AB', 'M4', 'M4AB'),1,0)) %>%
  mutate(metro=0) %>%
  mutate(bus=ifelse(LGN_NUM %in% c('M1','M1AB', 'M2', 'M3', 'M3AB', 'M4', 'M4AB'),0,1))

# check: count the number of times any value of lgn_id is used
wal_lines_check <- as.data.frame(wal_lines) %>%
  group_by(lgn_id) %>%
  summarize(n=n()) %>%
  filter(n>1)

cat("Number of duplicate WAL line IDs:", nrow(wal_lines_check), "\n")

# keep only voertuig column
wal_lines_tram <- wal_lines %>% 
  filter(tram==1) %>%
  select(tram)

# add to poteaux using st_within distance_matched_threshold
wal_poteaux_joined <- st_join(wal_poteaux, wal_lines_tram, join = st_is_within_distance, dist = distance_threshold_tram)

# keep only a single record for each id
wal_poteaux_joined <- wal_poteaux_joined %>% 
  distinct()


# spatial aggregation
wal_poteaux_agg <- wal_poteaux_joined %>%
  mutate(geom_wkt = st_as_text(geometry)) %>%
  group_by(geom_wkt) %>%
  summarize(
    original_id = paste(unique(GlobalID), collapse = '; '),
    source="TEC",
    name_fre = paste(unique(POT_NOM), collapse = '; '),
    arret_id = paste(unique(ARRET_ID), collapse = '; '),
    pmr = paste(unique(PMR), collapse = '; '),
    pot_id = paste(unique(POT_ID), collapse = '; '),
    tram = max(tram)
  ) %>%
  select(-geom_wkt)

cat("Total WAL stops:", nrow(wal_poteaux), "\n")
cat("Total WAL lines:", nrow(wal_lines), "\n")
cat("Total WAL stops after simplify:", nrow(wal_poteaux_agg), "\n")


### Unify the lines ----

# VL set all names to lower
# keep only some columns
reiswegen <- reiswegen %>%
  rename_all(tolower) %>%
  select(oidn, routeid, lijn, naamlijn, voertuig, variant, variantid) %>%
  mutate(source="De Lijn") %>%
  mutate(tram=ifelse(voertuig=="Tram",1,0)) %>%
  mutate(bus=ifelse(voertuig=="Bus",1,0)) %>%
  select(-voertuig)

# rename geometry column shape to geometry
reiswegen <- reiswegen %>%
  rename(geometry=shape)

# BRU
brussels_lines <- brussels_lines %>%
  select(LIGNE,VARIANTE) %>%
  mutate(source="STIB") %>%
  rename_all(tolower)

# extract the last digit of the ligne
brussels_lines <- brussels_lines %>% 
  mutate(
    last_char = substr(ligne, nchar(ligne), nchar(ligne)), # Extract the last character
    metro = ifelse(last_char == "m", 1, 0),                # 1 if last character is 'm', else 0
    tram = ifelse(last_char == "t", 1, 0),                 # 1 if last character is 't', else 0
    bus = ifelse(last_char == "b", 1, 0)                   # 1 if last character is 'b', else 0
  ) %>%
  select(-last_char) # Optionally remove the helper column `last_char`  

# WAL
wal_lines <- wal_lines %>%
  rename_all(tolower) %>%
  select(-gmlid,-shape_length) %>%
  mutate(source="TEC")

# merge all lines
all_lines <- bind_rows(reiswegen, brussels_lines, wal_lines)

all_lines <- all_lines %>%
  mutate(data_list_id = ifelse(
    source == "STIB", '903be823-7368-48bf-835e-841699dff861', 
    ifelse(source == "De Lijn", 'bdbb2fa4-0902-4496-a1e1-5aaebf2fbccf', 
           ifelse(source == "TEC", '315ed87c-559e-4dfe-86fc-933ed76bae69', NA))))


### Unify the stops ----


# combine all stops
all_stops <- bind_rows(vl_haltes_agg, brussels_stops_agg, wal_poteaux_agg)

# join region

### Download NGI data
con_pg <- get_con()
ngi_muni <- dbGetQuery(con_pg, "SELECT CASE WHEN languagestatute=1 THEN 'vl'
WHEN languagestatute=5 THEN 'vl'
WHEN languagestatute=4 THEN 'brussels'	
WHEN languagestatute=8 THEN 'wal'	
WHEN languagestatute=7 THEN 'wal'		
WHEN languagestatute=6 THEN 'wal'
WHEN languagestatute=2 THEN 'wal'
ELSE 'und' END as region, ST_AsText(shape) as geometry FROM raw_data.ngi_ign_municipality")
dbDisconnect(con_pg)
ngi_muni<-st_as_sf(ngi_muni, wkt="geometry")
ngi_muni$geometry <- st_set_crs(ngi_muni$geometry, 4326)
#reproject to 31370
ngi_muni <- st_transform(ngi_muni, 31370)



# spatial join to stop data
all_stops <- st_join(all_stops, ngi_muni, join = st_within)

# identify stops outside their main region
all_stops <- all_stops %>%
  mutate (region_type=ifelse(
    (region == "vl" & (source == "STIB" | source == "TEC")) |
      (region == "wal" & (source == "STIB" | source =="De Lijn")) |
      (region == "brussels" & (source == "TEC" | source == "De Lijn")), "special", "normal"))


# add data_list_id
all_stops <- all_stops %>%
  mutate(data_list_id = ifelse(
    source == "STIB", 'fddda401-6757-4057-a7d9-20484b47b55a', 
    ifelse(source == "De Lijn", '6d678593-8feb-4b39-97a5-47a3b5b20f62', 
           ifelse(source == "TEC", '320b07a2-0c1c-4647-a0b8-a0021190d38b', NA))))





# merge stops from outside the region to nearest stop within the region if closer than 15 meters
# 1. Separate the stops into normal and special
normal_stops <- all_stops %>% filter(region_type == "normal")
special_stops <- all_stops %>% filter(region_type == "special")

# 2. Calculate the nearest normal stop for each special stop
# Get the distance between each special stop and all normal stops
distances <- st_distance(special_stops, normal_stops)

# 3. Identify the closest normal stop
# For each special stop, find the index of the nearest normal stop
min_distances <- apply(distances, 1, min)
nearest_indices <- apply(distances, 1, which.min)

# 4. Assign merge_id if the distance is less than 15 meters
special_stops <- special_stops %>%
  mutate(merge_id = ifelse(min_distances < 15, normal_stops$original_id[nearest_indices], NA))
special_stops <- as.data.frame(special_stops) %>%
  select(original_id,merge_id) %>%
  filter(!is.na(merge_id))


# 5. Combine the datasets back together
all_stops <- all_stops %>%
  left_join(special_stops, by = c("original_id" = "original_id")) %>%
  mutate(merge_id = ifelse(is.na(merge_id), original_id, merge_id))

# 6. Aggregate the stops

#isolate "normal" stops
## add a count of the number of times a merge_id exists
all_stops <- all_stops %>%
  group_by(merge_id) %>%
  mutate(count = n()) %>%
  ungroup()

# create tec_properties
all_stops <- all_stops %>%
  rowwise() %>%
  mutate(tec_properties = jsonlite::toJSON(
    purrr::discard(list(
      original_id = original_id,
      arret_id=arret_id,
      pmr=pmr,
      pot_id=pot_id
    ), is.na), auto_unbox = TRUE))

# create delijn_properties

all_stops <- all_stops %>%
  rowwise() %>%
  mutate(delijn_properties = jsonlite::toJSON(
    purrr::discard(list(
      original_id = original_id,
      stop_type= stop_type,
      stopid = stopid),
      is.na), auto_unbox = TRUE))

# create mivb_properties
all_stops <- all_stops %>%
  rowwise() %>%
  mutate(mivb_properties = jsonlite::toJSON(
    purrr::discard(list(
      original_id = original_id),
      is.na), auto_unbox = TRUE))


# remove the columns used to make properties
all_stops <- all_stops %>%
  select(-arret_id, -pot_id, -pmr, -stop_type, -stopid)


all_stops <- all_stops %>%
  group_by(merge_id) %>%
  mutate(
    # Identify the group leader's source where original_id equals group_id
    group_leader_source = source[original_id == merge_id],
    # Set count = 1 if the record is special and has the same source as the group leader
    count = if_else(
      count > 1 & region_type == "special" & source == group_leader_source,
      1,
      count
    )
  ) %>%
  ungroup() %>%
  select(-group_leader_source)  # Remove temporary column if not needed



# put the correct properties in the correct columns
## select rows where count > 1 and the source is not the source of the region. Remove these rows from the main dataset
mergeable_stops <- all_stops %>% filter(count > 1 & region_type == "special")
unmergeable_stops <- all_stops %>% filter(count == 1 | (count > 1 & region_type == "normal"))

## in the main dataset, set the properties column with the content of that region if count=1 or count>1 and source is the source of the region
unmergeable_stops <- unmergeable_stops %>%
  mutate(properties = case_when(
    source == "STIB" ~ mivb_properties,
    source == "De Lijn" ~ delijn_properties,
    source == "TEC" ~ tec_properties
  ))
unmergeable_stops <- unmergeable_stops %>%
  select(-mivb_properties, -delijn_properties, -tec_properties)

## in the selection, set properties_secondary with the content of the source
mergeable_stops <- mergeable_stops %>%
  mutate(properties_secondary = case_when(
    source == "STIB" ~ mivb_properties,
    source == "De Lijn" ~ delijn_properties,
    source == "TEC" ~ tec_properties
  ))

## add the uuid of the source to the properties

library(jqr)
# Define jq expressions
jq_expression_stib <- '. | {"fddda401-6757-4057-a7d9-20484b47b55a": .}'
jq_expression_delijn <- '. | {"6d678593-8feb-4b39-97a5-47a3b5b20f62": .}'
jq_expression_tec <- '. | {"320b07a2-0c1c-4647-a0b8-a0021190d38b": .}'


# Function to apply jq transformation safely
apply_jq_safe <- function(json_str, jq_expr) {
  if (!is.na(json_str) && json_str != "") {
    tryCatch(
      jqr::jq(json_str, jq_expr),
      error = function(e) {
        json_str  # return the original json_str if jq fails
      }
    )
  } else {
    json_str  # return the original json_str if it is NA or empty
  }
}

# Apply jq transformation cell by cell within the off_properties column
mergeable_stops <- mergeable_stops %>%
  mutate(properties_secondary = case_when(
    "STIB"== source ~ sapply(properties_secondary, apply_jq_safe, jq_expression_stib),
    "De Lijn"== source ~ sapply(properties_secondary, apply_jq_safe, jq_expression_delijn),
    "TEC"== source ~ sapply(properties_secondary, apply_jq_safe, jq_expression_tec)
  ))
# deactivating jqr as it affects general R behavior
detach("package:jqr", unload = TRUE)




## keep only relevant information in the selection
mergeable_stops <- as.data.frame(mergeable_stops) %>%
  select(merge_id,properties_secondary,source)


## aggregate by merge_id
mergeable_stops <- mergeable_stops %>%
  group_by(merge_id) %>%
  mutate(
    properties_secondary = list(toJSON(reduce(map(properties_secondary, fromJSON), modifyList), auto_unbox = TRUE)),
    source_secondary = paste(unique(source), collapse = '; ')) %>%
  ungroup() %>%
  distinct(merge_id, .keep_all = TRUE) %>%
  select(-source)


## left join the selection based on the merge id
all_stops_merged <- unmergeable_stops %>%
  left_join(mergeable_stops, by = "merge_id")

all_stops_merged <- all_stops_merged %>%
  select(-count, -merge_id)

## set columns as text
all_stops_merged <- all_stops_merged %>%
  mutate(
    properties = as.character(properties),
    properties_secondary = as.character(properties_secondary)
  )

## merge secondary source to source
all_stops_merged <- all_stops_merged %>% 
  mutate(source = if_else(is.na(source_secondary), source, paste(source, source_secondary, sep = ", "))) %>%
  select(-source_secondary)


# LOAD ----
# """""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_stops_sql <- c("DROP TABLE IF EXISTS ingestion.bus_tram_metro_stops CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.bus_tram_metro_stops
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
  CONSTRAINT bus_tram_metro_stops_pkey PRIMARY KEY (id)
);",paste0(" WITH 
cleaned as (SELECT
            original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'fre', CASE WHEN (name_fre='' OR name_fre is NULL) AND name_dut IS NULL THEN 'halte openbaar vervoer'
              WHEN name_fre='' OR name_fre is NULL THEN name_dut 
              ELSE name_fre END,
              'dut', CASE WHEN (name_dut='' OR name_dut is NULL) AND name_fre IS NULL THEN 'arrêt de transport en commun'
              WHEN name_dut='' OR name_dut is NULL THEN name_fre 
              ELSE name_dut END)) as name,
            jsonb_build_object(
              'dut', CASE WHEN source='TEC' then 'halte openbaar vervoer' || ' (' || source || ')'
              WHEN metro=1 then 'metrohalte' || ' (' || source || ')'
              WHEN tram=1 then 'tramhalte' || ' (' || source || ')'
              ELSE 'bushalte' || ' (' || source || ')' END,
              'fre', CASE WHEN source='TEC' then 'arrêt de transport en commun' || ' (' || source || ')'
              WHEN metro=1 then 'arrêt de métro' || ' (' || source || ')'
              WHEN tram=1 then 'arrêt de tram' || ' (' || source || ')'
              ELSE 'arrêt de bus' || ' (' || source || ')' END,
              'ger', CASE WHEN source='TEC' then 'Haltestelle' || ' (' || source || ')'
              WHEN metro=1 then 'U-Bahn-Haltestelle' || ' (' || source || ')'
              WHEN tram=1 then 'Straßenbahnhaltestelle' || ' (' || source || ')'
              ELSE 'Bushaltestelle' || ' (' || source || ')' END,
              'eng', CASE WHEN source='TEC' then 'public transport stop' || ' (' || source || ')'
              WHEN metro=1 then 'metro stop' || ' (' || source || ')'
              WHEN tram=1 then 'tram stop' || ' (' || source || ')'
              ELSE 'bus stop' || ' (' || source || ')' END) as legend_item,
            data_list_id::uuid,
            properties,
            CASE WHEN properties_secondary='NULL' THEN NULL ELSE properties_secondary END as properties_secondary,
            ST_Transform(geometry, 4326) AS geometry 
            FROM raw_data.all_public_transport_stops
)
INSERT INTO ingestion.bus_tram_metro_stops 
(original_id, name, legend_item, data_list_id, risk_level, properties, properties_secondary, geometry, created_at)
SELECT
original_id,
name,
legend_item,
data_list_id,
1 as risk_level,
properties::jsonb,
properties_secondary::jsonb,
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))


ingestion_table_routes_sql <- c("DROP TABLE IF EXISTS ingestion.bus_tram_metro_routes CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.bus_tram_metro_routes
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
  CONSTRAINT bus_tram_metro_routes_pkey PRIMARY KEY (id)
);",paste0("WITH cleaned AS (
  SELECT
  CASE WHEN source='De Lijn' THEN oidn::text
  WHEN source='TEC' THEN lgn_id
  ELSE ligne || '_' || variante END
  as original_id,
  jsonb_build_object('und',CASE WHEN source='De Lijn' THEN variant || ' (' || lijn || ')'
                     WHEN source='TEC' THEN lgn_nom || ' (' || lgn_num || ')'
                     ELSE ligne END)
  as name,
  jsonb_build_object(
    'dut', CASE WHEN metro=1 then 'metrolijn'
    WHEN tram=1 then 'tramlijn'
    ELSE 'buslijn' END,
    'fre', CASE WHEN metro=1 then 'ligne de métro'
    WHEN tram=1 then 'ligne de tram'
    ELSE 'ligne de bus' END,
    'eng', CASE WHEN metro=1 then 'metro line'
    WHEN tram=1 then 'tram line'
    ELSE 'bus line' END,
    'ger', CASE WHEN metro=1 then 'U-Bahn-Linie'
    WHEN tram=1 then 'Straßenbahnlinie'
    ELSE 'Buslinie' END
  ) as legend_item,
  data_list_id::uuid as data_list_id,
  1 as risk_level,
  jsonb_strip_nulls(jsonb_build_object(
    'routeid', routeid,
    'lijn', lijn,
    'naamlijn', naamlijn,
    'variantid', variantid,
    'objectid', objectid,
    'lgn_id', lgn_id,
    'lgn_nom', lgn_nom,
    'lgn_num', lgn_num)) as properties,
  ST_Transform(geometry, 4326) AS geometry 
  FROM raw_data.all_public_transport_routes)
INSERT INTO ingestion.bus_tram_metro_routes 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
data_list_id,
1 as risk_level,
properties::jsonb,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"))



### Create transformation table ----
transformation_table_stops_sql <- c("
DROP TABLE IF EXISTS transformation.bus_tram_metro_stops CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.bus_tram_metro_stops
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
    CONSTRAINT bus_tram_metro_stops_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.bus_tram_metro_stops
(id, original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id, properties, geometry, created_at FROM ingestion.bus_tram_metro_stops;
")

transformation_table_routes_sql <- c("
DROP TABLE IF EXISTS transformation.bus_tram_metro_routes CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.bus_tram_metro_routes
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
    CONSTRAINT bus_tram_metro_routes_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.bus_tram_metro_routes
(id, original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id, properties, geometry, created_at FROM ingestion.bus_tram_metro_routes;
")


### Create fdw views ----
fdw_views_stops_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_bus_tram_metro_stops CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_bus_tram_metro_stops
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
FROM transformation.bus_tram_metro_stops;
","
GRANT ALL ON TABLE fdw.fdw_bus_tram_metro_stops TO paragon;
")

fdw_views_routes_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_bus_tram_metro_routes CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_bus_tram_metro_routes
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
FROM transformation.bus_tram_metro_routes;
","
GRANT ALL ON TABLE fdw.fdw_bus_tram_metro_routes TO paragon;
")



### Execute the SQL commands ----
execute_sql_commands <- function(sql_commands, task_name) {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in sql_commands) {
        dbExecute(con_pg, sql_command)
      }
      print(paste(task_name, "SQL ran without error"))
    },
    error = function(err) {
      message(paste("The SQL functions for", task_name, "failed"))
      message(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}

# Now you can call this function for different tasks:

create_ingestion_table_stops <- function() {execute_sql_commands(ingestion_table_stops_sql, "Stops Ingestion table")}
create_ingestion_table_routes <- function() {execute_sql_commands(ingestion_table_routes_sql, "Routes Ingestion table")}
create_transformation_table_stops <- function() {execute_sql_commands(transformation_table_stops_sql, "Stops Transformation table")}
create_transformation_table_routes <- function() {execute_sql_commands(transformation_table_routes_sql, "Routes Transformation table")}
create_fdw_views_stops <- function() {execute_sql_commands(fdw_views_stops_sql, "FDW Stops views")}
create_fdw_views_routes <- function() {execute_sql_commands(fdw_views_routes_sql, "FDW Routes views")}



# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("bus_tram_metro_stops", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
  smart_update_process("bus_tram_metro_routes", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}





# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = vl_haltes_raw, schema = "raw_data", table_name = "vla_delijn_stops") 
  CreateImportTable(dataset = reiswegen_raw, schema = "raw_data", table_name = "vla_delijn_routes") 
  CreateImportTable(dataset = brussels_stops_raw, schema = "raw_data", table_name = "bru_mivb_stib_stops") 
  CreateImportTable(dataset = wal_poteaux_raw, schema = "raw_data", table_name = "wal_tec_stops") 
  CreateImportTable(dataset = all_stops_merged, schema = "raw_data", table_name = "all_public_transport_stops") 
  CreateImportTable(dataset = all_lines, schema = "raw_data", table_name = "all_public_transport_routes")
  CreateImportTable(dataset = brussels_lines_raw, schema = "raw_data", table_name = "bru_mivb_stib_routes")
  CreateImportTable(dataset = wal_lines_raw, schema = "raw_data", table_name = "wal_tec_routes")
  create_ingestion_table_stops()
  create_ingestion_table_routes()
  run_smart_update()
  #create_transformation_table_stops()
  #create_transformation_table_routes()
  #create_fdw_views_stops()
  #create_fdw_views_routes()
}

if(F){
  main_function()
}



