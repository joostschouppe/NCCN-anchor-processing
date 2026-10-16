## ---------------------------
##
## Script name: Import scout camps
##
## Purpose of script: Load scout camp data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2025-06-30
##
##
## ---------------------------


## Load environment ----

legend_item_id <- "ce698d55-5d83-4db3-95a2-affb1b6b33ad"
data_list_id <- "3e1581c5-ab7e-42f9-97dc-59dd208b2891"


#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

# connection details
db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))


# run status
run_status<-Sys.getenv("RUN_STATUS")
## this is set to false and prevents any accedental changes to the database by switching off the main_function(). On Airflow, this is set to true.

# overrule the checks
overrule_checks<-Sys.getenv("OVERRULE_CHECKS")
## Set to FALSE by default. That means we do not update the anchors if some tests fail. Those tests include "the data has grown or shrunk by a lot of objects". If, after review of the log, you decide that nothing is wrong, set this manually to TRUE.
# If the input is not correctly understood as boolean, this will force it to it.
overrule_checks<-ifelse(tolower(overrule_checks) == "true", TRUE, FALSE)
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

# Set location for files received by email
offline_storage <-Sys.getenv("OFFLINE_STORAGE")



# Extra libraries -------------------------------
# """""""""""""""""" ----------------------

library(writexl)
library(rvest)
library(readxl)

## Geocoding libraries
library(devtools)
library(phacochr)
phaco_setup_data()
phacochr::phaco_best_data_update()

# avoid scientific notation
options(scipen = 999)

## Load data ----

# the data contains a lot of duplicates
# they are hard to detect automatically
# when they are full duplicates, add deleted=yes in the excel
# when they are full duplicates but with other contact info, change the volgnummer to be the same

# the data may contain obviously wrong dates. Check camps starting or ending before or after the summer; ending before they start

# review the addresses of objects that are not geocoded
# have a look at the x and y columns. Convert notations like 12°12'12'' to decimal
# check for huge numbers - usually they can be converted by manually adding a comma after 5 or 51 (depending on whether it's an X or Y)

# open excel "C:\temp\scouts\sjabloon-limburg-scout-paragon.xlsx"
scout <- read_excel(file.path(offline_storage, "scout/20260702 Gegevens jeugdkampen Limburg 2026.xlsx"))

# remove if deleted=yes
scout <- scout %>%
  filter(deleted != "yes" | is.na(deleted))


## Geocode the data ----
scout_sel <- scout %>% select (volgnummer, street, number, ZIP)
scout_geocoded <- phaco_geocode(data_to_geocode=t_adresse <- scout_sel, colonne_rue= "street", colonne_num="number", colonne_code_postal="ZIP")

## change geometry column name and add results to all records
full_geocode <- scout_geocoded$data_geocoded_sf
simple_geocode <- scout_geocoded$data_geocoded_sf[, c("volgnummer", "street_detected", "type_geocoding", "tx_munty_descr_nl")]
scout <- left_join(scout, simple_geocode, by = "volgnummer")

# select records without valid geometry
scout_nofind <- scout %>%
  filter(st_is_empty(geometry))

## deal with geometries

# create geometry_scout from x and y columns
scout_sf <- scout %>%
  mutate(geometry_scout = st_sfc(
    map2(x, y, ~ st_point(c(.x, .y))),
    crs = 4326
  ))
# transform to lambert 72
scout_sf$geometry_scout <- st_transform(scout_sf$geometry_scout, 31370)

# calculate distance between geometry and geometry_scout
scout_sf <- scout_sf %>%
  mutate(distance = as.numeric(st_distance(geometry, geometry_scout, by_element = TRUE)))


# decide which geom to use
## if the distance is great, trust address not coordinates
## if there are no coordinates, use the geocoded location
## in all other cases prefer the provided coordinates
scout_sf <- scout_sf %>%
  mutate(new_geometry = 
           case_when(distance>1500 ~ geometry,
                st_is_empty(geometry_scout) ~ geometry,
                      TRUE ~ geometry_scout))
# if municipality is not identical, flag as possible issue
# if address not found, flag as issue
scout_sf <- scout_sf %>%
  mutate(quality_remark = 
           case_when(
                               st_is_empty(new_geometry) ~ "address not found",
                               city != tx_munty_descr_nl ~ "municipality name mismatch between raw data and location",
                               distance > 499 ~ "address over 500 meters from raw data location",
                               str_detect(type_geocoding, "elargissement_adj") ~ "post number mismatch between raw data and found address"))

scout_sf$geometry_scout <- st_transform(scout_sf$geometry_scout, 4326)
scout_sf$new_geometry <- st_transform(scout_sf$new_geometry, 4326)
scout_sf$geometry <- st_transform(scout_sf$geometry, 4326)

# save a field with an URL with an OSM route from one geometry to the other
scout_sf <- scout_sf %>%
  mutate(
    osm_route_url = {
      coords_from <- st_coordinates(geometry)
      coords_to   <- st_coordinates(new_geometry)
      sprintf(
        "https://www.openstreetmap.org/directions?from=%.6f%%2C%.6f&to=%.6f%%2C%.6f",
        coords_from[, "Y"], coords_from[, "X"],
        coords_to[, "Y"],   coords_to[, "X"]
      )
    }
  )
 

# remove unneeded geometry columns
scout_sf<-as.data.frame(scout_sf) %>%
  select(-geometry, -geometry_scout) %>%
  rename(geometry = new_geometry)

# set the list of coordinates at geometry_def as geometry
scout_sf <- scout_sf %>%
  st_set_geometry("geometry")


# clean phone number



# write to geojson at C:\temp\scouts\
#st_write(scout_sf, file.path(offline_storage, "scout/Gegevens jeugdkampen Limburg 2026 processed.geojson"), driver = "GeoJSON", delete_dsn = TRUE)
# write to KML
#st_write(scout_sf, "C:/temp/scouts/sjabloon-limburg-scout-paragon-2025.kml", driver = "kml", delete_dsn = TRUE)
# write to XLSX
#write_xlsx(scout_sf, file.path(offline_storage, "scout/delivery 2 Gegevens jeugdkampen Limburg 2026 processed.xlsx"))



standardize_be_phone <- function(x) {
  # Strip everything except digits and leading +
  cleaned <- gsub("[^0-9+]", "", x)
  
  # Case 1: already has country code with +
  has_plus <- grepl("^\\+", cleaned)
  
  # Case 2: starts with 00 (alt international prefix) -> convert to +
  cleaned <- ifelse(grepl("^00", cleaned), paste0("+", substring(cleaned, 3)), cleaned)
  has_plus <- grepl("^\\+", cleaned)
  
  # Case 3: starts with country code 32 but no + (e.g. "32488595528")
  starts_32_no_plus <- !has_plus & grepl("^32", cleaned) & nchar(cleaned) %in% c(10, 11)
  cleaned <- ifelse(starts_32_no_plus, paste0("+", cleaned), cleaned)
  has_plus <- grepl("^\\+", cleaned)
  
  # Case 4: starts with 0 (standard BE local format) -> replace 0 with +32
  starts_0 <- !has_plus & grepl("^0", cleaned)
  cleaned <- ifelse(starts_0, paste0("+32", substring(cleaned, 2)), cleaned)
  has_plus <- grepl("^\\+", cleaned)
  
  # Case 5: no +, no leading 0, no 32 prefix -> bare 9-digit number missing leading 0
  # e.g. "478335088" (9 digits) -> add +32
  bare_8_or_9digit <- !has_plus & grepl("^[0-9]{8,9}$", cleaned)
  cleaned <- ifelse(bare_8_or_9digit, paste0("+32", cleaned), cleaned)
  
  cleaned
}

scout_sf <- scout_sf %>%
  rename(phone = `tags (keyword)`)



scout_sf <- scout_sf %>%
  mutate(phone_cleaned=standardize_be_phone(phone))



# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c(
  "DROP TABLE IF EXISTS ingestion.scout_camps CASCADE;",
  "CREATE TABLE IF NOT EXISTS ingestion.scout_camps
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
  CONSTRAINT scout_camps_pkey PRIMARY KEY (id)
);",
  paste0("with base as (select *, 
	LTRIM(CONCAT(street, ' ' || CASE WHEN \"number\" IS NULL or \"number\" = '0' THEN 'w/n' ELSE number END, ', ' || CONCAT((\"ZIP\" || ' '), city)))
	AS address
	FROM raw_data.scout_limburg_2026),

aggregated as (
select 
volgnummer as original_id,
STRING_AGG(DISTINCT name,'; ') AS name,
STRING_AGG(DISTINCT address,'; ') AS address,
STRING_AGG(DISTINCT risk,'; ') AS contact_person,
STRING_AGG(DISTINCT phone_cleaned,'; ') AS local_phone,
MIN(\"start date\") as start_date,
MAX(\"end date\") as end_date,
MIN(quality_remark) as quality_remark,
MIN(geometry) as geometry
FROM base		 
group by original_id),

cleaned as (SELECT original_id,
	jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN 'youth camp' ELSE name END,
              'dut', name) as name,
	jsonb_build_object(
              'dut', 'zomerkamp',
              'fre', 'camp d''été',
              'ger', 'Sommercamp',
              'eng', 'summer camp') as legend_item,
	2 as risk_level,
	'",legend_item_id,"'::uuid as legend_item_id,
  '",data_list_id,"'::uuid as data_list_id,
	address,
	contact_person,
	local_phone,
	start_date,
	end_date,
	quality_remark,
	geometry
FROM aggregated)

INSERT INTO ingestion.scout_camps
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
legend_item_id,
data_list_id,
risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'address', address,
	'local_phone',local_phone,
	'contact_person',contact_person,
	'start_date',start_date,
	'end_date',end_date
)),
ST_ReducePrecision(geometry, 1e-8),
CURRENT_DATE as created_at
FROM cleaned;
"),
"ALTER TABLE ingestion.scout_camps OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.scout_camps TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.scout_camps TO pgn_user_airflow;")


create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}





do_dry_run<-TRUE #set to FALSE when you're happy with results
reuse_ingestion_data<-FALSE 
overrule_checks<-TRUE # set to TRUE because the checks will likely fail, as it is normal in this case that a very high percentage of records change
run_smart_update = function() {
  smart_update_process("scout_camps", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



main_function = function() {
  CreateImportTable(dataset = scout_sf, schema = "raw_data", table_name = "scout_limburg_2026")  
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
}
