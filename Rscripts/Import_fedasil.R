## ---------------------------
##
## Script name: Import reception centres from Fedasil
##
## Purpose of script: Load reception centres & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-12-02
##
##
## ---------------------------

# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------
# External IDs
data_list_id <- "290fa28c-fe90-46e5-967d-b75c4d847608"
legend_item_id <- "83416c0b-d13b-43f7-a528-3eb51cf8dcb9"

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


# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(rvest)
library(tibble)


# EXTRACT ----
# """""""""""""""""" ----

process_fresh_data <- function(){
  ## Geocoding libraries
  library(devtools)
  library(phacochr)
  phaco_setup_data()
  phacochr::phaco_best_data_update()
  
# DOWNLOAD THE DATA ----

# URL of the website
url <- "https://www.fedasil.be/nl/opvangcentra"

# Read the HTML content
webpage <- read_html(url)




# Read the page
url <- "https://www.fedasil.be/nl/opvangcentra"
webpage <- read_html(url)


# TRANSFORM ----
# """""""""""""""""" ----

# Extract all map points
locations <- html_elements(webpage, ".geolocation-location")



parse_location_info <- function(loc) {
  id <- html_attr(loc, "data-views-row-index") 
  lat <- html_attr(loc, "data-lat")
  lng <- html_attr(loc, "data-lng")
  
  title_node <- loc %>% html_element("h4 a")
  title <- title_node %>% html_text(trim = TRUE)
  url <- title_node %>% html_attr("href") %>% paste0("https://www.fedasil.be", .)  # optional domain prepend
  
  partner_type <- loc %>% html_element("h6.field-content") %>% html_text(trim = TRUE)
  
  # Extract and clean lines from <p>
  desc_html <- loc %>% html_element(".field-content p")
  desc_nodes <- xml2::xml_contents(desc_html)
  lines <- purrr::map_chr(desc_nodes, function(x) {
    if (xml2::xml_name(x) == "br") "\n" else xml2::xml_text(x)
  }) %>%
    paste(collapse = "") %>%
    str_split("\n", simplify = FALSE) %>%
    .[[1]] %>%
    str_trim() %>%
    discard(~ .x == "")
  
  # Handle name that may span two lines
  if (length(lines) >= 2 && !str_detect(lines[2], "\\d")) {
    name <- paste(lines[1], lines[2])
    lines <- lines[-2]
  } else {
    name <- lines[1]
  }
  
  # Remaining fields (at least street and postcode+municipality will exist)
  rest <- lines[-1]
  
  street_housenumber <- rest[1] %||% NA_character_
  postcode_municipality <- rest[2] %||% NA_character_
  phone <- NA_character_
  email <- NA_character_
  
  for (line in rest[-c(1,2)]) {
    if (str_detect(line, "@")) {
      email <- line
    } else {
      phone <- line
    }
  }
  
  tibble::tibble(
    id = id,
    partner_type = partner_type,
    place = title,
    url = url,
    name = name,
    street_housenumber = street_housenumber,
    postcode_municipality = postcode_municipality,
    phone = phone,
    email = email,
    latitude = as.numeric(lat),
    longitude = as.numeric(lng)
  )
}


map_data <- lapply(locations, parse_location_info) %>% bind_rows()

map_data <- map_data %>%
  mutate(
    phone = phone %>%
      str_remove_all("(?i)^tél\\s*:?[\\s]*|^tel\\s*:?[\\s]*|^t\\s*:?[\\s]*") %>%  # remove T, T:, Tél etc.
      str_replace_all("[/.]", " ") %>%                          # replace slashes and dots with space
      str_squish() %>%                                          # remove extra/multiple spaces
      str_replace("^0", "+32 ")                                 # replace leading 0 with +32
  )

# change Partenaire into Partner
map_data$partner_type <- map_data$partner_type %>%
  str_replace_all("Partenaire", "Partner")
table(map_data$partner_type)

# turn into SF dataset
parsed_data <- st_as_sf(map_data, coords = c("longitude", "latitude"), crs = 4326)


# Geocode the address (because the original locations are often not very exact) ----

parsed_data <- parsed_data %>% mutate(ad_hoc_id = row_number())
data_input_geocode <- as.data.frame(parsed_data) %>% select(ad_hoc_id, street_housenumber, postcode_municipality)


data_geocoded <- phaco_geocode(data_to_geocode=t_adresse <- data_input_geocode, colonne_num_rue= "street_housenumber",colonne_code_postal="postcode_municipality")

simple_geocode <- data_geocoded$data_geocoded_sf[, c("ad_hoc_id")]

simple_geocode <- as.data.frame(simple_geocode) %>%
  rename(geometry_geocoded = geometry)

# transform parsed data to lambert72
parsed_data <- st_transform(parsed_data, crs = 31370)
data_merged <- left_join(parsed_data, simple_geocode, by = "ad_hoc_id")


data_merged <- data_merged %>%
  group_by(geometry) %>%
  summarise(
    id = paste(unique(na.omit(id)), collapse = "; "),
    partner_type = paste(unique(na.omit(partner_type)), collapse = "; "),
    place = paste(unique(na.omit(place)), collapse = " / "),
    url = paste(unique(na.omit(url)), collapse = "; "),
    name = paste(unique(na.omit(name)), collapse = "; "),
    street_housenumber = paste(unique(na.omit(street_housenumber)), collapse = " / "),
    postcode_municipality = paste(unique(na.omit(postcode_municipality)), collapse = " / "),
    phone = paste(unique(na.omit(phone)), collapse = "; "),
    email = paste(unique(na.omit(email)), collapse = "; "),
    geometry_geocoded = first(geometry_geocoded[!st_is_empty(geometry_geocoded)])
  ) %>%
  ungroup()




#calculate distance beween geometry and geometry_geocoded
data_merged <- data_merged %>%
  rowwise() %>%
  mutate(distance = as.numeric(st_distance(geometry, geometry_geocoded))) %>%
  ungroup()

# only if geocoding failed, use original coordinates
data_merged <- data_merged %>%
  mutate(geometry_cleaned = ifelse(st_is_empty(geometry_geocoded), geometry, geometry_geocoded)) %>%
  select(-geometry_geocoded)
data_merged <- st_set_geometry(data_merged, "geometry_cleaned") %>%
  select(-geometry) %>%
  rename(geometry = geometry_cleaned)
# make it clear geometry is 31370
st_crs(data_merged) <- 31370
#transform back to 4326
data_merged <<- st_transform(data_merged, crs = 4326)




}


# LOAD ----
# """""""""""""""""" ----



### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.asylum_reception_centres CASCADE;
  ","
  CREATE TABLE IF NOT EXISTS ingestion.asylum_reception_centres
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
    CONSTRAINT asylum_reception_centres_pkey PRIMARY KEY (id)
  );",paste0("
  WITH cleaned as (
    SELECT
    id as original_id,
    jsonb_strip_nulls(jsonb_build_object(
      'und', CASE 
      WHEN name='Aanmeldcentrum' OR COUNT(*) OVER (PARTITION BY name) > 1 THEN name || ' ' || place
      ELSE name END)) AS name,
    jsonb_build_object(
      'dut', 'opvangcentrum voor asielzoekers',
      'fre', 'centre d''accueil pour demandeurs d''asile',
      'eng', 'reception centre for asylum seekers')
    as legend_item,
    JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	  'asylum_centre_operator_type', partner_type,
      'email', email,
      'phone', phone,
      'website', url,
      'address', LTRIM(CONCAT(REPLACE(street_housenumber,',',''),', ',street_housenumber)))) as properties,
    2 as risk_level,
    geometry
    FROM
    raw_data.fedasil_reception_parsed)
  
  INSERT INTO ingestion.asylum_reception_centres 
  (original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
  SELECT
  original_id,
  name,
  legend_item,
  '",legend_item_id,"'::uuid as legend_item_id,
  '",data_list_id,"'::uuid as data_list_id,
  risk_level,
  properties,
  geometry,
  CURRENT_DATE as created_at
  FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.asylum_reception_centres OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.asylum_reception_centres TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.asylum_reception_centres TO pgn_user_airflow;")


### Execute the SQL commands ----
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----


run_smart_update = function() {
  smart_update_process("asylum_reception_centres", 150, 175, 150, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = data_merged, schema = "raw_data", table_name = "fedasil_reception_parsed")
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}
