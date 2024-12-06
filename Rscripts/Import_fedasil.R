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

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

data_list_id<-"290fa28c-fe90-46e5-967d-b75c4d847608"
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

library(stringr)
library(dplyr)
library(rvest)
library(purrr)
library(jsonlite)
library(tidyr)

## Geocoding libraries
library(devtools)
library(phacochr)
phaco_setup_data()
phacochr::phaco_best_data_update()


# EXTRACT ----
# """""""""""""""""" ----



# DOWNLOAD THE DATA ----

# URL of the website
url <- "https://www.fedasil.be/nl/opvangcentra"

# Read the HTML content
webpage <- read_html(url)

# Extract the script tags
scripts <- webpage %>% html_nodes("script") %>% html_text()

# Check the content of the scripts
gmap<-scripts[grep("gmap", scripts)]  # Look for scripts related to the Google Map

# Find the JSON string (you may need to extract it from a longer string)
json_data <- as.data.frame(gmap[grep("markers", gmap)])

markers_raw <- str_extract(json_data, "\\[\\{.*?\\}\\]")

markers <- fromJSON(markers_raw)

# Convert to a data frame
markers_df <- as.data.frame(markers)


# TRANSFORM ----
# """""""""""""""""" ----




# CLEAN THE DATA ----
# Function to normalize and clean the HTML text
normalize_text <- function(html_text) {
  str_replace_all(html_text, "\\s+", " ") # Collapse all whitespace into single spaces
}

# Function to extract the first <p><p> content using regex
extract_first_p <- function(html_text) {
  # Normalize the text and remove extra spaces
  cleaned_html <- normalize_text(html_text)
  
  # Regex to capture content inside the first <p><p> block
  match <- str_match(cleaned_html, "<p>\\s*<p>(.*?)</p>")
  
  # Return the captured content, or NA if not found
  if (!is.na(match[2])) {
    return(match[2])
  } else {
    return(NA)
  }
}

# Function to extract fields from the address block
extract_fields <- function(address_block) {
  # Split by <br /> and trim whitespace
  parts <- str_split(address_block, "<br />")[[1]] %>% str_trim()
  
  # Extract location name, street, and postcode
  location_name <- ifelse(length(parts) >= 1, parts[1], NA)
  street_housenumber <- ifelse(length(parts) >= 2, parts[2], NA)
  postcode_municipality <- ifelse(length(parts) >= 3, parts[3], NA)
  
  # Return as a named list
  list(
    location_name = location_name,
    street_housenumber = street_housenumber,
    postcode_municipality = postcode_municipality
  )
}

# Function to extract the phone number and adjust the format
adjust_phone <- function(phone_number) {
  if (is.na(phone_number)) return(NA) # Return NA for missing values
  if (startsWith(phone_number, "0")) {
    phone_number <- sub("^0", "+32", phone_number) # Replace leading 0 with +32
  } else {
    phone_number <- paste0("+", phone_number) # Add + to other numbers
  }
  return(phone_number)
}

# Function to extract and clean the phone number
extract_phone <- function(address_block) {
  # Remove `/`, `.`, and spaces
  cleaned_string <- str_replace_all(address_block, "[/\\.\\s]", "")
  
  # Extract any sequence of at least 8 digits
  phone_match <- str_extract(cleaned_string, "\\d{8,}")
  
  # Adjust phone number format if found
  if (!is.na(phone_match)) {
    return(adjust_phone(phone_match))
  } else {
    return(NA)
  }
}

# Function to parse the HTML text
parse_html_text <- function(html_text) {
  # Extract the div class value for "type"
  parsed_html <- read_html(html_text)
  type <- parsed_html %>% html_node("div.gmap-popup > div") %>% html_attr("class") %>%
    str_remove_all("term_")
  
  # Extract title
  title <- parsed_html %>% html_node("h3") %>% html_text(trim = TRUE)
  
  # Extract the first <p><p> block
  address_block <- extract_first_p(html_text)
  
  # If address block is found, proceed to extract address fields
  fields <- if (!is.na(address_block)) extract_fields(address_block) else list(
    location_name = NA,
    street_housenumber = NA,
    postcode_municipality = NA
  )
  
  # Extract email address
  email <- parsed_html %>% html_node("a[href^='mailto']") %>% html_attr("href") %>% str_remove("mailto:")
  
  # Extract phone number
  phone <- extract_phone(address_block)
  
  # Extract other links (read more, external)
  read_more <- parsed_html %>% html_node("a.internal-link") %>% html_attr("href")
  external_link <- parsed_html %>% html_node("a.external-link") %>% html_attr("href")
  
  # Return the parsed fields as a named list
  list(
    type = type,
    title = title,
    email = email,
    phone = phone,
    read_more = read_more,
    external_link = external_link,
    location_name = fields$location_name,
    street_housenumber = fields$street_housenumber,
    postcode_municipality = fields$postcode_municipality
  )
}

# Apply parsing to the dataset (markers_df assumed)
parsed_data <- markers_df %>%
  mutate(parsed = map(text, parse_html_text)) %>%
  unnest_wider(parsed)

# if external link contains an @, set as NA
parsed_data$external_link <- ifelse(str_detect(parsed_data$external_link, "@"), NA, parsed_data$external_link)



# SIMPLIFY THE DATA ----
parsed_data <- parsed_data %>%
  select(latitude, longitude, type, title, email, phone, read_more, external_link, location_name, street_housenumber, postcode_municipality)

# turn into SF dataset
parsed_data <- st_as_sf(parsed_data, coords = c("longitude", "latitude"), crs = 4326)


# GEOCODE ----

parsed_data <- parsed_data %>% mutate(ad_hoc_id = row_number())
data_input_geocode <- as.data.frame(parsed_data) %>% select(ad_hoc_id, street_housenumber, postcode_municipality)


data_geocoded <- phaco_geocode(data_to_geocode=t_adresse <- data_input_geocode, colonne_num_rue= "street_housenumber",colonne_code_postal="postcode_municipality")

simple_geocode <- data_geocoded$data_geocoded_sf[, c("ad_hoc_id")]

simple_geocode <- as.data.frame(simple_geocode) %>%
  rename(geometry_geocoded = geometry)

# transform parsed data to lambert72
parsed_data <- st_transform(parsed_data, crs = 31370)
data_merged <- left_join(parsed_data, simple_geocode, by = "ad_hoc_id")

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
data_merged <- st_transform(data_merged, crs = 4326)

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
    CONSTRAINT asylum_reception_centres_pkey PRIMARY KEY (id)
  );",paste0("
  WITH cleaned as (
    SELECT
    read_more as original_id,
    jsonb_strip_nulls(jsonb_build_object(
      'und', CASE 
      WHEN location_name='Aanmeldcentrum' OR COUNT(*) OVER (PARTITION BY location_name) > 1 THEN location_name || ' ' || title
      ELSE location_name END)) AS name,
    jsonb_build_object(
      'dut', 'opvangcentrum voor asielzoekers',
      'fre', 'centre d''accueil pour demandeurs d''asile',
      'eng', 'reception centre for asylum seekers')
    as legend_item,
    JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
      'email', email,
      'phone', phone,
      'operator_website', 'https://www.fedasil.be' || read_more,
      'website', external_link,
      'address', LTRIM(CONCAT(REPLACE(street_housenumber,',',''),', ',street_housenumber)))) as properties,
    2 as risk_level,
    geometry
    FROM
    raw_data.fedasil_reception_parsed)
  
  INSERT INTO ingestion.asylum_reception_centres 
  (original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
  SELECT
  original_id,
  name,
  legend_item,
  '",data_list_id,"' as data_list_id,
  risk_level,
  properties,
  geometry,
  CURRENT_DATE as created_at
  FROM cleaned;
"))
                         
### Create transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.asylum_reception_centres CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.asylum_reception_centres
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
    CONSTRAINT asylum_reception_centres_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.asylum_reception_centres
(id, original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id::uuid, properties, geometry, created_at FROM ingestion.asylum_reception_centres;
","
ALTER TABLE IF EXISTS transformation.asylum_reception_centres
OWNER to pgn_group_data_team_w;")




### Execute the SQL commands ----


create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}
create_fdw_views <- function() {execute_sql_commands(fdw_views_sql, "FDW view")}




# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("asylum_reception_centres", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = data_merged, schema = "raw_data", table_name = "fedasil_reception_parsed")
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
}

if(F){
  main_function()
}