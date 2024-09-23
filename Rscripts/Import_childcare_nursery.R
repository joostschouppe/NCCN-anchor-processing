## ---------------------------
##
## Script name: Import nursery
##
## Purpose of script: Load nursery data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-08-01
##
##
## ---------------------------


# TODO
# standaard structuur
# legend item: just "nursery" or with more categories
# uitbreiden met KBO nummer & adres ID https://www.desocialekaart.be/api/leaflet/07062e240127d14887de332f3851859a9a8269766694ca3a9da49c5bd28101a9?includeHiddenData=false
# risk level: just 3, or differentiate between groepsopvang & gezinsopvang


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

data_list_id_soka<-"94f691c4-542d-4d90-be7d-ff7136779660"
log_folder <- "C:/temp/logs/"

### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))

# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(sf)
library(purrr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(rvest)
library(DBI)
library(RPostgres)
library(httr)

## Geocoding libraries
library(devtools)
library(phacochr)
phaco_setup_data()
phacochr::phaco_best_data_update()

# EXTRACT ----
# """""""""""""""""" ----

# SOKA Loop through pages and content ------------------------------------------

getPagesAndContent<-function(query_url){
  
  response <- GET(url = query_url)
  json_val <- content(response)
  total_pages <- json_val$page$totalPages
  
  df <- data.frame()
  for (page in 1:total_pages){
    
    print(page)
    
    # pagenumber is zero-based. totalPages is one-based
    page_url = page - 1
    next_page <- paste0(query_url, "&page=", page_url)
    response <- GET(url = next_page)
    
    content <- fromJSON(rawToChar(response$content))$content 
    content <- jsonlite::flatten(content, recursive = TRUE)
    
    df <- rbind(df, content)
    Sys.sleep(0.5)
    if (page %% 10 == 0) {
      Sys.sleep(5)
    }
    if (page %% 50 == 0) {
      Sys.sleep(10)
    }
    
  }
  return(df)
  
}

# SOKA Define columns & unnest json --------------------------------------------

transformTable<-function(df){
  
  columns <- c(
    "id"="id.id",
    "authentic_source"="id.authenticSource",
    "legal_name"="legalName.description",
    "addresses"="addresses",
    "activities"="activities",
    "type"="type",
    "state"="state",
    "verified"="verified",
    "contactinfo_phones"="contactInfo.phones",
    "contactinfo_emails"="contactInfo.emails",
    "contactinfo_websites"="contactInfo.websites",
    "links"="links"
  )
  
  raw_data <- df %>%
    rowwise %>%
    mutate(addresses = toJSON(addresses, auto_unbox = TRUE)) %>%
    mutate(activities = toJSON(activities, auto_unbox = TRUE)) %>%
    mutate(contactInfo.phones = toJSON(contactInfo.phones, auto_unbox = TRUE)) %>%
    mutate(contactInfo.emails = toJSON(contactInfo.emails, auto_unbox = TRUE)) %>%
    mutate(contactInfo.websites = toJSON(contactInfo.websites, auto_unbox = TRUE)) %>%
    mutate(links = toJSON(links, auto_unbox = TRUE)) %>%
    select_at(columns)
  
  return(raw_data)
}

# Download SOKA data -----------------------------------------------------------

df_tmp <- NA
url <- "https://www.desocialekaart.be/api/health-offers?rubrics=10.07.03.%20Gezinsopvang%20voor%20kinderen"
df_tmp <- getPagesAndContent(query_url = url)
gezinsopvang <- transformTable(df = df_tmp)
gezinsopvang$type<- "Gezinsopvang voor kinderen"

df_tmp <- NA
url <- "https://www.desocialekaart.be/api/health-offers?rubrics=10.07.02.%20Groepsopvang%20voor%20kinderen"
df_tmp <- getPagesAndContent(query_url = url)
groepsopvang <- transformTable(df = df_tmp)
groepsopvang$type<- "Groepsopvang voor kinderen"

# Transform ----
# """""""""""""""""" ----

# Process SOKA data -----------------------------------------------------------

kinderopvang <- rbind(gezinsopvang, groepsopvang)
kinderopvang$case_number <- seq_len(nrow(kinderopvang))



# Extract address data

# Function to filter addresses where primary == TRUE and keep only the first record
filter_primary_addresses <- function(json_data, case_number) {
  if (is.data.frame(json_data)) {
    # Filter addresses based on primary attribute
    filtered_data <- json_data[json_data$primary == TRUE, ]
    
    # Keep only the first record if there are multiple
    if (nrow(filtered_data) > 0) {
      filtered_data <- filtered_data[1, , drop = FALSE]
    }
    
    # Add case number as a new column
    filtered_data$case_number <- case_number
    
    return(filtered_data)
  }
  return(NULL)  # Return NULL if json_data is not a data frame
}


parsed_filtered_data_list <- lapply(seq_along(kinderopvang$addresses), function(i) {
  json_str <- kinderopvang$addresses[i]
  json_data <- fromJSON(json_str)
  filter_primary_addresses(json_data, case_number = i)
})



# Combine all filtered data frames into a single data frame
final_data_frame <- do.call(bind_rows, parsed_filtered_data_list)

# Unnest nested columns with disambiguation
final_data_frame <- final_data_frame %>%
  unnest(
    cols = c(street, municipality, location),
    names_sep = "_"
  )
final_data_frame <- final_data_frame %>%
  unnest(
    cols = c(street_name, municipality_name),
    names_sep = "_"
  )

# Select and rename columns
final_data_frame <- final_data_frame %>%
  select(
    case_number,
    streetname = street_name_description,
    housenumber = number,
    postalcode = municipality_postalCode,
    city = municipality_name_description,
    lat = location_lat,
    lon = location_lon
  )


# Add to kinderopvang with case_number
kinderopvang <- kinderopvang %>%
  left_join(final_data_frame, by = "case_number")






# Parse phone data

# Function to extract contactinfo_phones and retain the case number

# Parse all records and extract contactinfo_phones
parsed_contactinfo_phones_list <- lapply(seq_along(kinderopvang$contactinfo_phones), function(i) {
  json_str <- kinderopvang$contactinfo_phones[i]
  json_data <- fromJSON(json_str)
  
  # Add case number to the parsed data
  json_data$case_number <- i
  
  return(json_data)
})


# Combine all contactinfo_phones data frames into a single data frame
final_contactinfo_phones_df <- do.call(bind_rows, parsed_contactinfo_phones_list)

# keep a single record for each case_number, with distinct "value" values concatenated with a ;
final_contactinfo_phones_df <- final_contactinfo_phones_df %>%
  group_by(case_number) %>%
  summarise(phone = paste(value, collapse = "; "))

# Add to kinderopvang using case_number
kinderopvang <- kinderopvang %>%
  left_join(final_contactinfo_phones_df, by = "case_number")


# Add email data

# Parse all records and extract contactinfo_emails
parsed_contactinfo_emails_list <- lapply(seq_along(kinderopvang$contactinfo_emails), function(i) {
  json_str <- kinderopvang$contactinfo_emails[i]
  json_data <- fromJSON(json_str)
  
  # Add case number to the parsed data
  json_data$case_number <- i
  
  return(json_data)
})

# Combine all contactinfo_emails data frames into a single data frame
final_contactinfo_emails_df <- do.call(bind_rows, parsed_contactinfo_emails_list)

# select only primary
final_contactinfo_emails_df <- final_contactinfo_emails_df %>%
  filter(primary == TRUE) %>%
  select(case_number, email = value)

# select only the first record for each case_number
final_contactinfo_emails_df <- final_contactinfo_emails_df %>%
  group_by(case_number) %>%
  slice(1)

# Add to kinderopvang using case_number
kinderopvang <- kinderopvang %>%
  left_join(final_contactinfo_emails_df, by = "case_number")



# Add website data
# Parse all records and extract contactinfo_websites
parsed_contactinfo_websites_list <- lapply(seq_along(kinderopvang$contactinfo_websites), function(i) {
  json_str <- kinderopvang$contactinfo_websites[i]
  json_data <- fromJSON(json_str)
  
  # Add case number to the parsed data
  json_data$case_number <- i
  
  return(json_data)
})
# Combine all contactinfo_websites data frames into a single data frame
final_contactinfo_websites_df <- do.call(bind_rows, parsed_contactinfo_websites_list)

# select only primary
final_contactinfo_websites_df <- final_contactinfo_websites_df %>%
  filter(primary == TRUE) %>%
  select(case_number, website = value)

# select only the first record for each case_number
final_contactinfo_websites_df <- final_contactinfo_websites_df %>%
  group_by(case_number) %>%
  slice(1)



# Add to kinderopvang using case_number
kinderopvang <- kinderopvang %>%
  left_join(final_contactinfo_websites_df, by = "case_number")

# if the website is not NA and does not start with http, add https://
kinderopvang$website <- ifelse(!is.na(kinderopvang$website) & !grepl("^http", kinderopvang$website), paste0("https://", kinderopvang$website), kinderopvang$website)

# keep interesting variables only
kinderopvang <- kinderopvang %>%
  select(original_id=id, name=legal_name, type, streetname, housenumber, postalcode, city, lat, lon, phone, email, website)

kinderopvang <- as.data.frame(kinderopvang)

# select only the first time any original_id appears
kinderopvang <- kinderopvang %>%
  distinct(original_id, .keep_all = TRUE)

# add geocoding


## geocode
kinderopvang_sel <- kinderopvang %>% select (original_id, streetname, housenumber, postalcode)
kinderopvang_geocoded <- phaco_geocode(data_to_geocode=t_adresse <- kinderopvang_sel, colonne_rue="streetname", colonne_num="housenumber", colonne_code_postal="postalcode")

## change geometry column name and add results to all records
simple_geocode <- kinderopvang_geocoded$data_geocoded_sf[, c("original_id")]
kinderopvang <- left_join(kinderopvang, simple_geocode, by = "original_id")

# Convert to sf object
kinderopvang_sf <- st_set_geometry(kinderopvang, "geometry")
# transform to WGS84
kinderopvang_sf <- st_transform(kinderopvang_sf, crs = 4326)

# Add original geometry
kinderopvang_sf <- kinderopvang_sf %>%
  mutate(original_geometry = st_sfc(
    map2(lon, lat, ~ st_point(c(.x, .y))),
    crs = 4326
  ))

# calculate distance
kinderopvang_sf <- kinderopvang_sf %>%
  filter(!is.na(geometry) & !is.na(original_geometry)) %>%
  mutate(
    distance_m = st_distance(
      st_transform(geometry, 31370),        # Transform 'geometry' to Belgian Lambert 72
      st_transform(original_geometry, 31370), # Transform 'original_geometry' to Belgian Lambert 72
      by_element = TRUE
    )
  )




# overrule geometry with original_geometry if geometry is null
kinderopvang_sf <- kinderopvang_sf %>%
  mutate(new_geometry = ifelse(is.na(geometry), st_as_sfc(original_geometry), geometry))

# keep only cases with a non-empty geometry
kinderopvang_sf <- kinderopvang_sf %>%
  filter(!is.na(new_geometry))

kinderopvang_sf<-as.data.frame(kinderopvang_sf) %>%
  select(-geometry, -original_geometry) %>%
  rename(geometry = new_geometry)

# set the list of coordinates at geometry_def as geometry
kinderopvang_sf <- kinderopvang_sf %>%
  st_set_geometry("geometry")


# add data list id
kinderopvang_sf <- kinderopvang_sf %>%
  mutate(data_list_id = data_list_id_soka)



# Extract KBO number & adres id


# Function to fetch and process data for a single ID with retry logic
process_id <- function(id, max_retries = 3) {
  url <- paste0("https://www.desocialekaart.be/api/leaflet/", id, "?includeHiddenData=false")
  
  attempt <- 1
  response <- NULL
  content <- NULL
  
  while (attempt <= max_retries) {
    try({
      response <- GET(url)
      content <- content(response, as = "text")
      parsed_data <- fromJSON(content, flatten = TRUE)
      
      # Extract the id
      id_value <- parsed_data$id$id
      
      # Extract the value for CBE_ID from administrativeData
      cbe_id_value <- parsed_data$administrativeData$value[parsed_data$administrativeData$type == "CBE_ID"]
      
      # Extract addresses and keep only type and href from links
      addresses <- parsed_data$addresses
      
      if (!"type" %in% names(addresses)) {
        addresses$type <- NA_character_
      }
      
      if (!"links" %in% names(addresses)) {
        addresses$links <- list(data.frame(href = NA_character_))
      }
      
      addresses$links <- map(addresses$links, ~ if (is.null(.x)) data.frame(href = NA_character_) else .x)
      
      addresses <- addresses %>%
        unnest(links) %>%
        mutate(href = ifelse(is.null(href), NA_character_, href)) %>%
        select(type, href)
      
      # Combine results into a single data frame
      result <- data.frame(
        id = id_value,
        CBE_ID = cbe_id_value
      )
      
      result <- cbind(result, addresses)
      return(result)
      
    }, silent = TRUE)
    
    # Handle errors and retry
    if (is.null(response) || http_status(response)$category != "Success") {
      message(sprintf("Attempt %d failed for ID %s. Waiting 10 seconds before retrying...", attempt, id))
      Sys.sleep(10)
      attempt <- attempt + 1
    } else {
      break
    }
  }
  
  # Skip if still failing after max retries
  if (attempt > max_retries) {
    message(sprintf("Failed to fetch data for ID %s after %d attempts. Skipping...", id, max_retries))
    return(data.frame(id = id, CBE_ID = NA, type = NA, href = NA))
  }
}

# Create a list of relevant IDs
input_ids <- as.data.frame(kinderopvang_sf) %>%
  select(id = original_id)  #%>% slice(1:5)  # Adjust the range if you want to test

# Initialize an empty data frame to store results
kinderopvang_extra <- data.frame()

# Loop through each ID and process it
for (i in seq_along(input_ids$id)) {
  id <- input_ids$id[i]
  final_results <- bind_rows(kinderopvang_extra, process_id(id))
  kinderopvang_extra <- final_results
  if (i %% 30 == 0) {
    Sys.sleep(1)  # Longer delay every 30 requests
  }
  if (i %% 51 == 0) {
    Sys.sleep(5)  # Even longer delay every 51 requests
  }
  if (i %% 99 == 0) {
    Sys.sleep(10)  # Even longer delay every 99 requests
  }
  print(i)
}


# check for duplicates
kinderopvang_extra <- kinderopvang_extra %>% distinct()

# get a list of ids in kinderopvang that are not in kinderopvang_extra
ids_not_in_kinderopvang_extra <- kinderopvang %>%
  anti_join(kinderopvang_extra, by = c("original_id"="id")) %>%
  select(id=original_id)


# Initialize an empty data frame to store results
kinderopvang_extra2 <- data.frame()

# Loop through each ID and process it
for (i in seq_along(ids_not_in_kinderopvang_extra$id)) {
  final_results <- bind_rows(ids_not_in_kinderopvang_extra, process_id(id))
  kinderopvang_extra2 <- final_results
  if (i %% 30 == 0) {
    Sys.sleep(1)  # Longer delay every 10 requests
  }
  
  if (i %% 51 == 0) {
    Sys.sleep(5)  # Even longer delay every 50 requests
  }
  
  if (i %% 99 == 0) {
    Sys.sleep(10)  # Even longer delay every 50 requests
  }
  print(i)
}

# bind together
kinderopvang_extra <- bind_rows(kinderopvang_extra, kinderopvang_extra2)

# check for duplicates
kinderopvang_extra <- kinderopvang_extra %>% distinct()

# add a count for number of times an id exists
kinderopvang_extra <- kinderopvang_extra %>%
  group_by(id) %>%
  mutate(count = n())

# select if count = 1 or count = 2 and type="Bezoekadres"
kinderopvang_extra <- kinderopvang_extra %>%
  filter(count == 1 | (count == 2 & type == "Bezoekadres"))

# check again
kinderopvang_extra <- kinderopvang_extra %>%
  group_by(id) %>%
  mutate(count = n()) 
# print number of cases where count is not 1
print(paste0("number of cases with more than one row: ",nrow(kinderopvang_extra[kinderopvang_extra$count != 1,])))

kinderopvang_extra <- kinderopvang_extra %>% select(-type, -count)

# add data to kinderopvang
kinderopvang_kbo <- kinderopvang_sf %>%
  left_join(kinderopvang_extra, by = c("original_id"="id"))

kinderopvang_kbo <- kinderopvang_kbo %>%  
  rename(kbo_bce = CBE_ID) %>%  
  rename(bestad_id = href)


# LOAD ----
# """""""" ----



### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.nursery_soka CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.nursery_soka
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
  CONSTRAINT nursery_soka_pkey PRIMARY KEY (id)
);",paste0("WITH 
cleaned as (SELECT
            original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN type
                          ELSE type END,
              'dut', name)) as name,
            jsonb_build_object(
              'dut', 'kinderopvang',
              'fre', 'crèche',
              'ger', 'Kinderkrippe',
              'eng', 'nursery') 
              as legend_item,
           CASE WHEN streetname IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(streetname, ' ' || housenumber, ', ' || postalcode, ' ' || city),', ') END
	AS address,
	phone,
	email,
	website,
	kbo_bce,
	bestad_id as best_address_id,
	data_list_id::uuid,
	geometry FROM raw_data.vla_depzorg_socialekaart_kinderopvang)
INSERT INTO ingestion.nursery_soka 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
data_list_id,
3 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'address', address,
  'phone', phone,
  'email', email,
  'website', website,
  'kbo_bce', kbo_bce,
  'best_address_id', best_address_id
)) as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))
                         
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
                         
# Main function -----------------------------------------------------------
# """"""""""""""""""""----

CreateImportTable(dataset = gezinsopvang, schema = "raw_data", table_name = "vla_depzorg_socialekaart_gezinsopvang") 
CreateImportTable(dataset = groepsopvang, schema = "raw_data", table_name = "vla_depzorg_socialekaart_groepsopvang")
CreateImportTable(dataset = kinderopvang_kbo, schema = "raw_data", table_name = "vla_depzorg_socialekaart_kinderopvang")
create_ingestion_table()
