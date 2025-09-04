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



# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# External IDs
data_list_id_soka<-"94f691c4-542d-4d90-be7d-ff7136779660"
data_list_id_ostbelgien<-"c5321bf8-4772-4ec5-9f65-a462382b6892"

legend_item_nursery_afterschool <- "cbc03961-798f-46ee-aaf5-2f4f4fc799cf"
legend_item_nursery_preschool <- "1d1db4eb-17e2-485a-9def-9f5df6efa63d"

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

# Set location for files received by email
offline_storage <-Sys.getenv("OFFLINE_STORAGE")
data_folder <- paste0(offline_storage,"/nursery/")
ostbelgien_data <- paste0(data_folder,"2025 02 Versand Kontaktangaben nationales Krisenzentrum cleaned.xlsx")


# Set log folder
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))

# Extra libraries -------------------------------
# """""""""""""""""" ----------------------

library(rvest)
library(readxl)

## Geocoding libraries
library(devtools)
library(phacochr)


# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    # Only load phaco data if we're actually going to use it
    phaco_setup_data()
    phacochr::phaco_best_data_update()

# 1. Sociale Kaart (SOKA) data ----

# SOKA Loop through pages and content ------------------------------------------

# function that hardcodes the page size
## max allowed: 2000. Use either very high or very low (default=20) value
## checks new page records against already known records
## should give a warning if the number of records retrieved is not the same as the expected number

getPagesAndContent <- function(query_url) {
  page_size <- 20
  response <- GET(url = query_url)
  json_val <- content(response, as = "parsed")
  total_pages <- json_val$page$totalPages
  total_elements <<- json_val$page$totalElements
  print(paste0("total pages: ", total_pages))
  print(paste0("total elements: ", total_elements))
  
  df <- data.frame()
  all_ids <- c()  # To keep track of unique identifiers
  
  for (page in 1:total_pages) {
    print(paste("Fetching page", page))
    
    # Pagenumber is zero-based. totalPages is one-based
    page_url = page - 1
    next_page <- paste0(query_url, "&page=", page_url, "&size=", page_size, "&sort=LAST_UPDATED_DESC")
    print(next_page)  # Debug: Print the URL being requested
    
    response <- GET(url = next_page)
    content <- fromJSON(rawToChar(response$content))$content
    content <- jsonlite::flatten(content, recursive = TRUE)
    
    # Assuming each record has a unique 'id' field
    if ("id" %in% colnames(content)) {
      new_ids <- content$id
      content <- content[!content$id %in% all_ids, ]  # Remove duplicates
      all_ids <- c(all_ids, new_ids)
    }
    
    df <- rbind(df, content)
    Sys.sleep(0.5)
    if (page %% 10 == 0) {
      Sys.sleep(5)
    }
    if (page %% 50 == 0) {
      Sys.sleep(10)
    }
  }
  
  # Remove duplicates based on all columns
  df <- unique(df)
  if (nrow(df) != total_elements) {
    warning("Number of unique records does not match totalElements")
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
df_tmp <- transformTable(df = df_tmp)
df_tmp <- unique(df_tmp)
if (nrow(df_tmp) != total_elements) {
  stop("The server did not return all the records it said it has for Gezinsopvang")
}
gezinsopvang <- df_tmp
gezinsopvang$type<- "Gezinsopvang voor kinderen"

df_tmp <- NA
url <- "https://www.desocialekaart.be/api/health-offers?rubrics=10.07.02.%20Groepsopvang%20voor%20kinderen"
df_tmp <- getPagesAndContent(query_url = url)
df_tmp <- transformTable(df = df_tmp)
df_tmp <- unique(df_tmp)
if (nrow(df_tmp) != total_elements) {
  stop("The server did not return all the records it said it has for Groepsopvang")
}
groepsopvang <- df_tmp
groepsopvang$type<- "Groepsopvang voor kinderen"


df_tmp <- NA
url <- "https://www.desocialekaart.be/api/health-offers?rubrics=10.07.04.%20Opvang%20schoolgaande%20kinderen%20(schooljaar%20en%20vakantie)"
df_tmp <- getPagesAndContent(query_url = url)
naschools <- transformTable(df = df_tmp)
naschools$type<- "Opvang schoolgaande kinderen (schooljaar en vakantie)"

# 2. Ostbelgien data ----

# Load Excel with Ostbelgien data
ostbelgien_andere <- read_excel(ostbelgien_data)
# Load second tab ZKB from the same file. Column Trager was manually added there
ostbelgien_zkb <- read_excel(ostbelgien_data, sheet = "ZKB")

# Merge both tables
ostbelgien <- rbind(ostbelgien_andere, ostbelgien_zkb)

# Add an identifier (just the row number)
ostbelgien$case_number <- seq_len(nrow(ostbelgien))

# Geocode the data
ostbelgien_sel <- ostbelgien %>% select (case_number, Straße, PLZ)
ostbelgien_geocoded <- phaco_geocode(data_to_geocode=t_adresse <- ostbelgien_sel, colonne_num_rue="Straße", colonne_code_postal="PLZ")

## change geometry column name and add results to all records
simple_geocode_ob <- ostbelgien_geocoded$data_geocoded_sf[, c("case_number")]
ostbelgien <- left_join(ostbelgien, simple_geocode_ob, by = "case_number")

# select the records not geocoded
ostbelgien_not_geocoded <- ostbelgien %>% filter(st_is_empty(geometry))

ostbelgien <- ostbelgien %>% filter(!st_is_empty(geometry))

#make sure it is SF dataframe
ostbelgien <- st_as_sf(ostbelgien)

# Make available outside the function for upload to raw data
ostbelgien<<-ostbelgien
#table(ostbelgien$"Form der Kinderbetreuung")




# Transform ----
# """""""""""""""""" ----

# Process SOKA data -----------------------------------------------------------

# Make the data available outside of the function for upload to raw data
gezinsopvang<<-gezinsopvang
groepsopvang<<-groepsopvang
naschools<<-naschools

kinderopvang <- rbind(gezinsopvang, groepsopvang,naschools)
kinderopvang$case_number <- seq_len(nrow(kinderopvang))



# Extract address data ----

# Function to filter addresses where primary == TRUE and keep only the first record
filter_primary_addresses <- function(json_data, case_number) {
  if (is.data.frame(json_data)) {
    filtered_data <- json_data[json_data$primary == TRUE, ]
    
    if (nrow(filtered_data) > 0) {
      filtered_data <- filtered_data[1, , drop = FALSE]
      filtered_data$case_number <- case_number
      return(filtered_data)
    }
  }
  return(NULL)
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






# Parse phone data ----

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


# Add email data ----

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



# Add website data ----
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


# add geocoding ----
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
##this was used during the building of the process to help find cases for manual inspection, in order to choose to trust the original or geocoded geometry as a rule
#kinderopvang_sf <- kinderopvang_sf %>%
#  filter(!is.na(geometry) & !is.na(original_geometry)) %>%
#  mutate(
#    distance_m = st_distance(
#      st_transform(geometry, 31370),        # Transform 'geometry' to Belgian Lambert 72
#      st_transform(original_geometry, 31370), # Transform 'original_geometry' to Belgian Lambert 72
#      by_element = TRUE
#    )
#  )




# overrule geometry with original_geometry if geometry is empty
kinderopvang_sf <- kinderopvang_sf %>%
  mutate(new_geometry = ifelse(st_is_empty(geometry), original_geometry, geometry))

# remove unneeded geometry columns
kinderopvang_sf<-as.data.frame(kinderopvang_sf) %>%
  select(-geometry, -original_geometry) %>%
  rename(geometry = new_geometry)

# set the list of coordinates at geometry_def as geometry
kinderopvang_sf <- kinderopvang_sf %>%
  st_set_geometry("geometry")


# keep only cases with a non-empty geometry
kinderopvang_sf <- kinderopvang_sf %>%
  filter(!is.na(geometry) & !st_is_empty(geometry))





# add data list id
kinderopvang_sf <- kinderopvang_sf %>%
  mutate(data_list_id = data_list_id_soka)



# Extract KBO number & adres id ----


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
      
      
      # commented out because a syntax error is reported by R
      #addresses$links <- map(addresses$links, ~ if (is.null(.x)) data.frame(href = NA_character_) else .x)
      addresses$links <- map(addresses$links, ~ if (is.null(.x)) {
        # Create a data frame with the same structure as non-NULL elements
        data.frame(href = NA_character_, stringsAsFactors = FALSE) 
      } else {
        .x
      })
      
      addresses <- addresses %>%
        unnest(links) %>%
        mutate(href = ifelse(is.null(href), NA_character_, href)) %>%
        select(type, href)
      
      # Combine results into a single data frame
      result <- data.frame(
        id = id,
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

# get a list of ids in kinderopvang_sf that are not in kinderopvang_extra
ids_not_in_kinderopvang_extra <- kinderopvang_sf %>%
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

# remove geometry
kinderopvang_extra <- as.data.frame(kinderopvang_extra) %>% select(-geometry)

# add data to kinderopvang
kinderopvang_kbo <- kinderopvang_sf %>%
  left_join(kinderopvang_extra, by = c("original_id"="id"))

kinderopvang_kbo <- kinderopvang_kbo %>%  
  rename(kbo_bce = CBE_ID) %>%  
  rename(bestad_id = href)




# Deal with spatial duplicates ----

# Group by coordinates and calculate the first ID and count per location
kinderopvang_kbo <- kinderopvang_kbo %>%
  # Extract coordinates for grouping
  mutate(lon = st_coordinates(.)[,1], lat = st_coordinates(.)[,2]) %>%
  group_by(lon, lat) %>%
  mutate(
    first_original_id = first(original_id),  # Assign the first original_id for this location
    count = n()  # Count occurrences at this location
  ) %>%
  ungroup() %>%
  # Optionally, drop the lon and lat helper columns if you don’t need them
  select(-lon, -lat)

# Make available outside the function for upload to raw data
kinderopvang_kbo <<- kinderopvang_kbo

# Aggregate by location and keep the first original_id
kinderopvang_summarized <- kinderopvang_kbo %>%
  group_by(first_original_id) %>%
  summarize(
    original_id = paste(na.omit(unique(original_id)), collapse = '; '),
    name = paste(na.omit(unique(name)), collapse = '; '),
    type = paste(na.omit(unique(type)), collapse = '; '),
    streetname = paste(na.omit(unique(streetname)), collapse = '; '),
    housenumber = paste(na.omit(unique(housenumber)), collapse = '; '),
    postalcode = paste(na.omit(unique(postalcode)), collapse = '; '),
    city = paste(na.omit(unique(city)), collapse = '; '),
    phone = paste(na.omit(unique(phone)), collapse = '; '),
    email = paste(na.omit(unique(email)), collapse = '; '),
    website = paste(na.omit(unique(website)), collapse = '; '),
    data_list_id = first(data_list_id),
    kbo_bce = paste(na.omit(unique(kbo_bce)), collapse = '; '),
    bestad_id = paste(na.omit(unique(bestad_id)), collapse = '; '),
    geometry = first(geometry)
  )

# Make available outside the function for upload to raw data
kinderopvang_summarized <<- kinderopvang_summarized





  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function


# LOAD ----
# """""""" ----

  


### Create SQL for proper ingestion table ----



soka_ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.nursery_soka CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.nursery_soka
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
  CONSTRAINT nursery_soka_pkey PRIMARY KEY (id)
);",paste0("WITH 
cleaned as (SELECT original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN type
                          ELSE name END,
              'dut', name)) as name,
            CASE WHEN type='Opvang schoolgaande kinderen (schooljaar en vakantie)' THEN 
			jsonb_build_object(
              'dut', 'opvang schoolgaande kinderen',
              'fre', 'garde d''enfants pour les enfants scolarisés',
              'ger', 'Betreuung schulpflichtiger Kinder',
              'eng', 'childcare for school-going children') ELSE
			jsonb_build_object(
              'dut', 'kinderopvang',
              'fre', 'crèche',
              'ger', 'Kinderkrippe',
              'eng', 'nursery') END
              as legend_item,
            CASE 
				WHEN type='Opvang schoolgaande kinderen (schooljaar en vakantie)' THEN '",legend_item_nursery_afterschool,"'::uuid
				ELSE '",legend_item_nursery_preschool,"'::uuid END
              as legend_item_id,
		   CASE WHEN type='Opvang schoolgaande kinderen (schooljaar en vakantie)' THEN 2 ELSE 3 END as risk_level,
           CASE WHEN streetname IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(streetname, ' ' || housenumber, ', ' || postalcode, ' ' || city),', ') END
	AS address,
	phone,
	email,
	website,
	kbo_bce,
	bestad_id as best_address_id,
	data_list_id::uuid,
	geometry FROM raw_data.vla_depzorg_socialekaart_kinderopvang_summ
	WHERE ST_IsValid(geometry) AND NOT ST_IsEmpty(geometry))
INSERT INTO ingestion.nursery_soka 
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
  'phone', phone,
  'email', email,
  'website', website,
  'kbo_bce', kbo_bce,
  'best_address_id', best_address_id
)) as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),"
ALTER TABLE IF EXISTS ingestion.nursery_soka OWNER to pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.nursery_soka TO pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.nursery_soka TO pgn_user_airflow;")



ostbelgien_ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.nursery_ostbelgien CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.nursery_ostbelgien (
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
  CONSTRAINT nursery_ostbelgien_pkey PRIMARY KEY (id)
);",paste0("
WITH cleaned as (
  select
  CASE WHEN \"ID\" IS NULL THEN 'none provided'
  ELSE \"ID\"::text END as original_id,
  jsonb_strip_nulls(jsonb_build_object(
    'und', CASE WHEN \"Name\" IS NULL THEN \"Form der Kinderbetreuung\"
    WHEN \"Form der Kinderbetreuung\" <> 'AUBE' THEN \"Form der Kinderbetreuung\" || ' ' || \"Name\"                          
    ELSE \"Name\" END,
    'ger', CASE WHEN \"Name\" IS NULL THEN \"Form der Kinderbetreuung\"
    WHEN \"Form der Kinderbetreuung\" <> 'AUBE' THEN \"Form der Kinderbetreuung\" || ' ' || \"Name\"                          
    ELSE \"Name\" END)) as name,
  jsonb_build_object(
    'dut', 'kinderopvang',
    'fre', 'crèche',
    'ger', 'Kinderkrippe',
    'eng', 'nursery') 
  as legend_item,
  '",legend_item_nursery_preschool,"'::uuid as legend_item_id,
  '",data_list_id_ostbelgien,"'::uuid as data_list_id,
  3 as risk_level,
  jsonb_strip_nulls(jsonb_build_object(	
    'address', CONCAT(\"Straße\" || ', ' || \"PLZ\",  ' ' || \"Gemeinde\"),
    'phone', \"Telefonnummer\",
    'mobile', \"Handynummer\",
    'email', \"E-Mail-Adresse\",
    'capacity', \"Kapazität\",
    'occupancy', \"Anzahl Kinder\",
    'age_groups', \"Alter\",
    'personnel', \"Anzahl Erwachsene\",
    'opening_hours', CASE WHEN \"Öffnungszeiten\" <> '/' THEN \"Öffnungszeiten\" ELSE NULL END)) as properties,
  st_transform(geometry,4326) as geometry
  from
  raw_data.ostbelgien_childcare)
INSERT INTO ingestion.nursery_ostbelgien 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
*,
CURRENT_DATE as created_at
FROM cleaned;"),"
ALTER TABLE IF EXISTS ingestion.nursery_ostbelgien OWNER to pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.nursery_ostbelgien TO pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.nursery_ostbelgien TO pgn_user_airflow;")

### Execute the SQL commands ----

create_ingestion_table_soka <- function() {execute_sql_commands(soka_ingestion_table_sql, "SOKA ingestion table made")}
create_ingestion_table_ostbelgien <- function() {execute_sql_commands(ostbelgien_ingestion_table_sql, "Ostbelgien ingestion table made")}


### Create SQL for final ingestion table ----

sql_merge <- c(
  "DROP TABLE IF EXISTS ingestion.nursery CASCADE;",
  "CREATE TABLE IF NOT EXISTS ingestion.nursery
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
  CONSTRAINT nursery_pkey PRIMARY KEY (id)
);",
  "  
WITH alldata as (SELECT * FROM ingestion.nursery_ostbelgien
UNION ALL
SELECT * FROM ingestion.nursery_soka)
INSERT INTO ingestion.nursery (id, original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, properties_secondary, imported_at, tags, deleted_at, updated_at, created_at, created_by, updated_by, geometry)
select * from alldata
WHERE geometry IS NOT NULL AND NOT ST_IsEmpty(geometry);","
ALTER TABLE IF EXISTS ingestion.nursery OWNER to pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.nursery TO pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.nursery TO pgn_user_airflow;")

create_ingestion_table_merged <- function() {execute_sql_commands(sql_merge, "Merged ingestion data")}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----


run_smart_update = function() {
  smart_update_process("nursery", 50, 40, 40, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = gezinsopvang, schema = "raw_data", table_name = "vla_depzorg_socialekaart_gezinsopvang") 
    CreateImportTable(dataset = groepsopvang, schema = "raw_data", table_name = "vla_depzorg_socialekaart_groepsopvang")
    CreateImportTable(dataset = kinderopvang_kbo, schema = "raw_data", table_name = "vla_depzorg_socialekaart_kinderopvang")
    CreateImportTable(dataset = kinderopvang_summarized, schema = "raw_data", table_name = "vla_depzorg_socialekaart_kinderopvang_summ")
    CreateImportTable(dataset = ostbelgien, schema = "raw_data", table_name = "ostbelgien_childcare")
    create_ingestion_table_soka()
    create_ingestion_table_ostbelgien()
    create_ingestion_table_merged()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}
