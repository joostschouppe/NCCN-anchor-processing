## ---------------------------
##
## Script name: Import institions for the handicapped
##
## Purpose of script: Load institions for the handicapped data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2025-09-12
##
##
## ---------------------------



# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# External IDs
data_list_id_soka_handicap <- "2f303312-a771-48e2-8a4f-e02db467e24f"
data_list_id_osm <- "c5d33627-db36-42b5-93d9-66ffd7a46922"
legend_item_day_care <- "5f1307cf-45c0-4404-beb7-ca80178085ad"
legend_item_group_home <- "3e0494c6-82d4-4e38-b618-eb930f916905"
legend_item_combo <- "e0bbc626-18fd-439b-b990-5fb178787f53"
legend_item_treatment <- "5346ce2a-b7ac-4327-8282-3772e44d50e1"
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
# 1. Sociale Kaart (SOKA) data ----

process_soka_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    # Only load phaco data if we're actually going to use it
    phaco_setup_data()
    phacochr::phaco_best_data_update()
    
    
    
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
    url <- "https://www.desocialekaart.be/api/health-offers?rubrics=13.08.%20Dagopvang%20personen%20met%20een%20handicap"
    df_tmp <- getPagesAndContent(query_url = url)
    df_tmp <- transformTable(df = df_tmp)
    df_tmp <- unique(df_tmp)
    if (nrow(df_tmp) != total_elements) {
      stop("The server did not return all the records it said it has for Gezinsopvang")
    }
    doh <- df_tmp
    doh$type<- "Dagopvang personen met een handicap"
    
    df_tmp <- NA
    url <- "https://www.desocialekaart.be/api/health-offers?rubrics=13.02.02.%20Observatie-%20en%20behandelcentra%20(OBC)"
    df_tmp <- getPagesAndContent(query_url = url)
    df_tmp <- transformTable(df = df_tmp)
    df_tmp <- unique(df_tmp)
    if (nrow(df_tmp) != total_elements) {
      stop("The server did not return all the records it said it has for Groepsopvang")
    }
    obc <- df_tmp
    obc$type<- "Observatie- en behandelcentra"
    
    df_tmp <- NA
    url <- "https://www.desocialekaart.be/api/health-offers?rubrics=13.09.%20Woonondersteuning%20personen%20met%20een%20handicap"
    df_tmp <- getPagesAndContent(query_url = url)
    df_tmp <- transformTable(df = df_tmp)
    df_tmp <- unique(df_tmp)
    if (nrow(df_tmp) != total_elements) {
      stop("The server did not return all the records it said it has for Groepsopvang")
    }
    wph <- df_tmp
    wph$type<- "Woonondersteuning personen met een handicap"
    
    
    
    CreateImportTable(dataset = doh, schema = "raw_data", table_name = "vla_depzorg_socialekaart_doh")
    CreateImportTable(dataset = obc, schema = "raw_data", table_name = "vla_depzorg_socialekaart_obc")
    CreateImportTable(dataset = wph, schema = "raw_data", table_name = "vla_depzorg_socialekaart_wph")
    
    
    
    
    # Transform ----
    # """""""""""""""""" ----
    
    # Process SOKA data -----------------------------------------------------------
    
    
    handicap_soka <- rbind(doh, obc, wph)
    handicap_soka$case_number <- seq_len(nrow(handicap_soka))
    
    
    
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
    
    
    parsed_filtered_data_list <- lapply(seq_along(handicap_soka$addresses), function(i) {
      json_str <- handicap_soka$addresses[i]
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
    handicap_soka <- handicap_soka %>%
      left_join(final_data_frame, by = "case_number")
    
    
    
    
    
    
    # Parse phone data ----
    
    # Function to extract contactinfo_phones and retain the case number
    
    # Parse all records and extract contactinfo_phones
    parsed_contactinfo_phones_list <- lapply(seq_along(handicap_soka$contactinfo_phones), function(i) {
      json_str <- handicap_soka$contactinfo_phones[i]
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
    
    # Add to main dataset using case_number
    handicap_soka <- handicap_soka %>%
      left_join(final_contactinfo_phones_df, by = "case_number")
    
    
    # Add email data ----
    
    # Parse all records and extract contactinfo_emails
    parsed_contactinfo_emails_list <- lapply(seq_along(handicap_soka$contactinfo_emails), function(i) {
      json_str <- handicap_soka$contactinfo_emails[i]
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
    handicap_soka <- handicap_soka %>%
      left_join(final_contactinfo_emails_df, by = "case_number")
    
    
    
    # Add website data ----
    # Parse all records and extract contactinfo_websites
    parsed_contactinfo_websites_list <- lapply(seq_along(handicap_soka$contactinfo_websites), function(i) {
      json_str <- handicap_soka$contactinfo_websites[i]
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
    handicap_soka <- handicap_soka %>%
      left_join(final_contactinfo_websites_df, by = "case_number")
    
    # if the website is not NA and does not start with http, add https://
    handicap_soka$website <- ifelse(!is.na(handicap_soka$website) & !grepl("^http", handicap_soka$website), paste0("https://", handicap_soka$website), handicap_soka$website)
    
    # keep interesting variables only
    handicap_soka <- handicap_soka %>%
      select(original_id=id, name=legal_name, type, streetname, housenumber, postalcode, city, lat, lon, phone, email, website)
    
    handicap_soka <- as.data.frame(handicap_soka)
    
    # select only the first time any original_id appears
    handicap_soka <- handicap_soka %>%
      distinct(original_id, .keep_all = TRUE)
    
    
    # add geocoding ----
    ## geocode
    geocode_sel <- handicap_soka %>% select (original_id, streetname, housenumber, postalcode)
    geocoded <- phaco_geocode(data_to_geocode=t_adresse <- geocode_sel, colonne_rue="streetname", colonne_num="housenumber", colonne_code_postal="postalcode")
    
    ## change geometry column name and add results to all records
    simple_geocode <- geocoded$data_geocoded_sf[, c("original_id")]
    handicap_soka <- left_join(handicap_soka, simple_geocode, by = "original_id")
    
    # Convert to sf object
    handicap_soka <- st_set_geometry(handicap_soka, "geometry")
    # transform to WGS84
    handicap_soka <- st_transform(handicap_soka, crs = 4326)
    
    # Add original geometry
    handicap_soka <- handicap_soka %>%
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
    handicap_soka <- handicap_soka %>%
      mutate(new_geometry = ifelse(st_is_empty(geometry), original_geometry, geometry))
    
    # remove unneeded geometry columns
    handicap_soka<-as.data.frame(handicap_soka) %>%
      select(-geometry, -original_geometry) %>%
      rename(geometry = new_geometry)
    
    # set the list of coordinates at geometry_def as geometry
    handicap_soka <- handicap_soka %>%
      st_set_geometry("geometry")
    
    
    # keep only cases with a non-empty geometry
    handicap_soka <- handicap_soka %>%
      filter(!is.na(geometry) & !st_is_empty(geometry))
    
    
    
    
    
    # add data list id
    handicap_soka <- handicap_soka %>%
      mutate(data_list_id = data_list_id_soka_handicap)
    
    
    
    # Extract KBO number & address id ----
    
    
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
    input_ids <- as.data.frame(handicap_soka) %>%
      select(id = original_id)  #%>% slice(1:5)  # Adjust the range if you want to test
    
    # Initialize an empty data frame to store results
    handicap_extra <- data.frame()
    
    # Loop through each ID and process it
    for (i in seq_along(input_ids$id)) {
      id <- input_ids$id[i]
      final_results <- bind_rows(handicap_extra, process_id(id))
      handicap_extra <- final_results
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
    handicap_extra <- handicap_extra %>% distinct()
    
    
    # add a count for number of times an id exists
    handicap_extra <- handicap_extra %>%
      group_by(id) %>%
      mutate(count = n())
    
    # select if count = 1 or count = 2 and type="Bezoekadres"
    handicap_extra <- handicap_extra %>%
      filter(count == 1 | (count == 2 & type == "Bezoekadres"))
    
    # select only the first record for each id
    handicap_extra <- handicap_extra %>%
      group_by(id) %>%
      slice_head(n = 1) %>%
      ungroup()
    
    handicap_extra <- handicap_extra %>% select(-type, -count)
    
    
    # add data to main dataset
    handicap_soka_kbo <- handicap_soka %>%
      left_join(handicap_extra, by = c("original_id"="id"))
    
    handicap_soka_kbo <- handicap_soka_kbo %>%  
      rename(kbo_bce = CBE_ID) %>%  
      rename(bestad_id = href)
    
    
    # Deal with spatial duplicates ----
    
    # Group by coordinates and calculate the first ID and count per location
    handicap_soka_kbo <- handicap_soka_kbo %>%
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
    
    
    # Aggregate by location and keep the first original_id
    handicap_soka_summarized <- handicap_soka_kbo %>%
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
    
    CreateImportTable(dataset = handicap_soka_kbo, schema = "raw_data", table_name = "vla_depzorg_socialekaart_handicap")
    CreateImportTable(dataset = handicap_soka_summarized, schema = "raw_data", table_name = "vla_depzorg_socialekaart_handicap_summ")
    
    
    table(handicap_soka_summarized$type)
    table(osm_filtered$social_facility)
    
  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function




# 2. Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

process_osm_data <- function(){
  
  # Define the list of features
  features_list <- list("social_facility:for")
  
  # Define extra tags to use as columns for properties
  extra_columns <- c("capacity","capacity:male","capacity:female", "social_facility", "social_facility:for")
  # Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
  datatypes <- c("points", "mpolygon")
  
  
  ### Actual OSM download & transformation ----
  osm_all <- run_process(
    download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
    paste0("OSM download & processing for ", paste(paste(names(features_list), unlist(features_list), sep = "="), collapse = ", "), collapse = ", ")
  )
  
  # the object should be for disabled people (grepl)
  # the object should be of social_facility type assisted_living, nursing_home, day_care or group_home (grepl)
  # the object should have a name
  # the object should not be in Flanders
  osm_filtered <- osm_all %>%
    filter(
      grepl("disabled", social_facility_for, ignore.case = TRUE) &
        grepl("assisted_living|nursing_home|day_care|group_home", social_facility, ignore.case = TRUE) &
        !is.na(name) &
        (language != "dut" | is.na(language))
    )
  # add day-care and group home dummy
  osm_filtered <- osm_filtered %>%
    mutate(day_care = ifelse(grepl("day_care", social_facility, ignore.case = TRUE), TRUE, FALSE)) %>%
    mutate(group_home = ifelse(grepl("assisted_living|nursing_home|group_home", social_facility, ignore.case = TRUE), TRUE, FALSE))
  
  osm_filtered <- osm_filtered %>%
    mutate(data_list_id=data_list_id_osm)
  
  CreateImportTable(dataset = osm_filtered, schema = "raw_data", table_name = "osm_handicap")
  
  
} # end process_fresh_data function


# LOAD ----
# """""""" ----




### Create SQL for proper ingestion table ----



soka_ingestion_table_sql <- c(
  "DROP TABLE IF EXISTS ingestion.handicap_soka CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.handicap_soka
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
  CONSTRAINT nursing_home_soka_pkey PRIMARY KEY (id)
);",paste0("WITH 
cleaned as (SELECT original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN type
                          ELSE name END,
              'dut', name)) as name,
            CASE WHEN type='Woonondersteuning personen met een handicap' THEN '",legend_item_group_home,"'::uuid
				WHEN type='Observatie- en behandelcentra' then '",legend_item_treatment,"'::uuid
				WHEN type='Dagopvang personen met een handicap' then '",legend_item_day_care,"'::uuid
				ELSE '",legend_item_combo,"'::uuid END
              as legend_item_id,
		   CASE WHEN type='Observatie- en behandelcentra' or type='Dagopvang personen met een handicap' THEN 2 ELSE 3 END as risk_level,
           CASE WHEN streetname IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(streetname, ' ' || housenumber, ', ' || postalcode, ' ' || city),', ') END
	AS address,
	phone,
	email,
	website,
	kbo_bce,
	bestad_id as best_address_id,
	data_list_id::uuid,
	geometry FROM raw_data.vla_depzorg_socialekaart_handicap_summ
	WHERE ST_IsValid(geometry) AND NOT ST_IsEmpty(geometry))
INSERT INTO ingestion.handicap_soka 
(original_id, name, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
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
ALTER TABLE IF EXISTS ingestion.handicap_soka OWNER to pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.handicap_soka TO pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.handicap_soka TO pgn_user_airflow;")


  
osm_ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.osm_handicap CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.osm_handicap
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
  CONSTRAINT osm_handicap_pkey PRIMARY KEY (id)
);",paste0("WITH 
cleaned as (SELECT
            'https://osm.org/' || osm_id as original_id,
			jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN social_facility ELSE name END,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
            CASE WHEN day_care = TRUE and group_home = FALSE then '",legend_item_day_care,"'::uuid
			        WHEN day_care = FALSE and group_home = TRUE then '",legend_item_group_home,"'::uuid
        			WHEN day_care = TRUE and group_home = TRUE then '",legend_item_combo,"'::uuid END
              as legend_item_id,
			CASE WHEN group_home = TRUE then 3 else 2 END as risk_level,
			data_list_id::uuid as data_list_id,
           CASE WHEN short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL 
	ELSE CONCAT_WS('; ',short_name, official_name, alt_name, old_name) END AS other_names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || addr_postcode, ' ' || addr_city),', ') END
	AS address,
CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_email, email) END AS local_email,
operator_email,
CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS local_phone,
CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS local_website,
	operator_website,
			opening_hours,
			image,
			capacity,
			social_facility,
			social_facility_for,
            geometry
            FROM raw_data.osm_handicap)
INSERT INTO ingestion.osm_handicap 
(original_id, name, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item_id,
data_list_id,
risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'other_names', other_names,
  'address', address,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
	'social_facility',social_facility,
	'social_facility_for',social_facility_for)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
  "ALTER TABLE IF EXISTS ingestion.osm_handicap OWNER to pgn_group_data_team_w;",
  "GRANT ALL ON TABLE ingestion.osm_handicap TO pgn_group_data_team_w;",
  "GRANT ALL ON TABLE ingestion.osm_handicap TO pgn_user_airflow;")
  
  
        

### Execute the SQL commands ----

create_ingestion_table_soka <- function() {execute_sql_commands(soka_ingestion_table_sql, "SOKA ingestion table made")}
create_ingestion_table_osm <- function() {execute_sql_commands(osm_ingestion_table_sql, "OSM ingestion table made")}


### Create SQL for final ingestion table ----

sql_merge <- c(
  "DROP TABLE IF EXISTS ingestion.handicap_care CASCADE;",
  "CREATE TABLE IF NOT EXISTS ingestion.handicap_care
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
  CONSTRAINT handicap_care_pkey PRIMARY KEY (id)
);",
  "  
WITH alldata as (SELECT *
FROM ingestion.osm_handicap
UNION ALL
SELECT s.*
FROM ingestion.handicap_soka s
WHERE NOT EXISTS (
    SELECT 1
    FROM ingestion.osm_handicap o
    WHERE ST_DWithin(ST_Transform(s.geometry, 31370),ST_Transform(o.geometry, 31370),50))
)
INSERT INTO ingestion.handicap_care (id, original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, properties_secondary, imported_at, tags, deleted_at, updated_at, created_at, created_by, updated_by, geometry)
select * from alldata
WHERE geometry IS NOT NULL AND NOT ST_IsEmpty(geometry);","
ALTER TABLE IF EXISTS ingestion.handicap_care OWNER to pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.handicap_care TO pgn_group_data_team_w;","
GRANT ALL ON TABLE ingestion.handicap_care TO pgn_user_airflow;")
  
create_ingestion_table_merged <- function() {execute_sql_commands(sql_merge, "Merged ingestion data")}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----


run_smart_update = function() {
smart_update_process("handicap_care", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=TRUE, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
      process_soka_data()
      process_osm_data()
      create_ingestion_table_soka()
      create_ingestion_table_osm()
      create_ingestion_table_merged()
    }
    run_smart_update()
  }
  
  
if(run_status){
  main_function()
  }
  