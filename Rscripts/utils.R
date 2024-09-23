library(RPostgres)
library(httr)
library(dplyr)
library(stringdist)
library(stringr)
library(osmdata)
library(purrr)

# Paragon database functions ----------------------------------------------

get_con<-function(){
  con_pg <- dbConnect(Postgres(),
                      user=postgres_user, 
                      password=postgres_password,
                      host=db_host_name,
                      dbname=db_name,
                      port=5432, 
                      sslmode = 'prefer')
  return(con_pg)
}

alter_column_data_type_to_json <- function(con_pg, schema, table, column) {
  safe_schema <- dbQuoteIdentifier(con_pg, schema)
  safe_table <- dbQuoteIdentifier(con_pg, table)
  safe_column <- dbQuoteIdentifier(con_pg, column)
  sql_query <- paste0("ALTER TABLE ", safe_schema, ".", safe_table, " ALTER COLUMN ", safe_column, " TYPE JSON USING ", safe_column, "::json;")
  print(sql_query)
  res <- dbSendQuery(con_pg, sql_query)
  dbClearResult(res)
}


# ICMS API functions ------------------------------------------------------

getIcmsCookies<-function(){
  
  get_id_token<-GET("https://icmsystem.be/cobrafr-be/Account/LogOn")$cookies$value[2]
  get_coookie<-POST(url=paste0("https://icmsystem.be/cobrafr-be/Account/LogOn?__RequestVerificationToken=",get_id_token,"&UserName=",user_icms,"&Password=",password_icms),authenticate(user_icms,password_icms ))
  list_cookie <-set_cookies('__RequestVerificationToken_L2NvYnJhZnItYmU1'=get_coookie$cookies$value[get_coookie$cookies$name=='__RequestVerificationToken_L2NvYnJhZnItYmU1'],'ICMS_Cookie'=get_coookie$cookies$value[get_coookie$cookies$name=='ICMS_Cookie'])
  
  return(list_cookie)
  
}

# Extract info from JSON --------------------------------------------------

library(jsonlite)

extract_json_value <- function(json_str, key) {
  if (is.na(json_str) || !nzchar(json_str)) {
    return(NA)
  }
  tryCatch({
    json_data <- fromJSON(json_str)
    value <- json_data[[key]]
    
    # Check if the value is atomic (a single value), else return NA
    if (is.atomic(value) && length(value) == 1) {
      return(value)
    } else {
      return(NA)
    }
  }, error = function(e) {
    return(NA)
  })
}


# example usage: extract the und attribute from the name field
# join$name_und <- tolower(mapply(extract_json_value, join$name, "und"))
# NOTE: if the json attribute does not always exist, force the correct datatype, e.g. 
# join$name_und <- as.character(tolower(mapply(extract_json_value, join$name, "und")))

# Fuzzy string match ------------------------------------------------------

fuzzyStringMatch<-function(table1, table2=NULL, name_column1, name_column2, id_column1, id_column2){
  # Input templates:
    # Input = 1 table with geographical pre-selection of matches: fuzzyStringMatch(table1 = nearest_anchors, name_column1 = name_source1, name_column2 = name_source2, id_column1 = id_source1, id_column2 = id_source2)
    # Input = 2 tables without geographical pre-selection: fuzzyStringMatch(table1 = anchor_source1, table2 = anchor_source2, name_column1 = name_source1, name_column2 = name_source2, id_column1 = id_source1, id_column2 = id_source2)
    # Input = 1 table to find duplicates: fuzzystringMatch(table1 = source_with_duplicates, table2 = source_with_duplicates, name_column1 = name.x, name_column2 = name.y, id_column1 = id.x, id_column2 = id.y)
  
  # Quote/unquote input columns
  name_column1 <- rlang::parse_expr(quo_name(enquo(name_column1)))
  name_column2 <- rlang::parse_expr(quo_name(enquo(name_column2)))
  id_column1 <- rlang::parse_expr(quo_name(enquo(id_column1)))
  id_column2 <- rlang::parse_expr(quo_name(enquo(id_column2)))
  
  # Do full join of table1 and table2 if no geographical pre-selection of matches has been made
  if(missing(table2)){
    print("Geographical join has been made: no full join required")
    match_table <- table1
    match_table <- match_table %>% select(all_of(id_column1), all_of(id_column2), all_of(name_column1), all_of(name_column2))
  }
  else{
    print("Making full join of table1 and table2")
    match_table <- table1 %>% full_join(table2, by = character())
    match_table <- match_table %>% select(all_of(id_column1), all_of(id_column2), all_of(name_column1), all_of(name_column2))
  }
  
  # Pre-select potential matches by total string similarity
  sim_select <- match_table %>%
    mutate(string_sim = stringsim(tolower(match_table[[name_column1]]), tolower(match_table[[name_column2]]))) %>%
    filter(string_sim > 0.6)
  
  # Calculate string distance word by word
  match_matrix <- sim_select %>%
    mutate(split_1 = str_extract_all(sim_select[[name_column1]], boundary("word")),
           split_2 = str_extract_all(sim_select[[name_column2]], boundary("word"))) %>%
    rowwise %>%
    mutate(split_1 = list(tolower(split_1)),
           split_2 = list(tolower(split_2))) %>%
    mutate(column_matrix = list(stringdistmatrix(split_1, split_2)))
  
  # Apply fuzzy matching
  fuzzy_match <- match_matrix %>%
    mutate(some_match = ifelse(0 %in% column_matrix, TRUE, FALSE)) %>%
    mutate(words = sum(column_matrix == 0)) %>%
    mutate(length_1 = length(split_1),
           length_2 = length(split_2)) %>%
    mutate(exact_match = ifelse(all(words == length_1) | all(words == length_2), TRUE, FALSE)) %>%
    mutate(near_match = ifelse((length_1 - words <= 1 & length_1 > 1) | (length_2 - words <= 1 & length_2 > 1), TRUE, FALSE)) %>%
    filter((some_match == TRUE | string_sim > 0.8) & !!id_column1 != !!id_column2)
  
  return(fuzzy_match)
}


# Upload a table to a PostgreSQL database -------------------------------

CreateImportTable<-function(dataset, schema, table_name){
  if(exists("dataset")){
    con_pg<-get_con()
    table_id <- DBI::Id(
      schema  = schema,
      table   = table_name
    )
    table_id_t <- paste0(schema,".",table_name)
    start<-Sys.time()
    print(paste0("Start :",format(Sys.time(), "%a %b %d %X %Y")))
    print(paste0("Import data into postgresql table ", table_id_t))
    dbWriteTable(con_pg, table_id, dataset, overwrite = TRUE, row.names = FALSE )
    
    print("ID primary key")
    query <- paste("ALTER TABLE ", table_id_t,
                   "ADD COLUMN ogc_fid SERIAL;")
    dbExecute(con_pg, query)
    query <- paste("ALTER TABLE ", table_id_t,
                   "ADD PRIMARY KEY (ogc_fid);")
    dbExecute(con_pg, query)
    
    ## Close connection --------------------------------------------------------
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
    
  }else{
    print(paste0("Error, the geojson you wanted to import into ", table_id_t, "does not exist, try again"))
  }
}


# String cleanup functions --------------------------------------------

## create removal function
string_removal <- function(df, columns, patterns) {
  for (col in columns) {
    for (pattern in patterns) {
      df[[col]] <- gsub(pattern, "", df[[col]])
    }
    # Apart from removals, also do these default replacements
    df[[col]] <- gsub("-", " ", df[[col]])
    df[[col]] <- gsub(" {2,}", " ", df[[col]])
  }
  return(df)
}

# USAGE
# define a list of patterns to remove:
# patterns <- c("this needs to go", "that needs to go")
# define a list of columns:
# columns_to_clean <- c("street_1", "street_2")
# call the function
# join <- string_removal(join, columns_to_clean, patterns)

string_replacement <- function(df, columns, replacements) {
  if (length(replacements) %% 2 != 0) {
    stop("Replacements list must contain an even number of elements.")
  }
  
  for (col in columns) {
    for (i in seq(1, length(replacements), by = 2)) {
      pattern <- replacements[[i]]
      replacement_value <- replacements[[i + 1]]
      df[[col]] <- gsub(pattern, replacement_value, df[[col]], perl = TRUE)
    }
  }
  return(df)
}

# USAGE
# define a list of pairs was/becomes:
# replacements <- c("this becomes", "another thing", "and that will become", "yet another thing")
# define a list of columns:
# columns_to_clean <- c("street_1", "street_2")
# call the function
# join <- string_replacement(join, columns_to_clean, replacements)
# you can use this function also to remove strings of course




# Download OSM data ------------------------------------------

#USAGE
# List of features
## features_list <- list("generator:source" = "wind")
## OR filter-list, where you list several tags that all must apply:
#features_list <- list(
#  list(key = "leisure", value = "track"),
#  list(key = "name")
#)
## this needs to be combined with a flag at the end

# use negate=TRUE to say "the key should NOT equal the value"
## list(key = "access", value = "private", negate = TRUE)

# use key_does_not_exist=TRUE to say "the key should not exist"
##list(key = "leisure", key_does_not_exist = TRUE)


# If default server fails, set to TRUE to use mail.ru server (older data)
## alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
## extra_columns <- c("generator:output:electricity", "generator:type", "generator:model", "manufacturer", "manufacturer:ref","manufacturer:type")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
#datatypes <- c("points", "mpolygon")
# Optionally, choose a different BBOX. The default is set below, but when calling the function you can overrule this
# bbox <- c(2.15,49.15,7.07,51.8) (or simply paste bbox=c(2.15,49.15,7.07,51.8) when you call the function )
# Optionally, keep the language of the area as a columns (default set to FALSE)
#keep_region <- TRUE
# required if you want to use the feature_tag_list
## feature_tag_list<-TRUE

download_osm_process <- function(
    features_list, 
    datatypes, 
    extra_columns, 
    alternative_overpass_server,
    bbox = c(2.15,49.15,7.07,51.7),
    keep_region=FALSE,
    feature_tag_list=FALSE)
{
  
  ## (external) code
  
  ## set custom server (only needed if the main instance is giving trouble; see https://wiki.openstreetmap.org/wiki/Overpass_API#Public_Overpass_API_instances doe more options)
  if (alternative_overpass_server == TRUE) {
    custom_server_url <- "https://maps.mail.ru/osm/tools/overpass/api/interpreter"
    set_overpass_url(custom_server_url)
  }
  #default overpass server: https://overpass-api.de/api/interpreter

 
  

  
  # special situation if the query is of an AND logic (where we create a feature tag list instead of a list of features)
  if (feature_tag_list==TRUE) {

    osm_query <- opq(bbox = bbox, timeout = 60)
    
    # Reduce function to add features to the query
    osm_query <- reduce(features_list, function(opq_obj, feature) {
      if ("key_does_not_exist" %in% names(feature) && feature$key_does_not_exist) {
        # Handle requests for "this key does not exist" on the object
        opq_obj <- add_osm_feature(opq_obj, key = paste0("!", feature[[1]]))
      } else if (!("value" %in% names(feature))) {
        # Handle the case where only the key is specified
        opq_obj <- add_osm_feature(opq_obj, key = feature$key)
      } else {
        # Handle the case where both key and value are specified
        if ("negate" %in% names(feature) && feature$negate) {
          # And the key should NOT equal the value
          opq_obj <- add_osm_feature(opq_obj, key = feature$key, value = paste0("!", feature$value))
        } else {
          # And the key should equal the value
          opq_obj <- add_osm_feature(opq_obj, key = feature$key, value = feature$value)
        }
      }
      opq_obj
    }, .init = osm_query)

  
  } else {
    #default situation where we simply query a list of features
    ## get OSM data (use http://bboxfinder.com/ to make a bbox)
    # Use the defined list of features in the opq function
    osm_query <- opq(
      bbox = bbox,
      timeout = 60
    ) %>%
      add_osm_features(features = features_list)
  } 
  
 
  
  

  
  
  ## Download and clean the data
  osm_data <- try({
    osm_data <- unique_osmdata(osmdata_sf(osm_query))
  })
  
  # flag failed OSM data download
  if (inherits(osm_data, "try-error")) {
    osm_download_failed <- TRUE
    print("OSM data download failed")
  } else { print("OSM data download successful")}
  
  
  # we split the OSM data blob up by data type. We do this for polygons (whcih we assume to always exist) and points, lines and multipolygons. Even if we don't need them! At the end, we merge all the data back together, but only of the type requested by the user.
  
  osm_polygons <- osm_data$osm_polygons %>% mutate(osm_id = paste0("way/", osm_id))
  
  # check and see if we can make points (this creates osm_points if there is no error else it creates an empty set)
  result_point <- try({
    if (!is.null(osm_data$osm_points) && (nrow(osm_data$osm_points) > 0)) {
      osm_points <- osm_data$osm_points %>% mutate(osm_id = paste0("node/", osm_id))
    } else {
      # Handle the case when osm_data$osm_points is NULL
      osm_points <- osm_polygons[0, ]
    }
  })
  
  # same for multipolygons
  result_mp <- try({
    if (!is.null(osm_data$osm_multipolygons) && (nrow(osm_data$osm_multipolygons) > 0)) {
              osm_multipolygons <- osm_data$osm_multipolygons %>% mutate(osm_id = paste0("relation/", osm_id))
      } else {
      # Handle the case when osm_data$osm_multipolygons is NULL
      osm_multipolygons <- osm_polygons[0, ]
    }
  })
  
  # and same for lines
  result_line <- try({
    if (!is.null(osm_data$osm_lines) && (nrow(osm_data$osm_lines) > 0)) {
      osm_lines <- osm_data$osm_lines %>% mutate(osm_id = paste0("way/", osm_id))
    } else {
      # Handle the case when osm_data$osm_multipolygons is NULL
      osm_lines <- osm_polygons[0, ]
    }
  })  
  
  

  
  
  
  
  
  
  ### Prepare OSM points
  # Standardise columns:
  ## select only the columns that interest us, then make sure the dataframe always has these columns
  ## "extra_columns" are provided by the user
  
  default_columns <- c("osm_id", "name", "name:nl", "name:fr", "name:de", "operator:wikidata",
                       "operator", "operator:type",
                       "addr:city", "addr:housenumber", "addr:street", "addr:postcode", "nohousenumber",
                       "short_name", "old_name", "alt_name", "official_name", 
                       "contact:email", "email", "website", "contact:website", "phone", "contact:phone", "opening_hours",  "phone:2", "mobile", "contact:mobile", "alt_website", "operator:email", "operator:website",
                       "check_date", "image", "wikidata")
  
  columns_to_select <- c(default_columns, extra_columns)
  
  
  
  # Keep only interesting columns in point data
  osm_points_limited <- osm_points %>%
    select(any_of(columns_to_select))
  
  # Create a data frame with missing columns filled with NA values
  missing_columns <- as.data.frame(matrix(NA, nrow = nrow(osm_points), ncol = length(setdiff(columns_to_select, colnames(osm_points)))))
  colnames(missing_columns) <- setdiff(columns_to_select, colnames(osm_points))
  
  # Combine the selected columns and the missing columns
  points <- cbind(osm_points_limited, missing_columns)
  
  # Note: we do this because once we are in the SQL environment, we want to have a stable set of columns. If for some reason one of the columns would not exist in the data we download, then no column would exist
  
  
  ### Prepare OSM polygon info (merge polygons & multipolygons and set all as mp)
  osm_mpolygon <- bind_rows(
    osm_multipolygons,
    osm_polygons %>% mutate(geometry = st_cast(geometry, "MULTIPOLYGON"))
  )
  osm_mpolygon <- osm_mpolygon %>%
    select(any_of(columns_to_select))
  
  # Create a data frame with missing columns filled with NA values
  missing_columns_p <- as.data.frame(matrix(NA, nrow = nrow(osm_mpolygon), ncol = length(setdiff(columns_to_select, colnames(osm_mpolygon)))))
  colnames(missing_columns_p) <- setdiff(columns_to_select, colnames(osm_mpolygon))
  
  # Combine the selected columns and the missing columns
  mpolygon <- cbind(osm_mpolygon, missing_columns_p)
  
  
  ### Prepare OSM line info
  # Keep only interesting columns
  osm_lines_limited <- osm_lines %>%
    select(any_of(columns_to_select))
  
  # Create a data frame with missing columns filled with NA values
  missing_columns <- as.data.frame(matrix(NA, nrow = nrow(osm_lines), ncol = length(setdiff(columns_to_select, colnames(osm_lines)))))
  colnames(missing_columns) <- setdiff(columns_to_select, colnames(osm_lines))
  
  # Combine the selected columns and the missing columns
  lines <- cbind(osm_lines_limited, missing_columns)
  
  
  # Merge all OSM data to a single dataset
  # Datatypes provided by the user
  # if datatypes variable has more than one value, bind_rows:
  if (length(datatypes) > 1) {
    osm_all <- bind_rows(mget(datatypes))
  } else {
    osm_all <- get(datatypes)
  }
  
  
  #standardize column names: replace . with _
  osm_all <- osm_all %>%
    rename_with(~ gsub("\\.", "_", .), contains("."))

  # all column names lowercase
  osm_all <- osm_all %>%
    rename_all(tolower)  
  
  # standardise emtpy values in string columns (for some reason, relations end up having empty strings instead of nulls)
  replace_empty_with_null <- function(x) {
    ifelse(x == "", NA, x)
  }
  string_columns <- names(osm_all)[sapply(osm_all, is.character) & names(osm_all) != "geometry"]
  osm_all <- osm_all %>%
    mutate_at(vars(all_of(string_columns)), ~ replace_empty_with_null(.))
  
  
  # columns with only NULLs are boolean type. This causes problems later. So recast all of them to string.
  recast_boolean_to_string <- function(sf_data) {
    boolean_columns <- sapply(sf_data, is.logical) & names(sf_data) != "geometry"
    
    sf_data <- sf_data %>%
      mutate_if(boolean_columns, as.character)
    
    return(sf_data)
  }
  osm_all <- recast_boolean_to_string(osm_all)
  
  
  ### Fix geometries & remove cases with null or unfixable geometries + generate basic statistics ----
  
  n_invalid_geometries <- sum(!st_is_valid(osm_all$geometry))
  osm_all$geometry <- st_make_valid(osm_all$geometry)
  n_invalid_geometries_after_fix <- sum(!st_is_valid(osm_all$geometry))
  
  total_records_orig <- nrow(osm_all)
  
  # Calculate missing geometries
  missing_geometries <- sum(is.na(osm_all$geometry))
  
  # remove cases that have no geometry or an invalid geometry
  
  osm_all<-osm_all[st_is_valid(osm_all$geometry) & !is.na(osm_all$geometry), ]
  
  
  # Create a data frame with the results
  basic_stats_raw_data <- data.frame(
    dataset = "osm_all",
    total_records_orig=total_records_orig,
    total_records = nrow(osm_all),
    invalid_geometries_raw = n_invalid_geometries,
    n_invalid_geometries_after_fix=n_invalid_geometries_after_fix,
    missing_geometries = missing_geometries
  )

  # Write a report ----
  filename <- paste0(log_folder,format(Sys.time(), "%Y%m%d_%H%M%S"),"_osm_download_report.txt")
  
  
  # Add transformation check
  write.table(t(basic_stats_raw_data), filename, sep = "\t", quote = FALSE, row.names=TRUE, append = TRUE)
  cat(paste0("The table shows how many objects were downloaded, and how many were kept after removing empty and unfixable geometries.\n\n OSM query: \n",paste(paste(names(features_list), unlist(features_list), sep = "="), collapse = "\n")), file = filename, append = TRUE)
  print(paste0("Report about the new OSM data download written to ", filename))
  
  
  
  
  # join ngi_municipality & deal with multilingual & missing names ----
  ### Download NGI data
  con_pg <- get_con()
  ngi_muni <- dbGetQuery(con_pg, "SELECT niscode, CASE WHEN languagestatute=1 THEN 'dut'
WHEN languagestatute=5 THEN 'dut'
WHEN languagestatute=4 THEN 'brussels'	
WHEN languagestatute=8 THEN 'ger'	
WHEN languagestatute=7 THEN 'fre'		
WHEN languagestatute=6 THEN 'fre'
WHEN languagestatute=2 THEN 'fre'
ELSE 'und' END as language, nameger, namefre, namedut, ST_AsText(shape) as geometry FROM raw_data.ngi_ign_municipality")
  dbDisconnect(con_pg)
  ngi_muni<-st_as_sf(ngi_muni, wkt="geometry")
  ngi_muni$geometry <- st_set_crs(ngi_muni$geometry, 4326)
  
  
  # spatial join to osm data
  osm_all <- st_join(osm_all, ngi_muni, join = st_within)
  
  # localise names where possible; add city to the address where missing
  osm_all <- osm_all %>%
    mutate(name_nl = ifelse(is.na(name_nl) & language == "dut", name, name_nl)) %>%
    mutate(name_fr = ifelse(is.na(name_fr) & language == "fre", name, name_fr)) %>%
    mutate(name_de = ifelse(is.na(name_de) & language == "ger", name, name_de)) %>%
    mutate(addr_city = ifelse(is.na(addr_city) & language == "dut", namedut, addr_city)) %>%
    mutate(addr_city = ifelse(is.na(addr_city) & language == "fre", namefre, addr_city)) %>%
    mutate(addr_city = ifelse(is.na(addr_city) & language == "ger", nameger, addr_city)) %>%
    mutate(addr_city = ifelse(is.na(addr_city) & language == "brussels", paste(namefre, " - ", namedut), addr_city))
  
  if (keep_region) {
    osm_all <- osm_all %>% select(-niscode, -nameger, -namedut, -namefre)
  } else {
    osm_all <- osm_all %>% select(-niscode, -language, -nameger, -namedut, -namefre)
  }
  
  return(osm_all)
  
}


