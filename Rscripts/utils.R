Sys.setenv(TZ = "UTC")

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


# Run a list of SQL commands with error reporting based on the task name ------------

execute_sql_commands <- function(sql_commands, task_name) {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in sql_commands) {
        dbExecute(con_pg, sql_command)
      }
      print(paste(task_name, "SQL ran correctly"))
    },
    error = function(err) {
      message(paste("The SQL functions for", task_name, "failed"))
      message(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


# Upload a table to a PostgreSQL database -------------------------------

# create new or replace content
CreateImportTable<-function(dataset, schema, table_name){
  if (!exists("dataset")) {
    print(paste0("Error, the dataset you wanted to import into ", table_id_t, "does not exist, try again"))
  }else{
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
                   "ADD COLUMN IF NOT EXISTS ogc_fid SERIAL;")
    dbExecute(con_pg, query)
    query <- paste("ALTER TABLE ", table_id_t,
                   "ADD PRIMARY KEY (ogc_fid);")
    dbExecute(con_pg, query)
    # make sure the whole team can work with the table
    dbExecute(con_pg,paste0("ALTER TABLE IF EXISTS ",table_id_t," OWNER to pgn_group_data_team_w;"))
    dbExecute(con_pg,paste0("GRANT ALL ON TABLE ",table_id_t," TO pgn_group_data_team_w;"))
    dbExecute(con_pg,paste0("GRANT ALL ON TABLE ",table_id_t," TO pgn_user_airflow;"))
    
    ## Close connection --------------------------------------------------------
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
  }
}

# add new content to existing table
AppendImportTable <- function(dataset, schema, table_name) {
  if (!exists("dataset")) {
    print(paste0("Error, the dataset you wanted to append into ", schema, ".", table_name, " does not exist, try again"))
    return(invisible(NULL))
  }
  
  con_pg <- get_con()
  table_id <- DBI::Id(schema = schema, table = table_name)
  table_id_t <- paste0(schema, ".", table_name)
  
  start <- Sys.time()
  print(paste0("Start :", format(Sys.time(), "%a %b %d %X %Y")))
  print(paste0("Appending data into postgresql table ", table_id_t))
  
  # append=TRUE, overwrite=FALSE — plain INSERT, no dedup/upsert
  dbWriteTable(con_pg, table_id, dataset, append = TRUE, overwrite = FALSE, row.names = FALSE)
  
  dbDisconnect(con_pg)
  print(paste0("End :", format(Sys.time(), "%a %b %d %X %Y")))
  print(Sys.time() - start)
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


# Aggregate text columns
## When you want to create a list of unique text values and add them to all relevant rows, without turning NA's into empty strings
## note that it assumes you want to overwrite existing columns
## use within summarize or mutate

collapse_unique <- function(x) {
  out <- paste(unique(na.omit(x)), collapse = "; ")
  if (out == "") NA_character_ else out
}

#example
#join_test <- join %>%
#  group_by(outer_id) %>%
#  mutate(
#    across(
#      c(operator, addr_city),
#      collapse_unique
#    )
#  )

# summarize default OSM address info
## used during grouping, usually by outer_id
## first summarize addresses per unique street (and other address info)
## So if we have both "Steltenstraat 3" and "Steltenstraat 5" in a group, we will get "Steltenstraat 3-5"
## then summarize based on the group ID, so we will get "Steltenstraat 3-5, 1234 Municipality; Anderestraat 2, 1234 Municipality"
collapse_address <- function(df, group_var) {
  group_var <- enquo(group_var)
  
  df %>%
    mutate(addr_housenumber = if_else(!is.na(nohousenumber) & nohousenumber == "yes", "w/n", addr_housenumber)) %>%
    st_drop_geometry() %>%
    select(!!group_var, addr_street, addr_housenumber, addr_postcode, addr_city) %>%
    distinct() %>%
    arrange(!!group_var, addr_street, addr_postcode, addr_city, addr_housenumber) %>%
    group_by(!!group_var, addr_street, addr_postcode, addr_city) %>%
    summarise(
     hn = paste(unique(na.omit(addr_housenumber)), collapse = "-"),
      .groups = "drop"
    ) %>%
    filter(!is.na(addr_street)) %>%
    mutate(address = trimws(paste0(
      addr_street,
      if_else(hn != "", paste0(" ", hn), ""),
      if_else(!is.na(addr_postcode), paste0(", ", addr_postcode), ""),
      if_else(!is.na(addr_city), paste0(" ", addr_city), "")
    ))) %>%
    group_by(!!group_var) %>%
    summarise(address = paste(address, collapse = "; "), .groups = "drop")
}

## usage example:
#join <- join %>%
#  left_join(collapse_address(join, outer_id), by = "outer_id")



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



# String cleanup functions --------------------------------------------

## string removal function (remove specific words from an existing value)----
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

## string replacement function (replace specific words with other words)----
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

## Set useless strings to NA ----
# turn useless string values into NA (empty, space, dash, dot), when it's a real string column (skip json columns masquerading as string)
clean_empty_strings <- function(df) {
  df %>%
    mutate(across(
      where(~is.character(.) && !inherits(., "pq_jsonb")),
      ~na_if(trimws(.), "") %>% na_if(".") %>% na_if("-")
    ))
}
#Usage: simply as df <- clean_empty_strings(df)


# Remove all characters except numbers, letters, hyphens, and semicolons ----
clean_string <- function(x) {
  gsub("[^0-9a-zA-Z;-]", "", x)
}



# Get an Azure password --------------------------------------------

# run status
run_status<-Sys.getenv("RUN_STATUS")
## this is set to FALSE in your local renviron and is TRUE on Airflow
run_status<-ifelse(tolower(run_status) == "true", TRUE, FALSE)

get_azure_access_token <- function() {
  if (run_status==FALSE) {
  # Run the command and capture the output
  command <- "az account get-access-token --resource-type oss-rdbms"
  output <- system(command, intern = TRUE)
  
  # Convert the captured output (JSON format) to a list
  json_output <- fromJSON(paste(output, collapse = ""))
  # Extract the accessToken
  access_token <- json_output$accessToken
  print(paste0("Your access token will expire on ",json_output$expiresOn))
  # Return the access token
  return(access_token)
  } else {
    return(Sys.getenv("POSTGRES_PASSWORD"))
  }
}






# Download OSM data ------------------------------------------

## USAGE ----
# List of features
# QUERY STRUCTURE: features_list is a list of or_groups, ANDed together.
#   - Each or_group is a list of tag_filter()s, ORed together.
#   - The final WHERE clause is: (or_group1) AND (or_group2) AND ...
#
# SIMPLE USAGE (single condition):
#   features_list <- list(or_group(tag_filter("amenity", "restaurant")))
# because of as_features_list() this can be written as simply list("amenity", "restaurant")
#
# COMPLEX USAGE (AND of OR-groups):
#   features_list <- list(
#     or_group(tag_filter("country", "EU"), tag_filter("wikidata"), tag_filter("operator:wikidata")),
#     or_group(tag_filter("office"), tag_filter("amenity", "childcare"), tag_filter("tourism", "museum"))
#   )
#
# NEGATE (key != value):
#   tag_filter("access", "private", negate = TRUE)
#
# KEY MUST NOT EXIST:
#   tag_filter("leisure", not_exists = TRUE)


# Define extra tags to use as columns for properties
## extra_columns <- c("generator:output:electricity", "generator:type", "generator:model", "manufacturer", "manufacturer:ref","manufacturer:type")

# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygon" (this is polygons+multipolygons together)
#datatypes <- c("points", "mpolygon")

# Optionally, choose a different BBOX. The default is set below, but when calling the function you can overrule this
#bbox <- c(2.15,49.15,7.07,51.8) #(or simply paste bbox=c(2.15,49.15,7.07,51.8) when you call the function )
#bbox <- c(1.32,48.77,10.55,54.42)

# Optionally, keep the language of the area as a columns (default set to FALSE)
#keep_region <- TRUE


## Functions needed outside main function ----

# 1. Schema helper
## Make sure we know which tags exist as columns vs are saved in the hstore "tags" column
### Prepare connection
osm_host_name <- Sys.getenv("POSTGRES_HOST_NAME_OSM")
osm_user <- Sys.getenv("POSTGRES_USER_OSM")
osm_password <- Sys.getenv("POSTGRES_PASSWORD_OSM")
osm_db_name <- Sys.getenv("POSTGRES_DB_NAME_OSM")

get_con_osm<-function(){
  con_pg <- dbConnect(Postgres(),
                      user=osm_user, 
                      password=osm_password,
                      host=osm_host_name,
                      dbname=osm_db_name,
                      port=5432, 
                      sslmode = 'prefer')
  return(con_pg)
}
#' Fetch column names from all OSM tables, combined and deduplicated.
#' Call once per session and pass the result into tag_filter() calls.
.osm_cols_cache <- NULL
get_osm_columns <- function(force = FALSE) {
  if (is.null(.osm_cols_cache) || force) {
    con_pg <- get_con_osm()
    on.exit(dbDisconnect(con_pg))
    points   <- dbGetQuery(con_pg, "SELECT * FROM public.planet_osm_point   LIMIT 0")
    lines    <- dbGetQuery(con_pg, "SELECT * FROM public.planet_osm_line    LIMIT 0")
    polygons <- dbGetQuery(con_pg, "SELECT * FROM public.planet_osm_polygon LIMIT 0")
    .osm_cols_cache <<- unique(names(rbind(points, lines, polygons)))
  }
  .osm_cols_cache
}


# 2. Basic filter constructor
#' Create a single filter condition.
#'
#' @param key        OSM key (column name or tag key).
#' @param value      Optional value to match. NULL means "key exists".
#' @param cols       Character vector from get_osm_columns(). Used to decide
#'                   whether to query a real column or the tags hstore.
#' @param negate     If TRUE: key != value (only meaningful when value is set).
#' @param not_exists If TRUE: key/tag must be absent.

tag_filter <- function(key, value = NULL, cols = get_osm_columns(), negate = FALSE, not_exists = FALSE) {
  stopifnot(is.character(key), length(key) == 1)
  if (negate && is.null(value)) stop("negate=TRUE requires a value")
  if (not_exists && !is.null(value)) stop("not_exists=TRUE cannot be combined with a value")
  
  structure(
    list(
      key        = key,
      value      = value,
      negate     = negate,
      not_exists = not_exists,
      is_column  = key %in% cols
    ),
    class = "osm_tag_filter"
  )
}

# 3. OR-group constructor
#' Wrap one or more tag_filter()s into an OR-group.
or_group <- function(...) {
  filters <- list(...)
  if (!all(sapply(filters, inherits, "osm_tag_filter"))) {
    stop("or_group() only accepts tag_filter() objects")
  }
  structure(filters, class = "osm_or_group")
}


# 4. Feature list helper 
# 'Transform any input to the expected format, so simple old style queries keep working
as_features_list <- function(x) {
  # already new format
  if (all(sapply(x, inherits, "osm_or_group"))) return(x)
  # convert old named list format
  nms <- names(x)
  filters <- lapply(seq_along(x), function(i) {
    key   <- if (!is.null(nms) && nzchar(nms[i])) nms[i] else as.character(x[[i]])
    value <- if (!is.null(nms) && nzchar(nms[i]) && nzchar(x[[i]])) x[[i]] else NULL
    tag_filter(key, value)
  })
  list(do.call(or_group, filters))
}

# 5. Description helper
#' Transform the features list to a human readable query so it can be printed when calling the function
describe_features_list <- function(features_list) {
  features_list <- as_features_list(features_list)
  and_parts <- sapply(features_list, function(grp) {
    or_parts <- sapply(grp, function(f) {
      if (is.null(f$value)) f$key
      else paste(f$key, f$value, sep = "=")
    })
    paste(or_parts, collapse = " OR ")
  })
  if (length(and_parts) > 1) {
    and_parts <- paste0("(", and_parts, ")")
  }
  paste(and_parts, collapse = " AND ")
}

## actual OSM download function ----
download_osm_process <- function(
    features_list, 
    datatypes, 
    extra_columns=c(), 
    bbox = c(1.32,48.77,10.55,54.42),
    keep_region=FALSE)
{

  print("Downloading data from Postgres DB")
  
  # Transform simple feature lists to expected format
  features_list <- as_features_list(features_list)
  
  # These columns are always in the download
  default_columns <- c("osm_id", "name", "name:nl", "name:fr", "name:de", "operator:wikidata",
                       "operator", "operator:type",
                       "addr:city", "addr:housenumber", "addr:street", "addr:postcode", "nohousenumber",
                       "short_name", "old_name", "alt_name", "official_name", 
                       "contact:email", "email", "website", "contact:website", "phone", "contact:phone", "opening_hours",  "phone:2", "mobile", 
                       "contact:mobile", "alt_website", "operator:email", "operator:website",
                       "check_date", "image", "wikidata")  
  

 
    
    

    # Convert features_list to SQL query ----
    #' Convert a single tag_filter() to a SQL fragment.
    tag_filter_to_sql <- function(filter) {
      stopifnot(inherits(filter, "osm_tag_filter"))
      
      if (filter$is_column) {
        col <- sprintf('"%s"', filter$key)
        if      (filter$not_exists)       sprintf('%s IS NULL', col)
        else if (is.null(filter$value))   sprintf('%s IS NOT NULL', col)
        else if (filter$negate)           sprintf("%s != '%s'", col, filter$value)
        else                              sprintf("%s = '%s'", col, filter$value)
      } else {
        tag <- sprintf("tags -> '%s'", filter$key)
        if      (filter$not_exists)       sprintf('%s IS NULL', tag)
        else if (is.null(filter$value))   sprintf('%s IS NOT NULL', tag)
        else if (filter$negate)           sprintf("%s != '%s'", tag, filter$value)
        else                              sprintf("tags @> '\"%s\"=>\"%s\"'", filter$key, filter$value)
      }
    }
    
    #' Convert a features_list of or_group()s to a full SQL WHERE clause string.
    #' Result is suitable for pasting directly after WHERE.
    build_where_clause <- function(features_list) {
      if (!is.list(features_list) || length(features_list) == 0) stop("features_list must be a non-empty list")
      
      and_parts <- vapply(features_list, function(grp) {
        stopifnot(inherits(grp, "osm_or_group"))
        sql_parts <- vapply(grp, tag_filter_to_sql, character(1))
        if (length(sql_parts) == 1) sql_parts else sprintf("(%s)", paste(sql_parts, collapse = " OR "))
      }, character(1))
      
      paste(and_parts, collapse = " AND ")
    }
    
    
    # Query execution -----
    
    #' Actual OSM download for a features_list.
    #'
    #' @param features_list  list of or_group()s — the query to execute.
    #' @param bbox           Numeric vector c(xmin, ymin, xmax, ymax) in WGS84.
    #' @param datatypes      Character vector, any of "points", "lines", "mpolygon".
    #' @return Named list with elements corresponding to requested datatypes.
      where <- build_where_clause(features_list)
      bbox_sql <- sprintf("ST_MakeEnvelope(%s, %s, %s, %s, 4326)", bbox[1], bbox[2], bbox[3], bbox[4])
      
      make_query <- function(table) {
        sprintf(
          "SELECT *, ST_AsText(ST_Transform(way, 4326)) AS geometry
       FROM public.%s
       WHERE (%s) AND ST_Transform(way, 4326) && %s",
          table, where, bbox_sql
        )
      }
      
      con_pg <- get_con_osm()
      on.exit(dbDisconnect(con_pg))
      
      if ("points"   %in% datatypes) points   <- dbGetQuery(con_pg, make_query("planet_osm_point"))
      if ("lines"    %in% datatypes) lines     <- dbGetQuery(con_pg, make_query("planet_osm_line"))
      if ("mpolygon" %in% datatypes) mpolygon  <- dbGetQuery(con_pg, make_query("planet_osm_polygon"))
    
    
      # Make readable OSM ids and set as SF objects
      if ("points" %in% datatypes) {
        points <- points %>% 
          mutate(osm_id = 
                   dplyr::if_else(osm_id < 0, # Relations recognizable by the negative integer
                                  paste0("relation/", abs(osm_id)),  # Make them positive and prefix with "relation/" for a valid url
                                  paste0("node/", osm_id))) # all the others are nodes
        # prepare the geometry
        points<-st_as_sf(points, wkt="geometry")
        points$geometry <- st_set_crs(points$geometry, 4326)
      }
      if ("lines" %in% datatypes) {
        lines <- lines %>% 
          mutate(osm_id = 
                   dplyr::if_else(osm_id < 0,
                                  paste0("relation/", abs(osm_id)),
                                  paste0("way/", osm_id)))
        lines<-st_as_sf(lines, wkt="geometry")
        lines$geometry <- st_set_crs(lines$geometry, 4326)
      }
      if ("mpolygon" %in% datatypes) { mpolygon <- mpolygon %>%
        mutate(osm_id = 
                 dplyr::if_else(osm_id < 0,
                                paste0("relation/", abs(osm_id)),
                                paste0("way/", osm_id)))
      mpolygon<-st_as_sf(mpolygon, wkt="geometry")
      mpolygon$geometry <- st_set_crs(mpolygon$geometry, 4326)
      # Set all polygons as multipolygons
      mpolygon <- mpolygon %>% mutate(geometry = st_cast(geometry, "MULTIPOLYGON"))
      }         
    
    # Merge all OSM data to a single dataset
    # Datatypes provided by the user
    # if datatypes variable has more than one value, bind_rows:
    osm_all <- try({
      if (length(datatypes) > 1) {
        osm_all <- bind_rows(mget(datatypes))
      } else {
        osm_all <- get(datatypes)
      }
    })
        

    # flag failed OSM data download
    if (inherits(osm_all, "try-error")) {
      stop("OSM data download failed")
    } else { print("Postgres OSM data download successful")}

    
    # multipolygons that are discontinuous are returned as "islands" with the same ID. Here we merge them again
    ## select just the duplicate osm_id's
    osm_all_dup <- osm_all %>% filter(duplicated(osm_id) | duplicated(osm_id, fromLast = TRUE))
    if(nrow(osm_all_dup) > 0) {
      osm_all_dup <- osm_all_dup %>%
        group_by(osm_id) %>%  # Group by osm_id
        summarise(
          geometry = st_union(geometry),  # Combine geometries into a single geometry
          across(everything(), first),    # Take the first value for other attributes
          .groups = 'drop'                  # Ungroup the result
        )
      
      dup_ids <- as.data.frame(osm_all_dup) %>% select(osm_id)
      
      # remove the duplicates from osm_all
      osm_all <- osm_all %>% anti_join(dup_ids, by = "osm_id")
      
      # add the merged objects
      osm_all <- bind_rows(osm_all, osm_all_dup)
    }


    
    ### Harmonize columns ----
    # Find missing columns compared to user request
    ## Function to compare requested keys with existing dataframe columns
    get_missing_keys <- function(requested_keys, data) {
      # Get the existing column names from the dataframe
      existing_keys <- names(data)
      
      # Find missing keys that are in requested_keys but not in existing_keys
      keys_to_extract <- requested_keys[!requested_keys %in% existing_keys]
      
      return(keys_to_extract)
    }
    
    ## Merge the default and extra columns
    expected_columns <- c(default_columns, extra_columns)
    ## Get missing keys
    keys_to_extract <- get_missing_keys(expected_columns, osm_all)
    
    # Get the missing columns based on the tag column
    ## Function to extract multiple key-value pairs from tags into new columns
    add_columns <- function(data, keys) {
      for (key in keys) {
        # Match the key and capture everything up to the next unescaped quote
        pattern <- paste0("\"", key, "\"=>\"(.*?)\"(?=,|$)")
        
        if (!key %in% names(data)) {
          data <- data %>%
            mutate(!!sym(key) := {
              extracted <- str_match(tags, pattern)[, 2]
              # Unescape any escaped quotes
              str_replace_all(extracted, "\\\\\"", "\"")
            })
        }
      }
      return(data)
    }
    
    
    osm_all <- add_columns(osm_all, keys_to_extract)
    
    
    # keep only columns listed in default_columns or extra_columns
    osm_all <- osm_all %>% select(all_of(expected_columns))
    
    #standardize column names: replace : with _
    osm_all <- osm_all %>%
      rename_with(~ gsub("\\:", "_", .), contains(":"))
    
        
    
  
  # keep only objects within our area of interest
  ## download the spatial filter
  con_pg_curation <- get_con()
  on.exit(dbDisconnect(con_pg_curation), add = TRUE)
  spatial_filter <- dbGetQuery(con_pg_curation, "SELECT ST_AsHEXEWKB(geometry) as geometry FROM raw_data.anchor_db_spatial_filter")
  spatial_filter$geometry <- st_as_sfc(structure(spatial_filter$geometry, class = "WKB"), EWKB = TRUE)
  spatial_filter$geometry <- st_set_crs(spatial_filter$geometry, 4326)
  spatial_filter <- st_as_sf(spatial_filter)
  ## select only records within this geometry
  osm_all <- st_filter(osm_all, spatial_filter, .predicate = st_intersects)
  
  #standardize column names: replace . with _
  osm_all <- osm_all %>%
    rename_with(~ gsub("\\.", "_", .), contains("."))

  # all column names lowercase
  osm_all <- osm_all %>%
    rename_all(tolower)  
  


  osm_all <- clean_empty_strings(osm_all)
  
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
  filename <- paste0(log_folder,"/",format(Sys.time(), "%Y%m%d_%H%M%S"),"_osm_download_report.txt")
  
  
  # Add transformation check
  cat("Summary of the data downloaded from OSM:
      ")
capture <- capture.output(print(t(basic_stats_raw_data), row.names=FALSE))
cat(paste(capture, collapse="\n"), "\n")
cat(paste0("The table shows how many objects were downloaded, and how many were kept after removing empty and unfixable geometries.\n\n OSM query: \n",
           paste(describe_features_list(features_list)),"\n"))  
  

  
  # join ngi_municipality & deal with multilingual & missing names ----
  ### Download NGI data
  # if ngi_muni_cleaned exists, set ngi_muni to ngi_muni_cleaned
  if (exists("ngi_muni_cleaned")) {
    ngi_muni <- ngi_muni_cleaned
  } else {
  ngi_muni <- dbGetQuery(con_pg_curation, "SELECT niscode, CASE WHEN languagestatute=1 THEN 'dut'
WHEN languagestatute=5 THEN 'dut'
WHEN languagestatute=4 THEN 'brussels'	
WHEN languagestatute=8 THEN 'ger'	
WHEN languagestatute=7 THEN 'fre'		
WHEN languagestatute=6 THEN 'fre'
WHEN languagestatute=2 THEN 'fre'
ELSE 'und' END as language, nameger, namefre, namedut, ST_AsHEXEWKB(shape) as geometry FROM raw_data.ngi_ign_municipality")
  ngi_muni$geometry <- st_as_sfc(structure(ngi_muni$geometry, class = "WKB"), EWKB = TRUE)
  ngi_muni$geometry <- st_set_crs(ngi_muni$geometry, 4326)
  ngi_muni <- st_as_sf(ngi_muni)
  ngi_muni_cleaned<<-ngi_muni
  }
 
  # spatial join to osm data (the "first" object it touches, so objects on a border get a random value assigned)
  osm_all <- osm_all %>%
    st_join(ngi_muni, join = st_intersects) %>% 
    group_by(osm_id) %>%
    slice(1) %>%
    ungroup()
  
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


# Run a process with stop or success message ----

run_process <- function(expr, process_name) {
  tryCatch(
    {
      result <- expr
      message(paste("Process", process_name, "ran successfully"))
      return(result)
    },
    error = function(e) {
      stop(paste("Error in process", process_name, ":", e$message))
    }
  )
}


# Join a dataset interesting because of its attributes to a dataset interesting for its geometry, based on distance, name and address notation ----

# assumes both datasets are in Lambert72
# can consume other distance_matched_threshold and distance_raw_threshold values

merge_to_external_polygons <- function(
    attribute_features, # name of the dataset with the interesting attributes
    geom_features, # name of the dataset with the interesting geometries
    attr_id_col = "original_id", # name of the unique identifier in the attribute dataset
    geom_id_col = "osm_id", # name of the unique identifier in the geometry dataset 
    attr_name_col = "name", # name of the name field (attr)
    geom_name_col = "name", # name of the name field (geom)
    attr_street_col, # name of the streetname column (attr)
    attr_hnr_col, # name of the house number column (attr)
    geom_street_col,# name of the streetname column (geom)
    geom_hnr_col, # name of the house number column (geom)
    distance_matched_threshold = 50, # distance where we consider something "quite closeby"
    distance_raw_threshold = 250, # remotest distance where we want to look for possible candidates
    patterns_to_remove, # a list of strings that should be removed from the name, because they are meaningless in the context (e.g. the word "school" should not help make a match better if the dataset is about schools and lots of records include that word in the name
    force_same_name = "no" # by default, objects can be merged, even if they don't have a similar name. Use "strong" to enforce same name in all circumstances and "soft" to enforce same name unless the distance is less than 1/10 of the distance_matched_threshold
) {
  
  start_time <- Sys.time()
  
  attr_df_name<-deparse(substitute(attribute_features))
  geom_df_name<-deparse(substitute(geom_features))
  
  print(paste0("Merging the records in ",attr_df_name," to the objects in ",geom_df_name," based on distance, name similarity and address notation similarity"))
  
  # stop if not both datasets are in 31370
  if (st_crs(attribute_features)$epsg != 31370) {
    stop("Geometry of the features providing attributes should be in EPSG:31370")
  }
  if (st_crs(geom_features)$epsg != 31370) {
    stop("Geometry of the features providing a geometry should be in EPSG:31370")
  }
  

  
  
  # Prepare attribute_features
  attr_features <- attribute_features %>%
    mutate(
      attr_id = .data[[attr_id_col]],
      name_attr = .data[[attr_name_col]],
      street_attr = .data[[attr_street_col]],
      hnr_attr = .data[[attr_hnr_col]]
    )
  
  # Prepare geom_features
  geom_features <- geom_features %>%
    mutate(
      geom_id = .data[[geom_id_col]],
      name_geom = .data[[geom_name_col]],
      street_geom = .data[[geom_street_col]],
      hnr_geom = .data[[geom_hnr_col]]
    )
  
  # create attr_features as sf with the original geometry
  st_geometry(attr_features) <- "off_geometry"
  st_geometry(geom_features) <- "geometry"
  
  # limit attributes to just the needed things
  attr_features <- attr_features %>%
    select(attr_id, name_attr, street_attr, hnr_attr)
  
  # limit geom to just needed things
  geom_features <- geom_features %>%
    select(geom_id, name_geom, street_geom, hnr_geom)
  
  
  
  # join nearby objects from both datasets
  join <- st_join(attr_features, geom_features, join = st_is_within_distance, dist = distance_raw_threshold)
  join <- as.data.frame(join)
  
  # add the geometry from the osm objects
  geom_features_geo<-geom_features %>% select(geom_id, geometry)
  geom_features_geo<-as.data.frame(geom_features_geo)
  join <- left_join(join, geom_features_geo, by = "geom_id")
  
  # Calculate distance between potential matches
  #join$distance <- mapply(calculate_distance, join$geometry, join$off_geometry)
  # we use euclidian distance, as our default Hausdorf distance isn't great for point vs polygon comparisons (de facto it gives the furthest point from the nearby polygons)
  join$distance <- mapply(calculate_eucl_distance, join$geometry, join$off_geometry)
  
  
  # status message
  join_time<-Sys.time()
  joining_time <- round(as.numeric(difftime(join_time, start_time, units = "secs")), 2)
  print(paste0("Joined attr_features data to OSM (took ",joining_time," seconds), now preparing choosing which links to keep"))
  
  
  # keep only the records with a link to an OSM geometry
  join <- join %>% filter(!is.na(geom_id))
  
  # acceptance elements:
  ### 1. name similarity ----
  
  #set names to lowercase
  join$name_attr<-tolower(join$name_attr)
  join$name_geom<-tolower(join$name_geom)
  
  # remove accents
  join <- join %>%
    mutate(
      name_attr=stri_trans_general(name_attr, "Latin-ASCII"),
      name_geom=stri_trans_general(name_geom, "Latin-ASCII")
    )
  

  # apply to these columns:
  columns_to_clean <- c("name_geom", "name_attr")
  
  # default removals
  default_patterns_to_remove <- c("\"", "”", "“", "´", "\\*")
  join <- string_removal(join, columns_to_clean, default_patterns_to_remove)
  # string_removal is a function defined in utils.R
  
  # remove words specific to the context
  
  join <- string_removal(join, columns_to_clean, patterns_to_remove)
  
  
  # remove leading and trailing whitespaces
  join <- join %>%
    mutate(
      name_attr = str_squish(name_attr),
      name_geom = str_squish(name_geom)
    )
 
  # check for identical names
  join <- join %>%
    mutate(
      same_name = ifelse(name_attr==name_geom, 1, 0)
    )
  
  # Small function to use LCS without throwing errors if one of the values is NA
  safe_lcs <- function(a, b) {
    if (is.na(a) | is.na(b)) return(NA_character_)
    LCSn(c(tolower(a), tolower(b)))
  }

  # find the longest common substring  
  join <- join %>%
    rowwise() %>%
    mutate(
      lcs = safe_lcs(name_attr, name_geom),
      longest_common_str_length = nchar(lcs)
    )
  
  # is one name the substring of the other
  join <- join %>%
    mutate(
      condition = !is.na(name_attr) & !is.na(name_geom) &
        nchar(as.character(name_attr)) > 1 &
        nchar(as.character(name_geom)) > 1,
      name_sim = ifelse(
        condition,
        as.integer(
          str_detect(as.character(name_attr), fixed(as.character(name_geom))) |
            str_detect(as.character(name_geom), fixed(as.character(name_attr)))
        ),
        0L
      )
    ) %>%
    select(-condition)

  # shorten name before calculating length
  join$length_name<-nchar(join$name_attr)

  
  # Decide when we consider the names to be the same (length should de significant)
  join <- join %>%
    mutate(
      same_name = ifelse(
        ((longest_common_str_length > 10 | (longest_common_str_length > 4 & longest_common_str_length <= 10 & longest_common_str_length > length_name / 2)) |
          name_attr==name_geom |
          same_name==1 |
          name_sim==1),
        1,
        0
      )
    )
  
  
  join <- join %>%
    select(-lcs, -longest_common_str_length, -length_name, -name_sim)
  
  
  ### 2. street similarity ----
  
  # set hnr as missing if identical to street name
  join <- join %>%
    mutate(hnr_geom = ifelse(hnr_geom == street_geom, NA, hnr_geom))
  
  
  # SIMPLIFY STREET NAMES
  
  # remove accents
  join <- join %>%
    mutate(
      street_geom = tolower(stri_trans_general(street_geom, "Latin-ASCII")),
      street_attr = tolower(stri_trans_general(street_attr, "Latin-ASCII"))
    )
  
  
  
  # deal with common abbreviations; remove usual additions to only compare the core of the street name
  replacements <- c(
    "rue de ", "",
    "rue des ", "",
    "rue du ", "",
    "rue", "",
    "st\\.", "sint",
    "^\\b(st)\\b(?=\\s|\\.)", "sint",
    "^\\b(bd)\\b(?=\\s|\\.)", "boulevard",
    "straat", "",
    "boulevard", "",
    "avenue", "",
    "-", " ",
    "straße", "",
    "strasse", "",
    "laan", "",
    "\\s+$", "",
    "^\\s+", ""
  )
  # Define the columns to clean
  columns_to_clean <- c("street_geom", "street_attr")
  join <- string_replacement(join, columns_to_clean, replacements)
  # string_replacement is a function defined in utils.R
  
  # remove leading and trailing whitespaces
  join <- join %>%
    mutate(
      street_geom = trimws(street_geom),
      street_attr = trimws(street_attr)
    )
  
  
  # if result is empty string, replace with NA
  join <- join %>%
    mutate(
      street_geom = ifelse(street_geom == "", NA, street_geom),
      street_attr = ifelse(street_attr == "", NA, street_attr)
    )
  # END SIMPLIFY STREET NAMES
  
  # make sure the result is still text
  join$street_geom <- as.character(join$street_geom)
  join$street_attr <- as.character(join$street_attr)
  
  
  # calculate string distance between streets in both datasets
  join <- join %>%
    mutate(streetsim = stringsim(street_geom,street_attr))
  
  # check if the street on one side is a substring of street on the other side
  join <- join %>%
    mutate(
      condition = nchar(street_geom) > 3 & nchar(street_attr) > 3,
      substring_left_check = ifelse(condition, str_detect(street_attr, replace_na(street_geom,"")), FALSE),
      substring_right_check = ifelse(condition, str_detect(street_geom, replace_na(street_attr,"")), FALSE),
      streetsim = ifelse((substring_left_check | substring_right_check) & streetsim<0.9, 0.9, streetsim)
    ) %>%
    select(-condition, -substring_right_check, -substring_left_check)
  
  join <- join %>%
    mutate(
      hnr_geom = clean_string(hnr_geom),
      hnr_attr = clean_string(hnr_attr)
    )
  # make sure the housenumber is text
  join$hnr_geom <- as.character(join$hnr_geom)
  join$hnr_attr <- as.character(join$hnr_attr)
  
  join <- join %>%
    mutate(hnr_sim = stringsim(as.character(hnr_geom), as.character(hnr_attr)))
  
  
  # check if the housenumber on one side is a substring of housenumber on the other side, if the number is at least two digits
  join <- join %>%
    mutate(
      condition = nchar(as.character(hnr_attr)) > 1 & nchar(as.character(hnr_geom)) > 1,
      substring_left_check = ifelse(condition, str_detect(as.character(hnr_attr), replace_na(as.character(hnr_geom),"")), FALSE),
      substring_right_check = ifelse(condition, str_detect(as.character(hnr_geom), replace_na(as.character(hnr_attr),"")), FALSE),
      hnr_sim = ifelse((substring_left_check | substring_right_check) & hnr_sim<0.9 , 0.9, hnr_sim)
    ) %>%
    select(-condition, -substring_right_check, -substring_left_check)
  
  join <- join %>%
    mutate(
      hnr_sim = if_else(
        !is.na(as.numeric(hnr_geom)) & !is.na(as.numeric(hnr_attr)),
        ifelse(abs(as.numeric(hnr_geom) - as.numeric(hnr_attr)) < 10 & hnr_sim<0.9, 0.9, hnr_sim),
        hnr_sim
      )
    )
  
  
  
  ## end street similarity
  
  
  
  ## combine choice elements
  
  # calculate address certainty
  join<- join %>%
    mutate(address_certainty = case_when(
      streetsim == 1 & hnr_sim == 1 ~ 1,
      streetsim == 1 & is.na(hnr_sim) ~ 0.95,
      streetsim >= 0.9 & hnr_sim >= 0.9 ~ 0.93,
      streetsim >= 0.9 & is.na(hnr_sim) ~ 0.92,
      streetsim >= 0.9 & hnr_sim < 0.9 ~ 0.85,
      TRUE ~ 0
    ))
  
  # status message
  choice_prep_time<-Sys.time()
  choice_prepping_time <- round(as.numeric(difftime(choice_prep_time, join_time, units = "secs")), 2)
  print(paste0("Prepared for choosing which links to keep (took ",choice_prepping_time," seconds), now making choices"))
  
  
  
  ### Make choices ----
  
  # SELECT only if certain quality thresholds met
  
  ## how important is name concordance?
  ## make sure a valid option was provided
  force_same_name <- match.arg(force_same_name, choices = c("no", "strong", "soft"))
  if (force_same_name=="no") {
    join <- join %>%
      filter(distance < distance_matched_threshold | address_certainty >= 0.85 | same_name ==1)
    } else if (force_same_name=="strong") {
    join <- join %>%
      filter(same_name ==1)
    } else if (force_same_name=="soft") {
    join <- join %>%
      filter(same_name == 1 | distance <= distance_matched_threshold/10)
  }  
  
  
  # order by distance
  join <- join %>%
    group_by(attr_id) %>% 
    arrange(distance) %>%
    mutate(distance_id = row_number()) %>%
    ungroup()
  
  # order by address certainty
  join <- join %>%
    group_by(attr_id) %>% 
    arrange(desc(address_certainty)) %>%
    mutate(address_certainty_id = row_number()) %>%  
    ungroup()
  
  # create scoring variable:
  # the closest object gets 1 point if it is within threshold, 0.75 if it is within 2x treshold. Further objects get 0.65 points if they are within the raw distance threshold
  # the best address gets 0.75 points, other addresses with a decent score get 0.5 points
  # if the names are the same, the object gets 1 point
  join <- join %>%
    mutate(same_name=ifelse(is.na(same_name), 0, same_name)) %>%
    mutate(
      score = 
        ifelse(distance_id == 1 & distance < distance_matched_threshold, 1, 0) +
        ifelse(distance_id == 1 & distance > distance_matched_threshold & distance < 2*distance_matched_threshold, 0.75, 0) +
        ifelse(distance_id >1 & distance < distance_matched_threshold, 0.65, 0) +
        ifelse(address_certainty>0 & address_certainty_id == 1, 0.75, 0) + ifelse(address_certainty>0 & address_certainty_id > 1, 0.5, 0) +
        ifelse(same_name == 1, 1, 0)
    )
  
  
  
  # keep only the cases where the score equals the max score for the ogc_fid
  join <- join %>%
    group_by(attr_id) %>% 
    filter(score == max(score)) %>%
    ungroup()
  
  # if there's still more than one, keep the one with the closest distance (prepare filter)
  join <- join %>%
    group_by(attr_id) %>% 
    arrange(distance) %>%
    mutate(
      count=n(),
      distance_id = row_number()
    ) %>%
    ungroup()
  # filter
  join <- join %>%
    filter(count == 1 | (count>1 & distance_id == 1))
  
  
  # Minimum threshold to keep a link: if score<1.75 & distance > threshold, delete the row
  join <- join %>%
    filter(score >= 1.75 | distance < distance_matched_threshold)
  
  
  # simplify data
  join_simplified <- join %>%
    select(attr_id, geom_id) %>%
    distinct()
  
  # status message
  choice_time<-Sys.time()
  choosing_time <- round(as.numeric(difftime(choice_time, choice_prep_time, units = "secs")), 2)
  print(paste0("Choices made (took ",choosing_time," seconds), now summarizing data"))
  
  # enrich original data
  print("Adding the geom IDs to the attribute dataset. Don't forget to add geometries, attributes and summarize, as well as potentially adding the unused records from the geometry dataset later")
  attr_features <- left_join(attribute_features, join_simplified, by = setNames("attr_id", attr_id_col))
  
  # count number of succesful matches
  print(paste0("From the ",nrow(attr_features)," records in ",attr_df_name," a total of ",sum(!is.na(attr_features$geom_id))," were matched to a record of the ",geom_df_name," dataset"))

  # rename the added column back to its original value
  attr_features <- attr_features %>%
    rename(!!geom_id_col := geom_id)
  
  # transform back to WGS84
  attr_features <- st_transform(attr_features, 4326)
  
  return(attr_features)
  
}



# Concatenate names of records in a group, with the value of the "outer" as primary, and any other names between brackets ----

aggregate_names <- function(df, names_to_aggregate) {
  for (col in names_to_aggregate) {
    df <- df %>%
      group_by(outer_id) %>%
      mutate(
        primary_name = ifelse(!is.na(get(col))[osm_id == outer_id][1], get(col)[osm_id == outer_id][1], NA),
        other_names = paste(
          unique(na.omit(get(col)[osm_id != outer_id & (is.na(primary_name) | get(col) != primary_name)])),
          collapse = ", "
        ),
        across(col, ~ case_when(
          count > 1 & !is.na(other_names) & other_names != "" & (is.na(primary_name) | primary_name == "") ~ other_names,
          count > 1 & !is.na(other_names) & other_names != "" ~ paste0(primary_name, " (", other_names, ")"),
          TRUE ~ .
        ))
      ) %>%
      ungroup() %>%  select(-primary_name, -other_names)  # Remove temporary columns
  }
  
  return(df)
}


# Merge OSM objects to OSM objects they are completely within ----
osmspatial_deduplication <- function(
	osm_dataset, # name of the dataset to deduplicate
	custom_columns_to_aggregate, # list of columns in c(x, y) format beyond default OSM selection that you wish to aggregate
	pre_aggregation_hook = NULL
	) {
## first define a set of potential outers
    osm_outers<-osm_dataset %>%
      select(outer_id=osm_id) %>%
      filter(!st_is(geometry, "POINT"))

    print(paste0("There are ",nrow(osm_outers)," potential outers"))
	  print(paste0("Original number of objects is ",nrow(osm_dataset)))
      ## then join them to the objects
      join <- st_join(osm_dataset, osm_outers, join = st_within)
      
      # count the number of times an inner is used (to later find objects that are linked to itself and something else too, or to more than one outer)
      join<-join %>%
        mutate(outer_id=case_when(!is.na(outer_id)~outer_id, TRUE~osm_id)) %>%
        group_by(osm_id) %>%
        mutate(count_inner=n())
      # remove if the inner is used by an outer but still refers to itself as well
      join<-join %>%
        filter(count_inner==1 | (count_inner>1 & osm_id!=outer_id))
      
      
      # add a count with the number of times an outer is used
      join<-join %>%
        group_by(outer_id) %>%
        mutate(count=n())
      
	    print(paste0("The object with the largest amount of 'inners' has ",max(join$count)-1," objects that will be merged to it"))
	  
      # count inners again and give them an order_inner number, where 1 is for the case linked to the biggest count (outer)
      join<-join %>%
        group_by(osm_id) %>%
        mutate(count_inner=n()) %>%
        arrange(desc(count)) %>%
        mutate(order_inner = row_number())
      
      # if count_inner>1, take the first one (i.e. the one linked to the biggest polygon)
      join<-join %>%
        filter(count_inner==1 | (count_inner>1 & order_inner==1))
      
	  
	  default_columns_to_aggregate <- c("contact_email", "email", "website", "contact_website", "phone", "contact_phone",
              "opening_hours", "phone_2", "mobile", "contact_mobile", "alt_website", "operator_email", "check_date", "image", "wikidata", "network_wikidata", "language")
	  
	  all_columns_to_aggregate <- c(custom_columns_to_aggregate,default_columns_to_aggregate)
	  
	  # Apply optional pre-aggregation transformation
	  if (!is.null(pre_aggregation_hook)) {
	    join <- pre_aggregation_hook(join)
	  }

      # create normal aggregated variables
      join <- join %>%
        group_by(outer_id) %>%
        mutate(
          across(any_of(all_columns_to_aggregate), ~ collapse_unique(.x))
        )
      
      
      # Define list of columns for name aggregation
      names_to_aggregate <- c("name", "name_nl", "name_fr", "name_de", "alt_name", "short_name", "official_name", "old_name")
      
      
      # Call the function to aggregate names
      join <- aggregate_names(join, names_to_aggregate)
      
      # Build collapsed address per outer
      address_df <- collapse_address(join, outer_id)
      join <- join %>%
        left_join(address_df, by = "outer_id")
      
      # Save a dataframe with just the OSM_id of objects that have been simplified
      osm_diagnostic <- as.data.frame(join) %>%
        filter(count > 1 & osm_id != outer_id) %>%
        select(osm_id, outer_id, count)
      
      # Filter records based on the count and outer_id conditions
      ## Grouped objects only have a different ID and geometry at this point. This filter keeps just the record we defined as outer.
      osm_dedup <- join %>%
        filter(count == 1 | (count > 1 & outer_id == osm_id)) %>%
        select(-outer_id, - count_inner, -count, -order_inner)
      print(paste0("Number of objects after deduplication ",nrow(osm_dedup),". There were ",nrow(osm_diagnostic)," objects that received info of at least one other object."))
      
      # make available outside the function
      return(list(
        data = osm_dedup,
        diagnostic = osm_diagnostic
      ))
    
}

# usage example:  
#result <- osmspatial_deduplication(osm_all, c("tourism", "rooms"))
#osm_all <- result$data
#osm_diagnostic <- result$diagnostic


# summarize dataset based on some shared attribute ----
summarize_by_value <- function(df, group_col, id_col, summary_cols, max_cols = NULL, geom_col = "geometry") {
  max_cols <- intersect(max_cols, summary_cols)
  cols_to_collapse <- setdiff(c(id_col, summary_cols), c(group_col, max_cols))
  cols_to_max <- setdiff(max_cols, group_col)
  has_geom <- geom_col %in% names(df)
  add_geom <- has_geom && !(geom_col %in% group_col)
  
  collapse_unique <- function(x) {
    vals <- unique(na.omit(x))
    if (length(vals) == 0) NA_character_ else paste(vals, collapse = "; ")
  }
  
  # note: make sure the data has no empty strings before running this (run clean_empty_strings if needed)
  collapse_max <- function(x) {
    vals <- na.omit(x)
    if (length(vals) == 0) return(NA)
    num_vals <- suppressWarnings(as.numeric(as.character(vals)))
    if (any(is.na(num_vals))) {
      stop("collapse_max: non-numeric value(s) encountered: ",
           paste(unique(vals[is.na(num_vals)]), collapse = ", "))
    }
    
    max(num_vals)
  }
  
  grouped <- df %>%
    group_by(across(all_of(group_col)))
  
  agg_exprs <- list(
    rlang::expr(across(all_of(cols_to_collapse), collapse_unique))
  )
  if (length(cols_to_max) > 0) {
    agg_exprs <- c(agg_exprs, list(rlang::expr(across(all_of(cols_to_max), collapse_max))))
  }
  
  if (add_geom) {
    grouped %>%
      summarize(
        !!!agg_exprs,
        "{geom_col}" := first(.data[[geom_col]]),
        .groups = "drop"
      )
  } else {
    grouped %>%
      summarize(
        !!!agg_exprs,
        .groups = "drop"
      )
  }
}

# usage example
#seveso_dedup <- summarize_by_value(
#  df           = seveso_all,
#  group_col    = "geometry",
#  id_col       = "original_id",
#  summary_cols = c("name", "data_list_id", "risk_level", "streetname", "city", "zip"),
#  max_cols     = "risk_level"
#)

# use certain columns to create a properties json column, then remove the columns used for it ----
build_json_column <- function(df, cols, output_col, data_list_id = NULL) {
  df %>%
    rowwise() %>%
    mutate(!!output_col := {
      vals <- purrr::discard(
        as.list(pick(all_of(cols))),
        is.na
      )
      if (!is.null(data_list_id)) {
        vals <- list(vals)
        names(vals) <- data_list_id
      }
      jsonlite::toJSON(vals, auto_unbox = TRUE)
    }) %>%
    ungroup() %>%
    select(-all_of(setdiff(cols, output_col)))
}

# standardize default OSM columns so they can be used in the properties ----
build_osm_derived_columns <- function(df) {
  
  cols_to_remove <- c(
    "short_name", "official_name", "alt_name", "old_name",
    "contact_email", "email",
    "contact_mobile", "mobile", "contact_phone", "phone", "phone_2",
    "website", "contact_website", "alt_website",
    "addr_street", "addr_housenumber", "nohousenumber", "addr_postcode", "addr_city"
  )
  
  # Ensure all potentially missing columns exist as NA
  address_cols <- c("address", "addr_street", "addr_housenumber", "nohousenumber", "addr_postcode", "addr_city")
  for (col in address_cols) {
    if (!col %in% names(df)) df[[col]] <- NA_character_
  }
  
  other_name_cols <- c("short_name", "official_name", "alt_name", "old_name")
  for (col in other_name_cols) {
    if (!col %in% names(df)) df[[col]] <- NA_character_
  }
  
  contact_cols <- c("contact_email", "email", "contact_mobile", "mobile", 
                    "contact_phone", "phone", "phone_2", "website", "contact_website", "alt_website")
  for (col in contact_cols) {
    if (!col %in% names(df)) df[[col]] <- NA_character_
  }
  
  df %>%
    rowwise() %>%
    mutate(
      other_names = ifelse(
        all(is.na(c(short_name, official_name, alt_name, old_name))),
        NA_character_,
        paste(na.omit(c(short_name, official_name, alt_name, old_name)), collapse = "; ")
      ),
      local_email = ifelse(
        all(is.na(c(contact_email, email))),
        NA_character_,
        paste(na.omit(c(contact_email, email)), collapse = "; ")
      ),
      local_phone = ifelse(
        all(is.na(c(contact_mobile, mobile, contact_phone, phone, phone_2))),
        NA_character_,
        paste(na.omit(c(contact_mobile, mobile, contact_phone, phone, phone_2)), collapse = "; ")
      ),
      local_website = ifelse(
        all(is.na(c(website, contact_website, alt_website))),
        NA_character_,
        paste(na.omit(c(website, contact_website, alt_website)), collapse = "; ")
      ),
      address = if_else(
        !is.na(address),  # priority: keep pre-built address (e.g. from collapse_address)
        address,
        if_else(
          is.na(addr_street),
          NA_character_,
          trimws(paste0(
            addr_street, " ",
            if_else(!is.na(nohousenumber) & nohousenumber == "yes", "w/n", coalesce(addr_housenumber, "")),
            ", ",
            paste0(coalesce(addr_postcode, ""), " "),
            coalesce(addr_city, "")
          ))
        )
      )
    ) %>%
    ungroup() %>%
    select(-any_of(cols_to_remove))
}



# Email functions ----
#  """""""""""""""""" ----------------------
# Get access token (client credentials flow) ----
get_office365_email_token <- function(tenant_id, client_id, client_secret) {
  url <- sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", tenant_id)
  
  resp <- request(url) |>
    req_body_form(
      grant_type    = "client_credentials",
      client_id     = client_id,
      client_secret = client_secret,
      scope         = "https://graph.microsoft.com/.default"
    ) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
  
  body <- resp |> resp_body_json()
  
  if (!is.null(body$error)) {
    stop(sprintf("Token error [%s]: %s", body$error, body$error_description))
  }
  
  message("✓ Access token obtained (expires in ", body$expires_in, "s)")
  body$access_token
}

# Paginated Graph API GET when accessing inbox ----

## add simple syntax for doing a unless it is missing
`%||%` <- function(a, b) if (!is.null(a)) a else b

graph_get <- function(token, url, ...) {
  all_values <- list()
  
  repeat {
    resp <- request(url) |>
      req_headers(Authorization = paste("Bearer", token)) |>
      req_url_query(...) |>
      req_error(is_error = \(r) FALSE) |>
      req_perform()
    
    if (resp_status(resp) >= 400) {
      body <- resp |> resp_body_json()
      stop(sprintf("Graph API error %s: %s",
                   resp_status(resp),
                   body$error$message %||% "unknown"))
    }
    
    body <- resp |> resp_body_json()
    all_values <- c(all_values, body$value)
    
    # Follow @odata.nextLink for pagination
    next_link <- body[["@odata.nextLink"]]
    if (is.null(next_link)) break
    url <- next_link
  }
  
  all_values
}

# List mail folders ----
email_list_folders <- function(token, mailbox) {
  url <- sprintf(
    "https://graph.microsoft.com/v1.0/users/%s/mailFolders",
    utils::URLencode(mailbox, reserved = TRUE)
  )
  folders <- graph_get(token, url, `$top` = 50)
  
  folder_df <- data.frame(
    id          = sapply(folders, `[[`, "id"),
    displayName = sapply(folders, `[[`, "displayName"),
    totalItems  = sapply(folders, \(f) f$totalItemCount %||% NA_integer_),
    unreadItems = sapply(folders, \(f) f$unreadItemCount %||% NA_integer_),
    stringsAsFactors = FALSE
  )
  folder_df
}


# Send an email ----
send_an_email <- function(token, from, to, subject, body) {
  url <- sprintf(
    "https://graph.microsoft.com/v1.0/users/%s/sendMail",
    utils::URLencode(from, reserved = TRUE)
  )
  
  payload <- list(
    message = list(
      subject = subject,
      body    = list(contentType = "Text", content = body),
      toRecipients = list(list(emailAddress = list(address = to)))
    )
  )
  
  resp <- request(url) |>
    req_headers(Authorization = paste("Bearer", token), `Content-Type` = "application/json") |>
    req_body_json(payload) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
  
  if (resp_status(resp) == 202) {
    message(paste0("Email sent to ", to, " about ",subject))
    return(invisible(TRUE))
  } else {
    message("Sending email failed: ", resp_body_json(resp)$error$message)
    return(invisible(FALSE))
  }
}

# Turn html into human readable text
strip_html <- function(html) {
  html %>% read_html() %>% html_text2()
}
