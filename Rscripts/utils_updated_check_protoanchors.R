# Functions related to data validation & update process ----

library(sf)
library(httr)
library(utils)
library(DBI)
library(RPostgres)
library(jsonlite)
library(dplyr)
library(tidyr)
library(uuid)


# Assumed information ----

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


# distance calculator (Hausdorff distance, or the longest shortest path between any two points of the two geometries) - this is usually NOT a good to choice to compare points to polygons!
calculate_distance <- function(geometry1, geometry2) {
  # Calculate Hausdorff distance for any geometry types
  distance <- st_distance(geometry1, geometry2, which = "Hausdorff")
  return(distance)
}

calculate_eucl_distance <- function(geometry1, geometry2) {
  distance <- st_distance(geometry1, geometry2, which = "Euclidean")
  return(distance)
}


# use hausdorff distance by default, but not if you are comparing a point to a polygon or multipolygon
calculate_distance_integrated <- function(geometry1, geometry2) {
  # Determine the geometry types
  geom1_type <- st_geometry_type(geometry1)
  geom2_type <- st_geometry_type(geometry2)
  
  # Check if one geometry is a point and the other is a polygon or multipolygon
  if ((geom1_type == "POINT" & geom2_type %in% c("POLYGON", "MULTIPOLYGON")) |
    (geom2_type == "POINT" & geom1_type %in% c("POLYGON", "MULTIPOLYGON"))) {
    # Calculate Euclidean distance
    distance <- st_distance(geometry1, geometry2, by_element = TRUE)
  } else {
    # Calculate Hausdorff distance for other cases
    distance <- st_distance(geometry1, geometry2, which = "Hausdorff")
  }
  
  return(distance)
}



# Dataset check function ----
perform_check <- function(dataset, name, geom, check_results, result_column_name=NULL) {
  n_invalid <- sum(!st_is_valid(geom))
  n <- nrow(dataset)
  n_missing_geom <- sum(is.na(geom)) + sum(st_is_empty(geom))
  bbox_area <- st_area(st_transform(st_as_sfc(st_bbox(dataset)), 31370)) / 1e6
  
  # Create a dataframe to store the results
  results_df <- data.frame(
    name = rep(name, 4),
    check = c("invalid geometry", "missing geometry", "n", "bbox size"),
    result = c(n_invalid, n_missing_geom, n, bbox_area),
    stringsAsFactors = FALSE
  )
  
  # Rename the result column if specified
  if (!is.null(result_column_name)) {
    names(results_df)[names(results_df) == "result"] <- result_column_name
  }
  
  # Append results to the existing check_results dataframe
  check_results <- rbind(check_results, results_df)
  
  return(check_results)
}

## Example

#dataset: dataframe name
#name: string to be used to name the dataset
#geom: geometry column, including the dataframe name
#check_results: dataframe to store the results
#result_column_name: name of the column to store the results (optional)


# Create empty dataframe to store check results
#check_newdata_raw <- data.frame(name = character(),check = character(), results = numeric(), stringsAsFactors = FALSE)

# Perform checks
#check_newdata_raw <- perform_check(b, "b", b$geometry, check_newdata_raw, "new data raw")



# _____________________ -----------------------------------------------------


# SMART UPDATE SCRIPT ----    

# NOTE: the function can deal with both unique ID_NUMBERs and non-unique ID_NUMBERs. It will also check if things with the same ID_NUMBER have moved a lot. We use a combination of distance and ID number to match the old and new datasets.

# Process to merge new to the old data ----

# put all of this in a function, so we can run the same code more than once





# Smart update parameters (with examples):
# Name of the table in Postgres
#pgsql_table_name<-"seveso"
# Max allowed distance for objects with the same ID to be considered the same object
#same_id_distance_threshold<-300
# Max distance to be allowed to be taken in account for "nearby" features (multiple can be left over)
#different_id_distance_raw_threshold<-5
# Threshold distance to decide a new feature is the same if there is only one nearby existing feature
#different_id_distance_unique_threshold<-5

# Identifying name for the version of the dataset that was used for this process
#source_identifier<-date_part
#source_identifier<-format(Sys.Date(), "%Y-%m-%d")
# If TRUE, the transformation table will be updated, even if the tests fail. Do this if you have verified that the changes in the data are understandable and acceptable.
#allow_update_even_if_checks_fail <- FALSE
# By default, the function will actually update the table. If you want to generate the geojson outputs without updating the transformation table, set this to TRUE.
#dry_run<-TRUE


# BEGIN SMART UPDATE FUNCTION ----
### Note: this assumes you have a table ready for use in ingestion, which has the exact same name as the transformation table
smart_update_process <- function(
    pgsql_table_name,
    same_id_distance_threshold,
    different_id_distance_raw_threshold,
    different_id_distance_unique_threshold,
    source_identifier,
    allow_update_even_if_checks_fail,
    dry_run=FALSE)
{
  
  # avoid scientific notation
  options(scipen = 999)
  # allow scientific notation again
  #options(scipen = 0)  
  
  ### Download new ingestion dataset ----
  con_pg <- get_con()
  new_ingestion <- dbGetQuery(con_pg, paste("SELECT original_id,name,legend_item,data_list_id,risk_level,properties,properties_secondary,imported_at,tags,deleted_at,updated_at,created_at,created_by,updated_by,ST_AsText(geometry) as geometry FROM ingestion.", pgsql_table_name, sep = ""))
  
  # also try for pt and pg geometry columns. We will add them to the data at the end, if needed.
  new_ingestion_extra_geom <- tryCatch({
    dbGetQuery(con_pg, paste("SELECT original_id, ST_AsText(geometry_pt) as geometry_pt, ST_AsText(geometry_pg) as geometry_pg FROM ingestion.", pgsql_table_name, sep = ""))
  }, error = function(e) {
    # Suppress the error message by returning NULL without any logging
    return(NULL)
  })
  dbDisconnect(con_pg)
  new_ingestion<-st_as_sf(new_ingestion, wkt="geometry")
  new_ingestion$geometry <- st_set_crs(new_ingestion$geometry, 4326)



  ### Download the old transformation dataset ----
  con_pg <- get_con()
  old_transformation <- dbGetQuery(con_pg, paste("SELECT id, original_id,name,legend_item,data_list_id,risk_level,properties,properties_secondary,imported_at,tags,deleted_at,updated_at,created_at,created_by,updated_by,ST_AsText(geometry) as geometry FROM transformation.", pgsql_table_name, sep = ""))
  dbDisconnect(con_pg)
  old_transformation<-st_as_sf(old_transformation, wkt="geometry")
  old_transformation$geometry <- st_set_crs(old_transformation$geometry, 4326)


  print("Downloaded data from Postgres")

#st_write(old_transformation, paste0(log_folder,"old_school_data.geojson"))
  
    
  bbox_old_transformation <- as.numeric(st_area(st_transform(st_as_sfc(st_bbox(old_transformation)), 31370)) / 1e6)
  
  
  
  ### Prepare the data ----
  # Make sure the "created_at" date is of the day of analysis
  new_ingestion$created_at <- Sys.Date()
  
  # only keep cases from the old data if they have not been deleted yet!
  old_transformation <- old_transformation %>%
    filter(is.na(deleted_at))
  # NOTE: this implies that if a data source accidentally deletes something, and we use it in an EP, that the link will never be fixed automatically. Option: if there are NEW objects, check them against previously deleted objects, and if they are the same, undelete them.
  
  
  # isolate just the old metadata
  old_metadata <- as.data.frame(old_transformation)
  old_metadata <- old_metadata %>%
    select(id, imported_at, tags, deleted_at, updated_at, created_at, created_by, updated_by )
  
  # keep a clean copy to use for the deletions (which we handle at the end)
  deletions <- old_transformation
  
  
  # UNCHANGED CASES ----
  ## in this context it means "the ID number did not change and it didn't really move"
  
  # add row number (in case original_id is not unique)
  new_ingestion <- new_ingestion %>% mutate(new_rownumber = row_number())
  
  
  # FIRST, merge on original_id (from the external dataset) ---
  
  # if original_id match, calculate distance. If not too far, keep the attributes from new and the id from old. Then remove records from both datasets, which we will afterwards use for the UPDATED CASES
  
  ## make a copy of the old, with uuid, original id and geometry
  old_limited <- old_transformation %>%
    select(id, original_id)
  old_limited <- st_set_geometry(old_limited, "geometry_old")
  
  
  ## turn into non-SF dataset to be able to join by id
  old_limited <- as.data.frame(old_limited)
  unchanged <- as.data.frame(new_ingestion)
  
  
  ## keep only cases with a match
  unchanged <- inner_join(unchanged, old_limited, by = "original_id", relationship = "many-to-many")
  
  
  
  ### Calculate distances between geometries within each row and keep all rows that have a distance below 50m ----
  
  # set CRS and reproject to Lambert72 (to have distances in meters)
  unchanged$geometry <- st_set_crs(unchanged$geometry, 4326)
  unchanged$geometry_old <- st_set_crs(unchanged$geometry_old, 4326)
  unchanged$geometry_l <- st_transform(unchanged$geometry, crs = st_crs(31370))
  unchanged$geometry_l_old <- st_transform(unchanged$geometry_old, crs = st_crs(31370))
  

  # Apply the distance calculation to the dataframe and show the time elapsed
  print(paste0("Start calculating distances between objects with same original_ID: ",format(Sys.time(), "%a %b %d %X %Y")))
  start_time <- Sys.time()
  unchanged$distance <- mapply(calculate_distance_integrated, unchanged$geometry_l, unchanged$geometry_l_old)
  time_elapsed <- Sys.time() - start_time
  print(paste0("End calculating distances, time elapsed: ", time_elapsed))
  
  
# Visualize a special case:
  #library(ggplot2)
  #library(ggspatial)
  #library(rosm)
  #library(prettymapr)
  
  #original_id_value <- "VL0156"  # Replace with your actual original_id value
  #filtered_data <- unchanged %>% filter(original_id == original_id_value)
  #filtered_data$geometry_l <- st_transform(filtered_data$geometry_l, crs = 4326)  

  # Create the plot with OSM basemap
  #ggplot() +
  #  annotation_map_tile(type = "osm") +  # Add OSM basemap
  #  geom_sf(data = filtered_data, aes(geometry = geometry_l), color = 'blue', fill = NA, lwd=2) +
  #  geom_sf(data = filtered_data, aes(geometry = geometry_old), color = 'red', fill = NA, lwd=1) +
  #  labs(title = paste("Geometries for original_id:", original_id_value),
  #       subtitle = "Blue: new geometry, Red: old geometry") +
  #  theme_minimal()
  
  
# NOTE: example situation,
  ## - case 1 has duplicate ID and have a distance of 15 m between each other
  ## - case 2 has no duplicate ID and have a distance of 30 m between each other
  ## ideally, we would create a match for case 2, and for the closest match between the objects of case 1
  ## since this is relatively rare, the script is not adapted to this situation, and we will just throw out the duplicates
  ## and in fact, if the distance is greater, it will still be merged in the next step
  
  
  # throw out objects that have moved beyond the threshold for objects with the same ID
  unchanged <- unchanged %>%
    filter(distance <= same_id_distance_threshold)
  
  # count number of times new objects were used
  unchanged <- unchanged %>%
    group_by(new_rownumber) %>%
    mutate(count_new = n()) %>%
    ungroup()
  # count how many times old objects were used
  unchanged <- unchanged %>%
    group_by(id) %>%
    mutate(count_old = n()) %>%
    ungroup()
  
  # throw out objects that are not in a one-on-one relationship
  unchanged <- unchanged %>%
    filter(count_new == 1, count_old == 1)
  
  # isolate the successfully matched unique identifiers for later filtering
  success_match <- as.data.frame(unchanged)
  success_match_old <- success_match %>%
    select(id)
  success_match_new <- success_match %>%
    select(new_rownumber)
  
  # remove unneeded columns
  unchanged <- unchanged %>%
    select(-geometry_old, -distance, -geometry_l, -geometry_l_old, -count_new, -count_old, -new_rownumber)
  
  ### Finalize UNCHANGED CASES ----
  ## Now finish unchanged to be used as the first part of the upload. These will be used to overwrite the existing cases with the same ID in the transformation table in Postgres
  # old_metadata has the metadata from the old data
  # delete the metadata in the new data
  unchanged <- unchanged %>%
    select(-imported_at, -tags, -deleted_at, -updated_at, -created_at, -created_by, -updated_by)
  
  # merge the old metadata to the new data
  unchanged <- left_join(unchanged, old_metadata, by = "id")
  # NOTE: for now, we will NOT change updated_at here, even though some objects might have minor updates. We could compare column by column to define things that were updated
  
  # add the unchanged objects to the table to be pushed to postgres
  table_to_push_to_sql <- unchanged
  
  
  print("Identified the unchanged cases")
  
  
  # UPDATED CASES ----
  # these are cases where we couldn't link based on a common identifier, but we find a similar object that is likely to represent the same feature (same type, nearby, no nearby similar features)
  
  
  # remove the successfully matched from the old and the new datasets
  old_transformation <- anti_join(old_transformation, success_match_old, by = "id")
  new_ingestion <- anti_join(new_ingestion, success_match_new, by = "new_rownumber")
  
  
  
  
  # IF OLD TRANSFORMATION AND NEW INGESTION BOTH HAVE 0 cases, there is a lot of code we can skip
  #BEGIN LARGE IF
  
  # if there's no deletions but there are new cases:
  if (nrow(old_transformation) == 0 && nrow(new_ingestion) > 0) {
    new_ingestion <- new_ingestion %>%
      select(-new_rownumber) %>%
      rowwise() %>%
      mutate(id = UUIDgenerate())
    table_to_push_to_sql <- bind_rows(table_to_push_to_sql, new_ingestion )
    print("There are no deletions, but some new cases")
  }
  # when there are deletions but no new cases:
  if (nrow(old_transformation) > 0 && nrow(new_ingestion) == 0) {
    table_to_push_to_sql <- bind_rows(table_to_push_to_sql, old_transformation %>%mutate(deleted_at = Sys.Date()))
    print("There are no new cases, but some deletions")
  }
  
  if (nrow(old_transformation) > 0 && nrow(new_ingestion) > 0) {
    print("There are both deletions and new cases (when looking purely at perfect matches between objects with same original_id), so we start looking for matches between them (maybe they are in fact updates).")
    ### find nearest features from other dataset ----
    
    # rename the geometry to something new, to avoid confusion
    new_ingestion$geometry <- st_set_crs(new_ingestion$geometry, 4326)
    new_ingestion_nogeo <- st_transform(new_ingestion, crs = st_crs(31370))
    
    new_ingestion_nogeo <- as.data.frame(new_ingestion_nogeo)
    names(new_ingestion_nogeo) <- paste0(names(new_ingestion_nogeo), "_new")
    new_ingestion_nogeo<-st_as_sf(new_ingestion_nogeo)
    
    old_transformation$geometry <- st_set_crs(old_transformation$geometry, 4326)
    old_transformation <- st_transform(old_transformation, crs = st_crs(31370))
    
    
    
    # Execute spatial join: adjust distance as necessary
    join <- st_join(new_ingestion_nogeo, old_transformation, join = st_is_within_distance, dist = different_id_distance_raw_threshold)
    join <- as.data.frame(join)
    
    old_transformation_geo <- as.data.frame(old_transformation)
    
    old_transformation_geo <- old_transformation_geo %>%
      select(id,geometry)
    
    join <- left_join(join, as.data.frame(old_transformation_geo), by = "id")
    

    # Calculate distance between potential matches
    join$distance <- mapply(calculate_distance_integrated, join$geometry_new, join$geometry)
    
    
    ### filter the likely updated ----
    # first remove the unlikely candidates (further away then the threshold to be taken in account)
    join <- join %>%
      filter(distance < different_id_distance_raw_threshold)
    
    # only then count number of matches
    join <- join %>%
      group_by(new_rownumber_new) %>% # one could also use original_id_new here instead of new_rownumber_new. The difference is when dealing with cases where you have two "new" objects with the same ID that are pretty far away, and both have one decent match from the old data. With rownumber, these duos will be correctly matched, with original_id, they would still be considered as too hard to match.
      mutate(count = n())
    
    ## TODO: do we really want to break the ID if only the name changed?
    ## TODO: add minimal thresholds on top of distance to decide to keep the same object ID
    ## TODO: if there's more than one candidate, make a considered guess instead of just throwing all of it out
    ## NOTE: consider including name distance here, to avoid considering things that changed name as the same object (which makes sense for restaurants, but maybe not for firestations?) (mind that default name replaced by real name should not break the link). For lines, the line length could be considered. For polygons the overlap area
    # minimum improvement: if there is more than one candidate, first select the ones with distance=0, and then count again
    join <- join %>%
      filter(count==1 | (count>1 & distance==0))
    
    join <- join %>%
      group_by(new_rownumber_new) %>%
      mutate(count = n())
    
    
    # if distance < threshold parameter AND count = 1: we keep the id and update the attributes, keep the old metadata but fill in "updated_at" with today
    join <- join %>%
      filter(distance < different_id_distance_unique_threshold & count==1)
    
    # now test if we try to use the same old id more than once, order them by distance and create an ordered number
    join <- join %>%
      group_by(id) %>%
      arrange(distance) %>%
      mutate(rownumber = row_number()) %>%
      mutate(count = n())
    # keep only the first case if count > 1
    join <- join %>%
      filter(rownumber==1)
    
    
    updated <- as.data.frame(join)
    updated <- updated %>%
      select(new_rownumber_new, id)
    
    new_ingestion_updated <- left_join(new_ingestion, updated, by = c("new_rownumber"="new_rownumber_new"))
    # this dataset now contains the new objects (without an id) & the updated objects (with an id)
    
    # rename geometry back to normal
    new_ingestion_updated <- st_set_geometry(new_ingestion_updated, "geometry")
    new_ingestion_updated$geometry <- st_set_crs(new_ingestion_updated$geometry, 4326)
    new_ingestion_updated <- new_ingestion_updated %>%
      select(-new_rownumber)
    
    
    ### NEW CASES separate dataset ----
    new_objects <- new_ingestion_updated %>%
      filter(is.na(id)) 
    # and give them a uuid
    new_objects <- new_objects %>%
      rowwise() %>%
      mutate(id = UUIDgenerate())
    
    
    ### finalize the UPDATED CASES ----
    # keep only the updated records. Remove the metadata and get it from the old dataset. Then set the updated_at date to today
    new_ingestion_updated <- new_ingestion_updated %>%
      filter(!is.na(id)) 
    
    new_ingestion_updated <- new_ingestion_updated %>%
      select(-imported_at, -tags, -deleted_at, -updated_at, -created_at, -created_by, -updated_by)
    
    
    # merge the old metadata to the new data
    new_ingestion_updated <- left_join(new_ingestion_updated, old_metadata, by = "id")
    
    new_ingestion_updated <- new_ingestion_updated %>%
      mutate(updated_at = Sys.Date())
    
    
    
    # Create final table ----
    # merge new, updated and unchanged datasets
    table_to_push_to_sql <- bind_rows(new_ingestion_updated, new_objects, table_to_push_to_sql)
    
    # identify the IDs in the new dataset
    used_ids_new <- as.data.frame(table_to_push_to_sql)
    used_ids_new <- used_ids_new %>%
      select(id)
    
    ### deal with DELETED CASES ----
    # refer back to "deletions" (which is just a backup of the old data we made at the start!) and keep only records that are not found anymore in the dataset
    deletions <- anti_join(deletions, used_ids_new, by = "id")
    # add the end date
    deletions <- deletions %>%
      mutate(deleted_at = Sys.Date())
    
    
    ### finalize table for Postgres ----
    table_to_push_to_sql <- bind_rows(table_to_push_to_sql, deletions)
    
    
    print("Comparison script between deleted and new cases has run.")
  } else {
    print("Comparison script between deleted and new cases did not have to run")
  }
  #end large if
  
  

  
  # add potentially existing extra geometry columns
  if (!is.null(new_ingestion_extra_geom)) {
    # make sure the geometries are recognized as such
    new_ingestion_extra_geom<-st_as_sf(new_ingestion_extra_geom, wkt="geometry_pt")
    new_ingestion_extra_geom$geometry_pt <- st_set_crs(new_ingestion_extra_geom$geometry_pt, 4326)
    new_ingestion_extra_geom<-as.data.frame(new_ingestion_extra_geom)
    new_ingestion_extra_geom<-st_as_sf(new_ingestion_extra_geom, wkt="geometry_pg")
    new_ingestion_extra_geom$geometry_pg <- st_set_crs(new_ingestion_extra_geom$geometry_pg, 4326)
    new_ingestion_extra_geom<-as.data.frame(new_ingestion_extra_geom)
    table_to_push_to_sql <- left_join(table_to_push_to_sql, new_ingestion_extra_geom, by = "original_id")  
  }
  
  # Importing JSONB is apparently an issue, so we first turn those fields into text
  table_without_jsonb<- table_to_push_to_sql
  table_without_jsonb$name <- as.character(table_without_jsonb$name)
  table_without_jsonb$legend_item <- as.character(table_without_jsonb$legend_item)
  table_without_jsonb$properties <- as.character(table_without_jsonb$properties)
  table_without_jsonb$properties_secondary <- as.character(table_without_jsonb$properties_secondary)
  table_without_jsonb$tags <- as.character(table_without_jsonb$tags)
  
  # turn into SF object, to avoid errors when uploading to Postgres
  table_without_jsonb<-st_as_sf(table_without_jsonb)
  
  # now push it to a Postgres test table
  con_pg<-get_con()
  table_id <- DBI::Id(
    schema  = "tst",
    table   = pgsql_table_name)
  table_id_t <- paste0("tst.",pgsql_table_name)
  print(paste0("Import temporary transformation table to postgres at ", table_id_t))
  dbWriteTable(con_pg, table_id, table_without_jsonb, overwrite = TRUE, row.names = FALSE )
  dbDisconnect(con_pg)
  
  
  
  # DO SOME DATA QUALITY CHECKS & WRITE A REPORT
  ### Smart update check ----
  
  # Get numbers from table_without_jsonb with total number of rows, number of deleted_at, created_at and updated_at today's date
  # Get number of cases with broken geometry or without a name
  
  table_without_jsonb_test<-table_without_jsonb
  table_without_jsonb_test$new     <- ifelse(is.na(table_without_jsonb_test$created_at), 0, ifelse(table_without_jsonb_test$created_at == Sys.Date(), 1, 0))
  table_without_jsonb_test$deleted <- ifelse(is.na(table_without_jsonb_test$deleted_at), 0, ifelse(table_without_jsonb_test$deleted_at == Sys.Date(), 1, 0))
  table_without_jsonb_test$updated <- ifelse(is.na(table_without_jsonb_test$updated_at), 0, ifelse(table_without_jsonb_test$updated_at == Sys.Date(), 1, 0))
  table_without_jsonb_test$n <- 1
  table_without_jsonb_test$noname <- ifelse(is.na(table_without_jsonb_test$name) | table_without_jsonb_test$name == "" | table_without_jsonb_test$name == '{"und": ""}' | table_without_jsonb_test$name == '{"und": }' , 1, 0)
  
# count the number of times an id was used
 duplicates <- table_without_jsonb_test %>%
    group_by(id) %>%
    mutate(count = n())  %>%
    ungroup() %>%
    filter(count > 1)
  
  
  # Create empty dataframe to store check results
  geo_check_transformation <- data.frame(name = character(),
                                         check = character(),
                                         results = numeric(),
                                         stringsAsFactors = FALSE)
  geo_check_transformation <- perform_check(table_without_jsonb_test, "new transformation table", table_without_jsonb_test$geometry, geo_check_transformation, "new table")
  
  
  
  
  
  sum_new <- sum(table_without_jsonb_test$new)
  sum_deleted <- sum(table_without_jsonb_test$deleted)
  sum_updated <- sum(table_without_jsonb_test$updated)
  sum_n <- sum(table_without_jsonb_test$n)
  sum_noname <- sum(table_without_jsonb_test$noname)
  
  
  
  geometry_subset <- subset(geo_check_transformation, check == "invalid geometry")
  sum_invalidgeo <- sum(geometry_subset$"new table")
  
  geometry_subset <- subset(geo_check_transformation, check == "missing geometry")
  sum_nogeo <- sum(geometry_subset$result)
  
  geometry_subset <- subset(geo_check_transformation, check == "bbox size")
  bbox_new <- geometry_subset$"new table"
  
  test <- bbox_new - bbox_old_transformation
  
  
  # Create a summary dataframe
  check_smart_update <- data.frame(new = sum_new, deleted = sum_deleted, updated = sum_updated, n = sum_n, invalid_geo=sum_invalidgeo, no_geo=sum_nogeo, noname=sum_noname, old_bbox=bbox_old_transformation, new_bbox=bbox_new)
  
  if (
    # Check if any of new/updated/deleted percentage exceeds 10%
    sum(check_smart_update$new) / sum(check_smart_update$n) * 100 > 10 || 
    sum(check_smart_update$deleted) / sum(check_smart_update$n) * 100 > 10 || 
    sum(check_smart_update$updated) / sum(check_smart_update$n) * 100 > 10 ||
    # Check if there are any cases without a name or with no/broken geometry 
    sum(check_smart_update$noname) > 0 || sum(check_smart_update$invalid_geo) > 0 || sum(check_smart_update$no_geo) > 0 ||
    # Check if BBOX size changed significantly
    abs(bbox_new - bbox_old_transformation)/bbox_old_transformation*100 > 10)
  { 
    check_smart_update$failed_check <- 1 
  } else {
    check_smart_update$failed_check <- 0
  }
  
  if (sum(check_smart_update$failed_check)>0) {
    print("Smart update checks failed.")
  } else {
    print("Smart update checks passed.")
  }
  
  
  # Overrule checks if user asked to do so
  if (allow_update_even_if_checks_fail == TRUE & sum(check_smart_update$failed_check)>0) {
    smart_update_checks_failed = 0
    print("Smart update checks failed, but we are allowing the update to continue anyway.")
  } else {
    smart_update_checks_failed = sum(check_smart_update$failed_check)
  }
  

  
  # Write a report ----
  filename <- paste0(log_folder,format(Sys.time(), "%Y%m%d_%H%M%S"),"_smartupdate_",pgsql_table_name,"_",source_identifier,".txt")
  
  
  # Add transformation check
  write.table(t(check_smart_update), filename, sep = "\t", quote = FALSE, row.names=TRUE, append = TRUE)
  cat("# If there is a large number of new, deleted or updated cases, the check fails (as we want to do a manual review to check if this is realistic) \n\n", file = filename, append = TRUE)
  print(paste0("Report about the transformation table update for ", pgsql_table_name, " written to ", filename))

#write to geojson for inspection (note that "unchanged" and updated cases will only show the new version)
st_write(table_without_jsonb_test, paste0(log_folder,pgsql_table_name,"_new_transf_",format(Sys.time(), "%Y%m%d_%H%M%S"),".geojson"))
  
  
  
  
  
  # prepare the actual update ----
  ### existing id's get an update of all EXCLUDED fields (because they have been updated or deleted); new id's get inserted
  # the query changes depending on the available geometries
  UpdateTransformationsSQL <- character()
  
  if (!is.null(new_ingestion_extra_geom)) {
    # query if three geometry fields are available
    UpdateTransformationsSQL <- c(
      paste0("INSERT INTO transformation.",pgsql_table_name,"
             (id, original_id, name, legend_item, data_list_id, risk_level, properties, properties_secondary, imported_at, tags, deleted_at, updated_at, created_at, created_by, updated_by, geometry,geometry_pg,geometry_pt)",
             "\n SELECT id::uuid, original_id,name::jsonb,legend_item::jsonb,data_list_id::uuid,
risk_level,properties::jsonb,properties_secondary::jsonb,imported_at,tags::jsonb,
deleted_at,updated_at,created_at,created_by::uuid,updated_by::uuid,geometry,geometry_pg,geometry_pt
FROM tst.",pgsql_table_name, 
             "\n ON CONFLICT (id) DO UPDATE
SET 
original_id = EXCLUDED.original_id,
name = EXCLUDED.name,
legend_item = EXCLUDED.legend_item,
data_list_id = EXCLUDED.data_list_id,
risk_level = EXCLUDED.risk_level,
properties = EXCLUDED.properties,
properties_secondary = EXCLUDED.properties_secondary,
imported_at = EXCLUDED.imported_at,
tags = EXCLUDED.tags,
deleted_at = EXCLUDED.deleted_at,
updated_at = EXCLUDED.updated_at,
created_at = EXCLUDED.created_at,
created_by = EXCLUDED.created_by,
updated_by = EXCLUDED.updated_by,
geometry = EXCLUDED.geometry,
geometry_pg = EXCLUDED.geometry_pg,
geometry_pt = EXCLUDED.geometry_pt;"),
      paste0("DROP TABLE IF EXISTS tst.",pgsql_table_name,";"))
    
  } else {   
    # query if only one geometry field is available    
    UpdateTransformationsSQL <- c(
      paste0("INSERT INTO transformation.",pgsql_table_name,"
             (id, original_id, name, legend_item, data_list_id, risk_level, properties, properties_secondary, imported_at, tags, deleted_at, updated_at, created_at, created_by, updated_by, geometry)",
             "\n SELECT id::uuid, original_id,name::jsonb,legend_item::jsonb,data_list_id::uuid,
risk_level,properties::jsonb,properties_secondary::jsonb,imported_at,tags::jsonb,
deleted_at,updated_at,created_at,created_by::uuid,updated_by::uuid,geometry
FROM tst.",pgsql_table_name, 
             "\n ON CONFLICT (id) DO UPDATE
SET 
original_id = EXCLUDED.original_id,
name = EXCLUDED.name,
legend_item = EXCLUDED.legend_item,
data_list_id = EXCLUDED.data_list_id,
risk_level = EXCLUDED.risk_level,
properties = EXCLUDED.properties,
properties_secondary = EXCLUDED.properties_secondary,
imported_at = EXCLUDED.imported_at,
tags = EXCLUDED.tags,
deleted_at = EXCLUDED.deleted_at,
updated_at = EXCLUDED.updated_at,
created_at = EXCLUDED.created_at,
created_by = EXCLUDED.created_by,
updated_by = EXCLUDED.updated_by,
geometry = EXCLUDED.geometry;"),
      paste0("DROP TABLE IF EXISTS tst.",pgsql_table_name,";"))
  }
  
  
  # create backup ----
  # for the time being, we create backups of the transformation table. This can be dropped in the future.
  BackupTransformationsSQL <- c(
    paste0("-- create an empty table if it does not exist yet
CREATE TABLE IF NOT EXISTS tst.backups_transformation_",pgsql_table_name," AS
SELECT *, CURRENT_TIMESTAMP AS backup_at FROM transformation.",pgsql_table_name,"
WITH NO DATA;
"),
    paste0("-- make a backup 
DO $$ 
BEGIN
  -- Check if the table is empty
  IF NOT EXISTS (SELECT 1 FROM tst.backups_transformation_",pgsql_table_name,") THEN
    -- If the table is empty, perform the initial insertion
    INSERT INTO tst.backups_transformation_",pgsql_table_name,"
    SELECT *, CURRENT_TIMESTAMP AS backup_at FROM transformation.",pgsql_table_name,";
  ELSE
    -- If the table is not empty, check the most recent backup_at date
    DECLARE
    recent_backup_date TIMESTAMP;
    BEGIN
      SELECT MAX(backup_at) INTO recent_backup_date
      FROM tst.backups_transformation_",pgsql_table_name,";
      -- Compare the most recent backup_at date with the current timestamp
      IF (CURRENT_TIMESTAMP - recent_backup_date) >= INTERVAL '3 days' THEN
        -- If the most recent date is at least three days old, perform the insertion
        INSERT INTO tst.backups_transformation_",pgsql_table_name,"
        SELECT *, CURRENT_TIMESTAMP AS backup_at FROM transformation.",pgsql_table_name,";
      END IF;
    END;
  END IF;
END $$;"),
    paste0("-- make sure there are only 10 distinct backups
DO $$
DECLARE
  distinct_dates_count INTEGER;
BEGIN
  -- Count the number of distinct dates in backup_at
  SELECT COUNT(DISTINCT backup_at) INTO distinct_dates_count
  FROM tst.backups_transformation_",pgsql_table_name,";
  IF distinct_dates_count > 10 THEN
    -- Determine the 10th most recent date
    DECLARE
    cutoff_date TIMESTAMP;
    BEGIN
      SELECT backup_at INTO cutoff_date
      FROM (
      SELECT backup_at
      FROM tst.backups_transformation_",pgsql_table_name,"
      ORDER BY backup_at DESC -- Order by descending to get the most recent dates first
      LIMIT 10 -- Select the top 10 dates
      ) AS recent_dates
      ORDER BY backup_at -- Order again in ascending to get the 10th most recent date
      LIMIT 1; -- Retrieve the 10th most recent date
      -- Delete rows older than the 10th most recent date
      DELETE FROM tst.backups_transformation_",pgsql_table_name,"
      WHERE backup_at < cutoff_date;
    END;
  END IF;
END $$;"))     
  
### Update the data list with the most recent updated_at date ----
# Note: if there is no change in the data, the updated_at date will not be updated
# create a list of the data_list_id's in table_without_jsonb with their most recent updated_at, created_at and deleted_at date
data_list_id <- as.data.frame(table_without_jsonb) %>%
  group_by(data_list_id) %>%
  summarise(updated_at = max(updated_at, na.rm = TRUE),
            created_at = max(created_at, na.rm = TRUE),
            deleted_at = max(deleted_at, na.rm = TRUE)
  )
# set updated_at to the most recent date of the three created
data_list_id$updated_at <- pmax(data_list_id$updated_at, data_list_id$created_at, data_list_id$deleted_at, na.rm = TRUE)
data_list_id <- data_list_id %>% select(data_list_id, updated_at)

# send to postgres
con_pg<-get_con()
table_id <- DBI::Id(
  schema  = "tst",
  table   = "data_list_update")
table_id_t <- "tst.data_list_update"
dbWriteTable(con_pg, table_id, data_list_id, overwrite = TRUE, row.names = FALSE )

print(paste0("Imported data_list_update to postgres at ", table_id_t))
dbDisconnect(con_pg)  

# write the update sql
UpdateDataList <- c(
  paste0("UPDATE parameterization.data_list p
  SET update_at = d.updated_at
  FROM tst.data_list_update d
  WHERE p.id = d.data_list_id::uuid;",
  "DROP TABLE tst.data_list_update;"))
  
  
# Main update function ----  
  # only run the update if the checks are passed and dry run is false. That means the tst table is kept if the checks fail
  if (smart_update_checks_failed == 0 & !dry_run) {
    con_pg <- get_con()
    tryCatch(
      {
        for (sql_command in BackupTransformationsSQL) {
          dbExecute(con_pg, sql_command)
        }
        print(paste0("Transformation table ", pgsql_table_name, " backed up without error"))
        
        # Now, proceed with the update only if backup was successful
        tryCatch(
          {
            for (sql_command in UpdateTransformationsSQL) {
              dbExecute(con_pg, sql_command)
            }
            print(paste0("Transformation table ", pgsql_table_name, " updated without error"))
            # update the data_list table if the transformation table was updated
            tryCatch(
              {
                for (sql_command in UpdateDataList) {
                  dbExecute(con_pg, sql_command)
                }
                print(paste0("Data list updated without error"))
              }, 
              error = function(err) {
                print(paste0("The SQL function to update the data list failed."))
                print(err)
              })
          },
          error = function(err) {
            print(paste0("The SQL function to update the transformation table ", pgsql_table_name, " failed"))
            print(err)  # Print the error message for more details
          }
        )
      },
      error = function(err) {
        print(paste0("The SQL function to backup the transformation table ", pgsql_table_name, " failed, so we did NOT run the table update."))
        print(err)  # Print the error message for more details
      }
    )
    dbDisconnect(con_pg)
  } else {
    if (!dry_run) {
      stop(paste0(pgsql_table_name," transformation table not updated due to failed checks. Please check the report at ",filename," for details."))
    } else {
      print(paste0("Transformation table not updated because this is just a dry run. Data comparison report available at ",filename))
    }
  }
  
}

# END SMART UPDATE FUNCTION ----

