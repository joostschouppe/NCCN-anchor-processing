## ---------------------------
##
## Script name: Import hotels & campings
##
## Purpose of script: Load hotel& camping data from various sources & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-24
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

dl_osm <- "86b4b9cd-787b-45e1-84ed-b202996a4d95"
dl_fla <- "c82688ea-8981-4e61-b064-766d700b4766"
dl_wal <- "8286bec8-fd07-4553-890d-a41f437c6d54"

li_camping <- "537ea84c-29cb-4280-869e-9f22a4da8f8d"
li_hotel <- "e27ef6fb-7ee2-47d5-bf99-213028e727b1"
li_holiday_park <- "0212a7cf-64b5-4c76-ae15-dc0f6036af8e"

vla_official_properties_columns <- c("activity_dut", "address", "local_phone", "local_email", "local_website", "comfort_class", "number_of_units", "maximum_capacity",
                                     "normal_capacity", "capacity_tents", "number_of_short_term_camping_spots", "number_of_touristic_camping_spots",
                                     "number_of_camper_stands", "number_of_residence_units", "number_of_long_term_camping_spots",
                                     "number_of_residence_units_for_rental", "number_of_hikers_huts")    

wal_official_properties_columns <- c("activity_fre", "address", "local_phone", "local_email", "local_website", "delivery_d", "facebook")


default_osm_cleaned_columns <- c("address", "local_website", "local_phone", "local_email", "other_names", "operator", "operator_wikidata",
                                 "operator_type", "opening_hours", "operator_email", "operator_website", "check_date", "image", "wikidata")


osm_specific_properties <- c("rooms", "beds", "capacity_persons", "capacity_tents", "capacity_cabins", "capacity_caravans", "capacity_pitches", "brand", "comfort_class")


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



log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))

# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(rvest) # for the html reading
# other libraries loaded in utils



# EXTRACT ----
# """""""""""""""""" ----

process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    # Flanders ----
    ## Get dynamic download URL from https://linked.toerismevlaanderen.be/datasets ----
    webpage_url <- "https://linked.toerismevlaanderen.be/datasets"
    # Read page and extract links from the page
    all_links <- read_html(webpage_url) %>%
      html_nodes("a") %>%
      html_attr("href")
    # Select just the public.csv link
    csv_link <- all_links[grepl("public\\.csv$", all_links)]
    

    ## Download & filter data ----
    flanders_raw <- read.csv(paste0("https://linked.toerismevlaanderen.be",csv_link), sep =";")
    # limit categories
    flanders <- flanders_raw %>%
      filter(discriminator %in% c("CAMPING", "HOSTEL", "HOTEL", "HOLIDAY_PARK"))
    
    # remove objects without lat-long (should be very rare)
    flanders <- flanders %>%
      filter(!is.na(lat) & !is.na(long))
    
    # detect spatial duplicates (just for reviewing)
    flanders_dup <- flanders %>%
      group_by(x, y) %>%
      mutate(frequency = n()) %>%
      ungroup() %>%
      filter(frequency > 1) %>%
      arrange(.,x)

    # remove real duplicates: keep only the first time a business_product_id is found
    flanders <- flanders %>%
      group_by(business_product_id) %>%
      slice(1) %>%
      ungroup()
    
    # turn into SF dataset
    flanders <- st_as_sf(flanders, coords = c("long", "lat"), crs = 4326)
    
    # standardize column names
    flanders <- flanders %>%
      rename(activity_dut=discriminator) %>%
      rename(original_id=business_product_id) %>%
      rename(capacity_tents=tent_capacity) %>%
      rename(local_website=website) %>%
      rename(local_email=email) %>%
      mutate(address=paste0(street," ",house_number,", ",postal_code," ",city_name))
    
    # simplify phone
    flanders <- flanders %>%
      rowwise() %>%
      mutate(local_phone = {
        vals <- c(phone1, phone2, phone3)
        vals <- vals[!is.na(vals) & vals != ""]
        if (length(vals) == 0) NA_character_ else paste(vals, collapse = "; ")
      }) %>%
      ungroup()
    
    # turn empty strings into nulls
    flanders <- clean_empty_strings(flanders)
    

    # Wallonia ----
    # info: https://geoportail.wallonie.be/catalogue/91721175-5f01-410c-8c78-37c1d1893ba2.html#Informations
    # license is CC BY 4.0
    library(arcgislayers) # for the Wallonia ArcGIS REST service
    wal_url_hotel <- arc_open("https://geoservices.wallonie.be/arcgis/rest/services/TOURISME/OFFRES_TOURISTIQUES/MapServer/1/")
    wal_url_camping <- arc_open("https://geoservices.wallonie.be/arcgis/rest/services/TOURISME/OFFRES_TOURISTIQUES/MapServer/5/")
    wal_url_village <- arc_open("https://geoservices.wallonie.be/arcgis/rest/services/TOURISME/OFFRES_TOURISTIQUES/MapServer/7/")
    wal_hotel_raw <- arc_select(wal_url_hotel)
    wal_camping_raw <- arc_select(wal_url_camping)
    wal_village_raw <- arc_select(wal_url_village)
    if (nrow(wal_village_raw) > 0) {stop("There are now holiday villages, please add them to the rbind command")}
    wallonia <- rbind(wal_hotel_raw,wal_camping_raw)
    
    # lower case column names
    names(wallonia) <- tolower(names(wallonia))
    
    # reproject to wgs84
    wallonia <- st_transform(wallonia, 4326)

    
    # detect spatial duplicates
    wallonia_dup <- wallonia %>%
      group_by(x,y) %>%
      mutate(frequency = n()) %>%
      ungroup() %>%
      filter(frequency > 1) %>%
      arrange(.,x)

    # standardize column names
    wallonia <- wallonia %>%
      rename(activity_fre=type_offre) %>%
      rename(delivery_d=date_modification) %>%
      rename(local_website=site_web) %>%
      rename(local_email=mail) %>%
      rename(local_phone=phone1) %>%
      rename(name=nom) %>%
      rename(original_id=code) %>%
      mutate(address=paste0(rue," ",numero,", ",code_postal," ",commune))
    
    # turn empty strings into nulls
    wallonia <- clean_empty_strings(wallonia)    
    
    # Get hotels & campings in Brussels from OSM ----
    ### OSM DOWNLOAD PARAMETERS ----
    
    # Define the list of features
    features_list <- list(or_group(
      tag_filter("tourism", "hotel"),
      tag_filter("tourism", "hostel"),
      tag_filter("tourism", "motel"),
      tag_filter("tourism", "camp_site"),
      tag_filter("tourism", "chalet")
      ))
    
    # Define extra tags to use as columns for properties
    extra_columns <- c("tourism", "rooms", "beds", "capacity:persons", "capacity:tents", "capacity:cabins", "capacity:caravans", "capacity:pitches", "brand", "stars", "building")
    # Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
    datatypes <- c("points", "mpolygon")
    
    
    ### Actual OSM download & transformation ----
    
    # download hotels and campings
    osm_all <- run_process(
      download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE),
      paste0("OSM download & processing for ", describe_features_list(features_list))
    )
    
    # remove objects already in "higher" layers nursing_homes, sports, schools and cultural centres
    ## get the relevant object ids
    con_pg <- get_con()
    remove_objects <- dbGetQuery(con_pg, paste0("WITH presel AS (SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.nursing_homes WHERE deleted_at IS NULL 
                                     UNION ALL
                                     SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.sports WHERE deleted_at IS NULL
                                     UNION ALL
                                     SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.schools WHERE deleted_at IS NULL
                                     UNION ALL
                                     SELECT DISTINCT replace(original_id, 'https://osm.org/', '') AS osm_id
                                     FROM transformation.cultural_centre WHERE deleted_at IS NULL)
                                     SELECT distinct(osm_id) AS osm_id from presel"))
    dbDisconnect(con_pg)
    ## anti-join to throw them out
    osm_all <- osm_all %>%
      anti_join(remove_objects, by = "osm_id")
    
    
    names(osm_all)[names(osm_all) == "stars"] <- "comfort_class"
    
    # Deduplicate OSM objects ----
    
    # detect objects on top of other objects in the dataset
    result <- osmspatial_deduplication(osm_all, c("tourism","rooms","beds","capacity_persons","capacity_tents","capacity_cabins","capacity_caravans","capacity_pitches","comfort_class","brand"))
	osm_simplified <- result$data
	#osm_diagnostic <- result$diagnostic
	
    # clean out chalets: this tag mean both "holiday park with chalets" or "a single individual chalet"
    osm_check_chalet <- osm_simplified %>%
      filter(tourism=="chalet")
    
    ## get the original building tag
    osm_check_chalet <- osm_check_chalet %>%
      left_join(as.data.frame(osm_all) %>% select(osm_id, building_orig=building), by = "osm_id")
    
    ## keep only things that are not buildings
    ## keep only polygon objects
    ## only keep if size > 5000 m²
    ## only keep if it has a name
    ## remove unneeded columns
    osm_check_chalet <- osm_check_chalet %>%
      filter(is.na(building_orig)) %>%
      filter(grepl("node",osm_id)==FALSE) %>%
      mutate(area=as.numeric(st_area(geometry))) %>%
      filter(area>=5000) %>%
      filter(!is.na(name)) %>%
      select(-area,-building_orig)
    
    # remove all chalets from main set and add the filtered version
    osm_simplified <- osm_simplified %>%
      filter(tourism != "chalet")
    osm_simplified <- bind_rows(osm_simplified,osm_check_chalet)
    

    
    
    
    
    # Merge to official data objects ----
    # select relevant OSM areas only, one per official source
    osm_vla <- osm_simplified %>%
      filter(language=="dut")
    
    osm_wal <- osm_simplified %>%
      filter(language=="fre" | language=="ger")
    
    # Prepare for joining
    
    flanders_31370 <- st_transform(flanders, 31370)
    wallonia_31370 <- st_transform(wallonia, 31370)
    osm_vla_31370 <- st_transform(osm_vla, 31370)
    osm_wal_31370 <- st_transform(osm_wal, 31370)
    
    
    
    # OSM standardization for use in paragon ----
    ## standardize default columns
    osm_simplified <- build_osm_derived_columns(osm_simplified)

    ## save some columns from the OSM dataset in a json structure called properties
    all_osm_properties <- c(default_osm_cleaned_columns,osm_specific_properties)
    osm_simplified <- build_json_column(
      df   = osm_simplified,
      output_col = "properties",
      cols = all_osm_properties
    )    
    
    # save cases without a name, you can save them as a geojson later
    osm_be_noname <- osm_simplified %>%
      filter(is.na(name) & !is.na(language) & tourism != 'chalet') %>%
      select(osm_id)
    
    
    # remove cases without a name
    osm_simplified <- osm_simplified %>%
      filter(!is.na(name))

    
    # Join Flanders/OSM data ----

    osm_vla_off_merged <- merge_to_external_polygons(
      attribute_features = flanders_31370,
      geom_features = osm_vla_31370,
      attr_street_col = "street",
      attr_hnr_col = "house_number",
      geom_street_col = "addr_street",
      geom_hnr_col = "addr_housenumber",
      patterns_to_remove = c("hotel","camping"),
      force_same_name = "soft"
    )

    # extract official objects without OSM partner for potential MapRoulette task
    #maproulette_vla <- osm_vla_off_merged %>%
    #  filter(is.na(osm_id)) %>%
    #  select(original_id,name,activity_dut,address,phone1,email,website,comfort_class)
      
    ## Create dataset of merged objects, where only the official attributes are interesting ----
    vla_official_attributes_source <- as.data.frame(osm_vla_off_merged) %>%
      filter(!is.na(osm_id)) %>%
      select(-geometry)
    
    # attribute-only objects should be merged on OSM-id while keeping a summary of the relevant columns
    # the attributes source needs to be just a DF and the geometry should be lost
    vla_official_attributes_source <- summarize_by_value(
      df           = vla_official_attributes_source,
      group_col    = "osm_id",
      id_col       = "original_id",
      summary_cols = c(vla_official_properties_columns,"name")
    )

    # create properties columns
    ## in official_attributes_source, the only needed columns are properties_secondary & osm_id, afterwards this can be joined to the OSM dataset
    ## attribute to be inserted within a data list id object. "name" is also a property, (hopefully) already provided on the Anchor by OSM
    ## we don't want a property original_id, so we save it as official_id first
    vla_official_attributes_source <- vla_official_attributes_source %>%
      rename(official_id=original_id)
    
    vla_official_attributes_source <- build_json_column(
      df   = vla_official_attributes_source,
      output_col = "properties_secondary",
      cols = c(vla_official_properties_columns,"name","official_id"),
      data_list_id = dl_fla
    )
    

    ## Create dataset of unmerged objects, where the official geometry is also kept ----
    vla_official_isolates <- osm_vla_off_merged %>%
      filter(is.na(osm_id))

    # the official data not linked to OSM should be merged on unique geometry 
    vla_official_isolates <- summarize_by_value(
      df           = vla_official_isolates,
      group_col    = "geometry",
      id_col       = "original_id",
      summary_cols = c(vla_official_properties_columns,"name")
    )

    # add legend item
    vla_official_isolates <- vla_official_isolates %>%
      mutate(legend_item_id = case_when(
        grepl("CAMPING", activity_dut) ~ li_camping,
        grepl("HOSTEL|HOTEL", activity_dut) ~ li_hotel,
        grepl("HOLIDAY_PARK", activity_dut) ~ li_holiday_park
      ))
    if (nrow(vla_official_isolates %>% filter(is.na(legend_item_id))) > 0) {
      stop("Some Flemish objects didn't get a legend item ID")
    }
    
    # create properties columns
    ## in official_isolates the properties info goes to properties, all attribute columns can be removed
    vla_official_isolates <- build_json_column(
      df   = vla_official_isolates,
      output_col = "properties",
      cols = vla_official_properties_columns
    )    
    
    # name as json
    vla_official_isolates <- vla_official_isolates %>%
      rename(und=name)
    vla_official_isolates <- build_json_column(
      df   = vla_official_isolates,
      output_col = "name",
      cols = "und"
    )
    
    # datalist id
    vla_official_isolates <- vla_official_isolates %>%
      mutate(data_list_id=dl_fla)
    
    # Join Wallonia/OSM data ----
  
    osm_wal_off_merged <- merge_to_external_polygons(
      attribute_features = wallonia_31370,
      geom_features = osm_wal_31370,
      attr_street_col = "rue",
      attr_hnr_col = "numero",
      geom_street_col = "addr_street",
      geom_hnr_col = "addr_housenumber",
      patterns_to_remove = c("hotel","camping"),
      force_same_name = "soft"
    )
    
    # extract official objects without OSM partner for potential MapRoulette task
    #maproulette_wal <- osm_wal_off_merged %>%
    #  filter(is.na(osm_id)) %>%
    #  select(original_id,name,activity_fre,address)
    
    ## Create dataset of merged objects, where only the official attributes are interesting ----
    wal_official_attributes_source <- as.data.frame(osm_wal_off_merged) %>%
      filter(!is.na(osm_id)) %>%
      select(-geometry)
    
    # attribute-only objects should be merged on OSM-id while keeping a summary of the relevant columns
    # the attributes source needs to be just a DF and the geometry should be lost
    wal_official_attributes_source <- summarize_by_value(
      df           = wal_official_attributes_source,
      group_col    = "osm_id",
      id_col       = "original_id",
      summary_cols = c(wal_official_properties_columns,"name")
    )
    
    # create properties columns
    ## in official_attributes_source, the only needed columns are properties_secondary & osm_id, afterwards this can be joined to the OSM dataset
    ## attribute to be inserted within a data list id object. "name" is also a property, (hopefully) already provided on the Anchor by OSM
    ## we don't want a property original_id, so we save it as official_id first
    wal_official_attributes_source <- wal_official_attributes_source %>%
      rename(official_id=original_id)
    
    wal_official_attributes_source <- build_json_column(
      df   = wal_official_attributes_source,
      output_col = "properties_secondary",
      cols = c(wal_official_properties_columns,"name","official_id"),
      data_list_id = dl_wal
    )
    
    
    ## Create dataset of unmerged objects, where the official geometry is also kept ----
    wal_official_isolates <- osm_wal_off_merged %>%
      filter(is.na(osm_id))
    
    # the official data not linked to OSM should be merged on unique geometry 
    wal_official_isolates <- summarize_by_value(
      df           = wal_official_isolates,
      group_col    = "geometry",
      id_col       = "original_id",
      summary_cols = c(wal_official_properties_columns,"name"),
      geom_col     = "geometry"
    )
    
    # Add legend item id
    wal_official_isolates <- wal_official_isolates %>%
      mutate(legend_item_id = case_when(
        grepl("Camping", activity_fre) ~ li_camping,
        grepl("Hôtel", activity_fre) ~ li_hotel
      ))
    if (nrow(wal_official_isolates %>% filter(is.na(legend_item_id))) > 0) {
      stop("Some Wallonia objects didn't get a legend item ID")
    }
    
    
    # create properties columns
    ## in official_isolates the properties info goes to properties, all attribute columns can be removed
    wal_official_isolates <- build_json_column(
      df   = wal_official_isolates,
      output_col = "properties",
      cols = wal_official_properties_columns
    )
    
    # name as json
    wal_official_isolates <- wal_official_isolates %>%
      rename(und=name)
    wal_official_isolates <- build_json_column(
      df   = wal_official_isolates,
      output_col = "name",
      cols = "und"
    )
    
    # datalist id
    wal_official_isolates <- wal_official_isolates %>%
      mutate(data_list_id=dl_wal)
    

    # bring it all together ----
    ## osm_simplified: create name, add legend item, drop language & tourism
    osm_simplified <- osm_simplified %>%
      rename(und=name, dut=name_nl, fre=name_fr, ger=name_de)
    osm_simplified <- build_json_column(
      df   = osm_simplified,
      output_col = "name",
      cols = c("und", "dut", "fre", "ger")
    )    
    
    # add legend item. Camping takes precedence over hotel/motel/hostel over chalet
    osm_simplified <- osm_simplified %>%
      mutate(legend_item_id = case_when(
        grepl("camp_site", tourism) ~ li_camping,
        grepl("hotel|hostel|motel", tourism) ~ li_hotel,
        grepl("chalet", tourism) ~li_holiday_park
    ))
    if (nrow(osm_simplified %>% filter(is.na(legend_item_id))) > 0) {
      stop("Some OSM objects didn't get a legend item ID")
    }
    
    osm_simplified <- osm_simplified %>%
      select(osm_id, name, properties, legend_item_id)
    
    
    ## merge attributes dataset
    attributes <- rbind(vla_official_attributes_source,wal_official_attributes_source)
    dataset_for_upload <- osm_simplified %>%
      left_join(attributes, by = "osm_id") %>%
      mutate(osm_id=paste0("https://osm.org/",osm_id)) %>%
      rename(original_id=osm_id) %>%
      mutate(data_list_id=dl_osm)
    
    
    dataset_for_upload$name <- as.character(dataset_for_upload$name)
    dataset_for_upload$properties <- as.character(dataset_for_upload$properties)
    dataset_for_upload$properties_secondary <- as.character(dataset_for_upload$properties_secondary)
    
    wal_official_isolates$name <- as.character(wal_official_isolates$name)
    wal_official_isolates$properties <- as.character(wal_official_isolates$properties)    
    vla_official_isolates$name <- as.character(vla_official_isolates$name)
    vla_official_isolates$properties <- as.character(vla_official_isolates$properties)    
    
    dataset_for_upload <<- bind_rows(dataset_for_upload,wal_official_isolates,vla_official_isolates)


    # uncomment to create files for upload to maproulette
    #st_write(osm_be_noname,"c:/temp/hotels_noname.geojson", delete_dsn=TRUE)
    #st_write(maproulette_vla,"c:/temp/hotels_maproulette_vla.geojson", delete_dsn=TRUE)
    #st_write(maproulette_wal,"c:/temp/hotels_maproulette_wal.geojson", delete_dsn=TRUE)
    
       
  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function


# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function



# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----


ingestion_table_sql <- c(
"DROP TABLE IF EXISTS ingestion.hotel_camping CASCADE;",
"CREATE TABLE IF NOT EXISTS ingestion.hotel_camping
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
    CONSTRAINT hotel_pkey PRIMARY KEY (id)
  );
",paste0("
INSERT INTO ingestion.hotel_camping 
(original_id, name, legend_item_id, data_list_id, risk_level, properties, properties_secondary, geometry, created_at)
SELECT
original_id, name::jsonb, legend_item_id::uuid, data_list_id::uuid, 1 as risk_level, properties::jsonb, properties_secondary::jsonb, ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM raw_data.hotels_merged;"),
"ALTER TABLE IF EXISTS ingestion.hotel_camping OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hotel_camping TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.hotel_camping TO pgn_user_airflow;"
)

### Execute the SQL commands ----
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}



# set to TRUE if you want to update the transformation table even if the checks fail. 
#overrule_checks<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("hotel_camping", 100, 200, 100, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = dataset_for_upload, schema = "raw_data", table_name = "hotels_merged")  
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}
