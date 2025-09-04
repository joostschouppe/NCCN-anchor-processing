## ---------------------------
##
## Script name: ETL flow for seveso
##
## Purpose of script: Merge data about seveso sites from non-Belgian regions
##
## Author: Joost Schouppe
##
## Date Created: 2025-05-14
##
##
## ---------------------------

# Set up environment ----
# """""""""""""""""" ----

library(httr)
library(xml2)
library(readxl)
library(tidygeocoder)

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

# Load external IDs
legend_item_id <- "70088593-ec60-42c0-952d-81968e3b273f"
data_list_id_fr<-"b3677bb3-2508-49df-ab1a-72a54590bd13"
data_list_id_lux<-"01f3d602-40a1-45e7-bc8e-afbb42f5d665"
data_list_id_sl<-"1520d582-583e-4d35-8d34-4523742512f0"
data_list_id_nrw<-"86ce1f48-7bf6-4c03-b207-36c4d68db2c3"
data_list_id_nl<-"4e2f3b1c-2d0e-4f0a-8f3a-2e5e6f0c8b1a"

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




temporary_folder <-Sys.getenv("TEMPORARY_STORAGE")
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")
offline_storage <- Sys.getenv("OFFLINE_STORAGE")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))



# EXTRACT ----
# """""""""""""""""" ----------------------


# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    
    # The Netherlands ----
    
    wfs_service <- "https://service.pdok.nl/rws/faciliteiten-voor-productie-en-industrie/productie-installaties/wfs/v1_0"
    
    
    
    # Build a resultType=hits request to get the number of features
    hits_url <- modify_url(
      wfs_service,
      query = list(
        service = "WFS",
        version = "2.0.0",
        request = "GetFeature",
        typenames = "faciliteiten-voor-productie-en-industrie:production_installation_point",
        namespaces = "xmlns(faciliteiten-voor-productie-en-industrie,http%3A%2F%2Ffaciliteiten-voor-productie-en-industrie.geonovum.nl)",
        resultType = "hits"
      )
    )
    
    res <- GET(hits_url)
    doc <- read_xml(httr::content(res, "text"))
    number_matched <- as.numeric(xml_attr(doc, "numberMatched"))
    
    cat("Number of features available:", number_matched, "\n")
    
    
    
    # Helper function to build paginated URL
    build_request_url <- function(start_index, count = 1000) {
      url <- parse_url(wfs_service)
      url$query <- list(
        service = "WFS",
        version = "2.0.0",
        request = "GetFeature",
        typenames = "faciliteiten-voor-productie-en-industrie:production_installation_point",
        namespaces = "xmlns(faciliteiten-voor-productie-en-industrie,http%3A%2F%2Ffaciliteiten-voor-productie-en-industrie.geonovum.nl)",
        srsName = "urn:ogc:def:crs:EPSG::4326",
        startIndex = start_index,
        count = count
      )
      return(build_url(url))
    }
    
    # Initialize variables
    all_features <- list()
    batch_size <- 1000
    
    # Loop over pages
    for (start_index in seq(0, number_matched - 1, by = batch_size)) {
      request_url <- build_request_url(start_index, batch_size)
      cat("Querying:", request_url, "\n")
      
      batch <- read_sf(request_url)
      all_features <- append(all_features, list(batch))
    }
    
    # Combine into one sf object
    nl_raw <- do.call(rbind, all_features)
    
    
    # Reverse the coordinates
    # Extract X (lon) and Y (lat) from geometries
    nl_select <- nl_raw %>%
      mutate(
        x = st_coordinates(geom)[,1],
        y = st_coordinates(geom)[,2]
      )
    
    nl_select<-as.data.frame(nl_select) %>%
      select(-geom)
    nl_select <- st_as_sf(
      nl_select,
      coords = c("x", "y"),
      crs = 4326
    )
   
    
    # only these appear to actually exist
    table(nl_raw$statusXlinkHref)
    nl_select <- nl_select %>%
      filter(statusXlinkHref == "https://inspire.ec.europa.eu/codelist/ConditionOfFacilityValue/functional")
    
    # plot on an osm basemap
    #library(leaflet)
    #leaflet(data = nl_select) %>%
    #  addTiles() %>%  # adds OSM as basemap
    #  addCircleMarkers(radius = 3, color = "blue")
    
    # set all column names lowercase
    colnames(nl_select) <- tolower(colnames(nl_select))
    
    nl_select <-nl_select %>%
      select(original_id=gmlid) %>%
      mutate(name="Seveso installatie", data_list_id=data_list_id_nl, risk_level=3)
    
    
    # France ----
    
    
    wfs_service <- "https://mapsref.brgm.fr/wxs/georisques/seveso"
    
    url <- parse_url(wfs_service)
    url$query <- list(
      service = "WFS",
      version = "1.1.0",  # force a WFS version
      request = "GetFeature",
      typenames = "ms:SEVESO_GE_FXX",  # maybe remove "ms:" if still error
      srsName = "EPSG:4326"
    )
    
    final_url <- build_url(url)
    
    # check
    cat(final_url)
    
    # Now try reading
    fr_raw <- read_sf(final_url)
    
    table(fr_raw$status)
    table(fr_raw$type)
    fr_select <- fr_raw %>%
      filter(status=="En exploitation avec titre") %>%
      mutate(risk_level=case_when(
        type=="Seveso seuil bas" ~ 3,
        type=="Seveso seuil haut" ~ 4,
        TRUE ~ NA_integer_
      )) %>%
      select(original_id=identifier, name, streetname, city, zip=postalcode, risk_level, geometry=msGeometry) %>%
      mutate(data_list_id=data_list_id_fr)
    
    # stop if there are NA type values
    if (any(is.na(fr_select$risk_level))) {
      stop("There are NA values in the risk level column in the French data")
    }
    
    # Reverse the coordinates
    # Extract X (lon) and Y (lat) from geometries
    coords <- st_coordinates(fr_select)
    
    # Swap X and Y
    geom_corrected <- st_sfc(lapply(seq_len(nrow(coords)), function(i) {
      st_point(c(coords[i, "Y"], coords[i, "X"]))
    }), crs = st_crs(fr_select))
    
    # Replace geometry
    fr_select <- st_set_geometry(fr_select, geom_corrected)
    
    
    
    # plot on an osm basemap
    #library(leaflet)
    #leaflet(data = fr_select) %>%
    #  addTiles() %>%  # adds OSM as basemap
    #  addCircleMarkers(radius = 3, color = "blue")
    
    
    
    # Luxemburg ----
    
    # download excel
    tempfile_path <- tempfile(fileext = ".xlsx")
    download.file(
      "https://data.public.lu/fr/datasets/r/034c7498-4d0a-4641-acf6-c4fa83321a4d",
      destfile = tempfile_path,
      mode = "wb"
    )
    lux_raw <- readxl::read_excel(tempfile_path)
    
    # make SF
    lux_raw <- lux_raw %>%
      st_as_sf(
        coords = c("WGS84 Longitude", "WGS84 Latitude"),
        crs = 4326,  # WGS84
        remove = FALSE  # Keep the original columns
      )
    
    table(lux_raw$Classification)
    
    # clean
    lux_select <- lux_raw %>%
      mutate(risk_level=case_when(
        tolower(Classification)=="seuil bas" ~ 3,
        tolower(Classification)=="seuil haut" ~ 4,
        TRUE ~ NA_integer_
      )) %>%
      mutate(zip=as.character(`Code postal`)) %>%
      select(name=Nom,streetname=Adresse, city=Localité, zip, risk_level) %>%
      mutate(original_id=as.character(row_number())) %>%
      mutate(data_list_id=data_list_id_lux)
    
    
    # stop if there are NA type values
    if (any(is.na(lux_select$risk_level))) {
      stop("There are NA values in the risk level column in the Luxemburg data")
    }
    
    # plot on an osm basemap
    #leaflet(data = lux_select) %>%
    #  addTiles() %>%  # adds OSM as basemap
    #  addCircleMarkers(radius = 3, color = "blue")
    
    
    # Saarland ----
    
    # using WFS 2.0 in QGIS, combined with View>Panels>Log messages, I was able to find the working URL below from https://geoportal.saarland.de/mapbender/php/wfs.php?INSPIRE=1&FEATURETYPE_ID=1922&REQUEST=GetCapabilities&SERVICE=WFS&VERSION=2.0.0
    # https://geoportal.saarland.de/gdi-sl/inspirewfs_Produktions_und_Industrieanlagen?service=WFS&request=GetFeature&version=2.0.0&srsName=urn:ogc:def:crs:EPSG::3857&typeNames=pf%3AProductionFacility&namespaces=xmlns(pf,http%3A%2F%2Finspire.ec.europa.eu%2Fschemas%2Fpf%2F4.0)&count=1000
    # standardized, that gives this approach
    
    wfs_service <- "https://geoportal.saarland.de/gdi-sl/inspirewfs_Produktions_und_Industrieanlagen"
    url <- parse_url(wfs_service)
    
    url$query <- list(
      INSPIRE = "1",
      SERVICE = "WFS",
      VERSION = "2.0.0",
      REQUEST = "GetFeature",  # Ensure this is correct
      TYPENAMES = "pf:ProductionFacility",  # Ensure the typename is correct
      SRSNAME = "urn:ogc:def:crs:EPSG::4326",
      namespaces="xmlns(pf,http%3A%2F%2Finspire.ec.europa.eu%2Fschemas%2Fpf%2F4.0)"
      #outputFormat = "application/gml+xml; version=3.2"  # Add output format if required
    )
    
    final_url <- build_url(url)
    
    # Try reading the data
    sl_raw <- read_sf(final_url)
    plot(sl_raw$geometry)
    
    sl_select <- sl_raw %>%
      select(original_id=localId, name) %>%
      mutate(risk_level=3) %>%
      mutate(data_list_id=data_list_id_sl)
    
    
    # NRW ----
    nrw_raw <- read_excel("C:/projects/pgn-data-airflow/data/seveso/250616_Seveso_III_NRW_testing.xlsx")
    
    
    nrw_raw <- nrw_raw %>%
      mutate(full_address = paste(Adresse, PLZ, Stadt, "Germany"))
    
    nrw_geocoded <- nrw_raw %>%
      geocode(
        address = full_address,
        method = "osm",
        lat = latitude, 
        long = longitude,
        limit = 1
      )
    
    # set all column names lowercase
    colnames(nrw_geocoded) <- tolower(colnames(nrw_geocoded))
    
    # remove cases we couldn't geocode
    nrw_geocoded <- nrw_geocoded %>%
      filter(!is.na(latitude) & !is.na(longitude))
    
    # make SF dataset based on lat and lon
    nrw_geocoded <- st_as_sf(
      nrw_geocoded,
      coords = c("longitude", "latitude"),
      crs = 4326
    )
    
    # keep only address, name, risk_level
    nrw_select <- nrw_geocoded %>%
      mutate(zip=as.character(plz)) %>%
      select(name='name des betriebsbereichs',streetname=adresse, city=stadt, zip) %>%
      mutate(original_id=as.character(row_number()), data_list_id=data_list_id_nrw, risk_level=3)
    
    
    # merge datasets ----
    
    seveso_all <- bind_rows(
      nl_select,
      fr_select,
      lux_select,
      sl_select,
      nrw_select
    )
    
    # Import to raw data ----
    CreateImportTable(dataset = seveso_all, schema = "raw_data", table_name = "seveso_international")
    
  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function



# LOAD ----
# """""""""""""""""" ----------------------

### Import to raw data ----
### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.seveso_international CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.seveso_international
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
    CONSTRAINT seveso_int_pkey PRIMARY KEY (id)
  );
",paste0("
INSERT INTO ingestion.seveso_international
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, created_at, geometry)
SELECT original_id, 
jsonb_build_object('und',name) as name,
jsonb_build_object(
	'dut', 'Sevesobedrijf',
	'fre', 'entreprise Seveso',
	'ger', 'Seveso-betriebs',
	'eng', 'Seveso company') as legend_item,
'", legend_item_id, "'::uuid AS legend_item_id,
data_list_id::uuid,
risk_level,
jsonb_build_object('address', LTRIM(CONCAT(streetname, ', ' || zip, ' ' || city),', ')) as properties,
CURRENT_DATE as created_at,
geometry
FROM raw_data.seveso_international;"),
                         "ALTER TABLE ingestion.seveso_international OWNER to pgn_group_data_team_w;",
                         "GRANT ALL ON TABLE ingestion.seveso_international TO pgn_group_data_team_w;",
                         "GRANT ALL ON TABLE ingestion.seveso_international TO pgn_user_airflow;")


### Execute the SQL commands ----
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
#overrule_checks<-TRUE


run_smart_update = function() {
  smart_update_process("seveso_international", 100, 150, 100, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}




# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}


