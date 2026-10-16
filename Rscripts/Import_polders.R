## ---------------------------
##
## Script name: Import polders and wateringen
##
## Purpose of script: Load borders of "polders" and "wateringen" & transform into proto-anchors with territories for Paragon
##
## Source: Digitaal Vlaanderen (VMM)
##
## Author: Ric Janssens
##
## Date Created: 2026-01-12
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

data_list_id_polders <- "d91ecbd5-31bd-4a2e-be70-f15860fa17b4"
data_list_id_wateringen <- "1306f853-6072-483e-a4a9-b867c440c296"
legend_item_id_polders <- "3ca9c392-3aee-4e82-b576-d5428a9872aa"
legend_item_id_wateringen <- "59b9e2ea-8016-4bf7-a848-1b8cfd86f897"


#readRenviron("C:/Code base/pgn-data-airflow/.Renviron")

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

# loaded in utils

library(lwgeom) # something stronger than st_make_valid()


# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----

if(run_status == FALSE){
  Sys.setenv(GDAL_HTTP_UNSAFESSL = "YES") 
}
# GDAL does not trust the WFS on NCCN_Intern because it does not use the Windows' cert store but its own trust chain
# switch to NCCN_HUB or force GDAL to trust the "unsafe" certificate

process_and_upload_fresh_data <- function() {
  
  # Polders (WFS)
  base_url_polders <- "https://geo.api.vlaanderen.be/Polders/wfs"
  
  result_polders <- base_url_polders %>%
    parse_url() %>%
    list_merge(query = list(service = "WFS",
                            version = "2.0.2",
                            request = "GetFeature",
                            outputFormat = "application/json",
                            typeName = "Polders:Polder",
                            srsName = "EPSG:4326")) %>%
    build_url() %>%
    read_sf(crs = 4326) %>%
    rename(NAAM = NAAMPOL) %>%
    mutate(object_type = "polder")
  
  # Wateringen (WFS)
  base_url_wateringen <- "https://geo.api.vlaanderen.be/Wateringen/wfs"
  
  result_wateringen <- base_url_wateringen %>%
    parse_url() %>%
    list_merge(query = list(service = "WFS",
                            version = "2.0.2",
                            request = "GetFeature",
                            outputFormat = "application/json",
                            typeName = "Wateringen:Watering",
                            srsName = "EPSG:4326")) %>%
    build_url() %>%
    read_sf(crs = 4326) %>%
    rename(NAAM = NAAMWAT) %>%
    mutate(object_type = "watering")
  
  # Combine datasets
  raw_data <- rbind(result_polders, result_wateringen)
  
  # Make valid
  cat(sprintf("Raw data: %d geometries are invalid", sum(!st_is_valid(raw_data$geometry))))
  
  if (any(st_is_valid(raw_data) == FALSE)){
    
    fixed <- lwgeom_make_valid(st_as_sfc(raw_data))
    
    # Recheck validity
    still_invalid <- !st_is_valid(fixed)
    
    if(sum(still_invalid) > 0){
      stop(sprintf("%d geometries are still invalid", sum(still_invalid)))
    }
    
    raw_data_valid <- raw_data
    raw_data_valid$geometry <- fixed
    
  } else { 
    
    raw_data_valid <- raw_data 
    
  }
  
  raw_data_valid <<- raw_data_valid
}


# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function


# TRANSFORM ----
# """""""""""""""""" ----

calculate_territory <- function() {
  
  print(sprintf("Calculating territory for %d updated records", nrow(updated_uuids)))
  
  # Get records for polders & wateringen that have been updated
  list_uuids <- paste0(updated_uuids$id, collapse = "','")
  con_pg <- get_con()
  polders <- dbGetQuery(con_pg, paste0("SELECT id, name, ST_AsHEXEWKB(geometry) AS geometry_wkt FROM transformation.polders WHERE id IN ('", list_uuids, "')"))
  polders$geometry_wkt <- st_as_sfc(structure(polders$geometry_wkt, class = "WKB"), EWKB = TRUE)
  polders$geometry_wkt <- st_set_crs(polders$geometry_wkt, 4326)
  sf_polders <- st_as_sf(polders)
  
  # Get municipality geometries to intersect with
  municipalities <- dbGetQuery(con_pg, "SELECT id, name, ST_AsHEXEWKB(geometry) AS geometry_wkt, properties->>'province_niscode' AS province_niscode FROM transformation.municipality")
  municipalities$geometry_wkt <- st_as_sfc(structure(municipalities$geometry_wkt, class = "WKB"), EWKB = TRUE)
  municipalities$geometry_wkt <- st_set_crs(municipalities$geometry_wkt, 4326)
  sf_municipalities <- st_as_sf(municipalities)
  dbDisconnect(con_pg)
  
  # Spatial join
  join <- st_join(sf_municipalities, sf_polders, join = st_intersects)
  join <- filter(join, !is.na(id.y))
  
  # Calculate territory for each polder/watering
  territory <- join %>%
    group_by(id.y) %>%
    mutate(territory = list(id.x)) %>%
    # Ensure lists with only 1 element are also proper arrays
    mutate(territory = ifelse(length(territory) == 1, sapply(territory, toJSON), sapply(territory, toJSON, auto_unbox = TRUE))) %>%
    # Calculate geometry_territory from intersecting municipalities
    mutate(geometry_territory = st_union(geometry_wkt)) %>%
    ungroup() %>%
    distinct(id.y, .keep_all = TRUE) %>%
    rename(id = id.y) %>%
    select(id, territory, geometry_territory)
  
  # Make valid
  st_geometry(territory) <- "geometry_territory"
  territory <- territory %>% select(- geometry_wkt)
  
  sf_use_s2(FALSE) # disable s2 geometry engine
  territory$geometry_territory <- st_make_valid(territory$geometry_territory)
  st_is_valid(territory$geometry_territory)
  
  # Adding province_niscode is not possible for polders/wateringen: they often cross provincial borders
  
  # If territory only should include municipalities that the polder/Watering covers with more than x m²
  
  # options(scipen = 999)
  # 
  # join$geometry_l <- st_transform(join$geometry, crs = st_crs(31370))
  # join$geometry_wkt_l <- st_transform(join$geometry_wkt, crs = st_crs(31370))
  # 
  # join <- join %>%
  #   rowwise %>%
  #   mutate(intersection = st_area(st_intersection(geometry_l, geometry_wkt_l))) %>%
  #   ungroup()
  
  # cut-off = intersection area > 10.000 m² ?
  
  territory <<- territory
  
}


# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.polders CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.polders
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
    CONSTRAINT polders_pkey PRIMARY KEY (id)
  );
",paste0("WITH cleaned as 
  (SELECT
    id AS original_id,
    jsonb_strip_nulls(jsonb_build_object(
      'und', CASE WHEN \"NAAM\" IS NULL THEN object_type ELSE \"NAAM\" END,
      'dut', \"NAAM\")) AS name,
    CASE WHEN object_type='polder' THEN '", legend_item_id_polders, "'::uuid
      ELSE '", legend_item_id_wateringen, "'::uuid END AS legend_item_id,
    CASE WHEN object_type='polder' THEN '", data_list_id_polders, "'::uuid
      ELSE '", data_list_id_wateringen, "'::uuid END AS data_list_id,
    LTRIM(CONCAT(\"ADRES\", ', ' || \"POSTCODE\", ' ' ||  \"GEMEENTE\"), ', ') AS address,
    \"TELEFOON\" AS local_phone,
    CASE WHEN \"FAX\" IS NULL OR \"FAX\" = ' ' THEN NULL ELSE \"FAX\" END AS local_fax,
    \"EMAIL\" AS local_email,
    object_type AS type,
    \"VOLGNR\" AS official_id,
    \"STATUS\" AS status,
    geometry
  FROM raw_data.vmm_polders_wateringen)

INSERT INTO ingestion.polders
  (original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
  original_id,
  name,
  NULL AS legend_item,
  legend_item_id,
  data_list_id,
  NULL AS risk_level,
  JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	  'address', address,
	  'local_email',local_email,
	  'local_phone',local_phone,
	  'local_fax',local_fax,
	  'type',type,
	  'official_id',official_id,
	  'status', status)) AS properties,
  ST_ReducePrecision(geometry, 1e-8) as geometry,
  CURRENT_TIMESTAMP as created_at
FROM cleaned;"),

"ALTER TABLE IF EXISTS ingestion.polders OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.polders TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.polders TO pgn_user_airflow;")

create_tmp_table_sql <- "
  CREATE TEMP TABLE tmp_update (
    id uuid,
    territory jsonb,
    geometry_territory geometry(Geometry, 4326)
  )
"

update_territory_sql <- "
  UPDATE transformation.polders p
  SET territory = t.territory,
      geometry_territory = t.geometry_territory,
      coverage_type_string_id = 'polder'
  FROM tmp_update t
  WHERE p.id = t.id
"

### Create transformation table (one time) ----

# This transformation table needs to include the columns for anchor.jurisdiction
# Write values of territory dataframe with UPDATE query using a tmp table
# Alternative would be to calculate territory directly in PostGIS instead of R

transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.polders CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.polders
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
    legend_item_id uuid,
    territory jsonb,
    geometry_territory geometry(geometry, 4326),
    coverage_type_string_id text,
    CONSTRAINT polders_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.polders
(id, original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at, legend_item_id)
SELECT id, original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at, legend_item_id FROM ingestion.polders;",
                              
"ALTER TABLE IF EXISTS transformation.polders OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE transformation.polders TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE transformation.polders TO pgn_user_airflow;"
)

### Functions to execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}

write_territory <- function(){
  # to use the tmp table, all db operations need to happen in the same session
  print("Writing geometry_territory to transformation table")
  con_pg <- get_con()
  dbExecute(con_pg, create_tmp_table_sql)
  #dbAppendTable(con_pg, "tmp_update", territory)
  st_write(territory, con_pg, "tmp_update", append = TRUE) # st_write avoids converting geometries to text
  dbExecute(con_pg, update_territory_sql)
  dbDisconnect(con_pg)
}

run_smart_update = function() {
  smart_update_process("polders", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run, reuse_ingestion_data=reuse_ingestion_data)
}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function(){
  if (!reuse_ingestion_data){
    process_and_upload_fresh_data()
    CreateImportTable(dataset = raw_data_valid, schema = "raw_data", table_name = "vmm_polders_wateringen")
    create_ingestion_table()
    #create_transformation_table()
  }
  updated_uuids <- run_smart_update()
  if (isTRUE(nrow(updated_uuids) > 0)) { 
      calculate_territory()
      write_territory()
  }
}

if(run_status){
  main_function()
}


