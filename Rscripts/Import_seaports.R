## ---------------------------
##
## Script name: Import seaports
##
## Purpose of script: Load seaport data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-12-15
##
##
## ---------------------------


# NOTE: do not run this script automatically. The list of selected IDs may change. Re-make the selection of unique IDs from the new WFS download to get a similar result as before. 



# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(sf)
library(RPostgres)
library(DBI)

library(httr)
library(utils)
library(jsonlite)
library(dplyr)
library(tidyr)

# Specific packages
library(tidyverse) # a suite of packages for data wrangling, transformation, plotting, ...
library(ows4R) # interface for OGC webservices
library(gdalUtilities) # workaround to get decent geometries from the WFS data

# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

data_list_id<-"49725a4e-91b9-40ae-b0c9-ffa72b91878a"
log_folder <- "C:/temp/logs/"
local_folder <- "C:/projects/proto-anchors/raw-data/"


### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))




# EXTRACT ----
# """""""""""""""""" ----

# Load bedrijventerreinen from Flemish gov



wfs_bedrterr <- "https://geo.api.vlaanderen.be/Bedrijventerreinen/wfs"

bedrterr <- wfs_bedrterr %>% 
  parse_url() %>% 
  list_merge(query = list(service = "wfs",
                          #version = "1.1.0", # optional
                          request = "GetFeature",
                          typeName = "Bedrijventerreinen:Bedrter",
                          srsName = "EPSG:31370")) %>% 
  build_url() %>% 
  read_sf(crs = 31370)

#this should work but doesn't
#bedrterr <- bedrterr %>% st_cast(to = "MULTIPOLYGON")
#sf_prov_list <- st_cast(bedrterr, "GEOMETRYCOLLECTION")




# TRANSFORM ----
# """""""""""""""""" ----


# Workaround through GDAL to do the conversion


ensure_multipolygons <- function(X) {
  tmp1 <- tempfile(fileext = ".gpkg")
  tmp2 <- tempfile(fileext = ".gpkg")
  st_write(X, tmp1)
  ogr2ogr(tmp1, tmp2, f = "GPKG", nlt = "MULTIPOLYGON")
  Y <- st_read(tmp2)
  st_sf(st_drop_geometry(X), geom = st_geometry(Y))
}

sf_bedrterr <- ensure_multipolygons(bedrterr)




## we like our variable names in lowercase
names(sf_bedrterr) <- tolower(names(sf_bedrterr))



# Load seaport names & filter
csv_content <- 'idbedr,seaport
"3277",Oostende
"3278",Oostende
"5103",Oostende
"5104",Oostende
"3254",Oostende
"3288",Zeebrugge
"2428",Gent
"1754",Gent
"2345",Gent
"1745",Gent
"13567",Gent
"1626",Antwerpen
"2443",Antwerpen
"2584",Antwerpen
"2585",Antwerpen
"26990",Antwerpen
"16789",Antwerpen
"156200",Antwerpen
"12494",Antwerpen
"2460",Antwerpen'
selection <- read.csv(text=csv_content)



# Keep only objects that are interesting

sf_bedrterr <- merge(sf_bedrterr, selection, by.x = "idbedr", by.y = "idbedr", all = FALSE)

# Keep only relevant variables
sf_bedrterr <- sf_bedrterr %>%
  select(idbedr, naam, seaport)


# we could also add the bigger polygon but decided not to for now
#merged_sf <- sf_bedrterr %>%
#  group_by(seaport) %>%
#  summarize(
#    geometry = st_union(geometry),
#    idbedr = toString(idbedr)  # Concatenate idbedr values
#  ) %>%
#  ungroup()

#merged_sf <- merged_sf %>%
#  mutate(naam = "complete harbour")


#all_harbours <- rbind(merged_sf,sf_bedrterr)

#reproject to WGS84

sf_bedrterr <- st_transform(sf_bedrterr, crs = 4326)


# do a manual review
#st_write(old_transformation, "C:/temp/logs/old_harbours.geojson", driver = "GeoJSON")
#st_write(sf_bedrterr, "C:/temp/logs/new_harbour.geojson", driver = "GeoJSON", append=FALSE)




# LOAD ----
# """""""""""""""""" ----

# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function



### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.seaports CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.seaports
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
    CONSTRAINT seaports_pkey PRIMARY KEY (id)
  );
",paste0("
WITH 
cleaned as (SELECT
idbedr as original_id,
jsonb_strip_nulls(jsonb_build_object(
              'fre', CASE WHEN seaport='Gent' THEN CONCAT('Port de Gand (',naam,')')
						WHEN seaport='Antwerpen' THEN CONCAT('Port d''Anvers (',naam,')')
						WHEN seaport='Oostende' THEN CONCAT('Port d''Ostende (',naam,')')
						WHEN seaport='Zeebrugge' THEN CONCAT('Port de Zeebruges (',naam,')')
						ELSE NULL END,
              'ger', CASE WHEN seaport='Gent' THEN CONCAT('Hafen von Gent (',naam,')')
						WHEN seaport='Antwerpen' THEN CONCAT('Hafen von Antwerpen (',naam,')')
						WHEN seaport='Oostende' THEN CONCAT('Hafen von Ostende (',naam,')')
						WHEN seaport='Zeebrugge' THEN CONCAT('Hafen von Zeebrugge (',naam,')')
						ELSE NULL END,
              'dut', CONCAT('Haven van ',seaport,' (',naam,')'))) as name,
jsonb_build_object(
	'dut', 'zeehaven',
	'fre', 'port de mer',
	'ger', 'Seehafen',
	'eng', 'seaport') 
			as legend_item,
geometry
FROM raw_data.vlaio_bedrijventerreinen_seaports)


INSERT INTO ingestion.seaports 
(original_id, name, legend_item, data_list_id, risk_level, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",data_list_id,"' as data_list_id,
2 as risk_level,
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"))


### Create SQL for transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.seaports  CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.seaports 
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
    CONSTRAINT seaports_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.seaports  
(original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT original_id, name, legend_item, data_list_id::uuid, properties, geometry, created_at FROM ingestion.seaports ;
")



### Create fdw views ----
fdw_views_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_seaports CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_seaports
AS
SELECT
id,
original_id,
name,
legend_item,
NULL::uuid as best_address_id,
NULL::uuid as capakey_id,
data_list_id,
risk_level,
properties,
properties_secondary,
imported_at,
tags,
deleted_at,
updated_at,
created_at,
created_by,
updated_by,
geometry,
st_pointonsurface(geometry) AS geometry_pt
FROM transformation.seaports;
","
GRANT SELECT ON TABLE fdw.fdw_seaports TO fdw4dev;
")


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


create_transformation_table <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in transformation_table_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("Transformation table SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the transformation table failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


create_fdw_views <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in fdw_views_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("FDW table SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the FDW views failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----

# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("seaports", 150, 150, 100, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}



main_function = function() {
  CreateImportTable(dataset = sf_bedrterr, schema = "raw_data", table_name = "vlaio_bedrijventerreinen_seaports")   
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
  #create_fdw_views()
}


if(F){
  main_function()
}


