## ---------------------------
##
## Script name: ETL flow for schools
##
## Purpose of script: Merge data about schools from several sources and transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-10-19
##
##
## ---------------------------

# DONE
## add status reports in the SQL commands
## take in account landuse=education!
## expand Flemish data to not-quite-schools
## integrate more OSM attributes
## add risk level
## add school type
## take names in account when assigning official schools to osm geometries
## added name_1==name_2 | name_1==as.character(other_names) (to be added in VLA and BRWA?)except where name was empty
## added clean_string in VLA and BRWA
## add official id to properties if the original_id is an osm_id
## upload ingestion

# TODO: 
## integrate update transformation



# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

#data_list_id<-"cannot be added centrally, since several items are processed"
log_folder <- "C:/temp/logs/"

### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))

# Set conditions -----------------------------------------------------------
distance_matched_threshold <- 50
distance_raw_threshold <- 250



# Libraries -------------------------------
# """""""""""""""""" ----------------------


library(osmdata)
library(sf)
library(httr)
library(utils)
library(DBI)
library(RPostgres)
library(jsonlite)
library(dplyr)
library(tidyr)


# extra for geocoding
library(devtools)
devtools::install_github("phacochr/phacochr")
library(phacochr)
phaco_setup_data()
phacochr::phaco_best_data_update()

# to remove the é etc special characters
library(stringi)

# to find the longest common string
library(PTXQC)

# for the str_extract_all function
library(stringr)

library(purrr)

# Local functions
clean_string <- function(x) {
  # Remove all characters except numbers, letters, hyphens, and semicolons
  gsub("[^0-9a-zA-Z;-]", "", x)
}



# Download data -----------------------------------------------------------
# """"""""""""""""""----------------------
### Vlaanderen ----

#split up to get over the default limit of 10000 objects
# extract and format the features

DownloadFlanders <- function(){
  tryCatch({
  print("Start download Flanders")
rep_vl0 <- GET("https://geo.api.vlaanderen.be/POI/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=POI&outputFormat=application/json&CQL_FILTER=CATEGORIE=%20%27Basisonderwijs%27")
rep_vl0 <- jsonlite::fromJSON(rawToChar(rep_vl0$content))$features

rep_vl1 <- GET("https://geo.api.vlaanderen.be/POI/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=POI&outputFormat=application/json&CQL_FILTER=CATEGORIE=%20%27Hoger%20onderwijs%27")
rep_vl1 <- jsonlite::fromJSON(rawToChar(rep_vl1$content))$features

rep_vl2 <- GET("https://geo.api.vlaanderen.be/POI/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=POI&outputFormat=application/json&CQL_FILTER=CATEGORIE=%20%27Secundair%20onderwijs%27")
rep_vl2 <- jsonlite::fromJSON(rawToChar(rep_vl2$content))$features

rep_vl3 <- GET("https://geo.api.vlaanderen.be/POI/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=POI&outputFormat=application/json&CQL_FILTER=CATEGORIE=%20%27Deeltijds%20kunstonderwijs%27")
rep_vl3 <- jsonlite::fromJSON(rawToChar(rep_vl3$content))$features



## create dataframes for the attributes (properties) and the geometry
vl0_props <- rep_vl0$properties
vl0_geo <- rep_vl0$geometry
vl1_props <- rep_vl1$properties
vl1_geo <- rep_vl1$geometry
vl2_props <- rep_vl2$properties
vl2_geo <- rep_vl2$geometry
vl3_props <- rep_vl3$properties
vl3_geo <- rep_vl3$geometry

## merge the properties and merge the geographies
geojson_vl <- rbind(vl0_props,vl1_props,vl2_props,vl3_props)
geo <- rbind(vl0_geo,vl1_geo,vl2_geo,vl3_geo)

## add geometries to the properties
geojson_vl$x <- sapply(geo$coordinates, "[[", 1)
geojson_vl$y <- sapply(geo$coordinates, "[[", 2)

## we like our variable names in lowercase
names(geojson_vl) <- tolower(names(geojson_vl))

}, error = function(e) {
    print(paste("Error downloading VL:", e))
  })

print("Download Flanders is done")
return(geojson_vl)

}



### French community ----


DownloadBRWA <- function(){ 
  tryCatch({
geojson_brwa <- st_read(httr::GET("https://www.odwb.be/api/explore/v2.1/catalog/datasets/signaletique-fase/exports/geojson?lang=en&timezone=Europe%2FBrussels"))
  }, 
  error = function(e) {
    print(paste("Error downloading BRWA:", e))
  })
  print("Download BRWA is done")
  return(geojson_brwa)
}




### German community  ----

DownloadGER <- function(){  
  tryCatch({
ger <- read.csv("C:/projects/proto-anchors/raw-data/schuladressen.csv", sep=";")
#lower case names
names(ger) <- tolower(names(ger))
  }, 
  error = function(e) {
  print(paste("Error downloading GER:", e))
  })
print("Download GER is done")
return(ger)
}


### OSM data  ----


# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list_1 <- list("amenity" = "university")
features_list_2 <- list("amenity" = "school")
features_list_3 <- list("amenity" = "kindergarten")
features_list_4 <- list("landuse" = "education")
features_list_5 <- list("amenity" = "college")

# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("amenity","landuse","faculty","grades","isced:level","max_age","min_age","operator:type","pedagogy","religion","school:language","language:nl","language:de","language:fr","school")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")



### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_1<-download_osm_process(features_list_1, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE )
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_2<-download_osm_process(features_list_2, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_3<-download_osm_process(features_list_3, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_4<-download_osm_process(features_list_4, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_5<-download_osm_process(features_list_5, datatypes, extra_columns, alternative_overpass_server, keep_region=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

# remove duplicates: landuse=education can also be amenity like school
osm_4 <- osm_4 %>% filter((amenity!='kindergarten' & amenity!='school' & amenity!='university' & amenity!='college') | is.na(amenity))


osm_all<-rbind(osm_1,osm_2,osm_3,osm_4,osm_5)

osm_all <- osm_all %>% filter(amenity %in% c('university','kindergarten','school','college') | landuse=='education')

osm_all <- osm_all %>%
  mutate(isced_level = ifelse((grades=='1-4' | grades=='1-6' | grades=='1-5') & is.na(isced_level), '1', isced_level)) %>%
  mutate(isced_level = ifelse((grades=='7-12' & is.na(isced_level)), '2', isced_level)) %>%
  mutate(isced_level = ifelse((grades=='0-6' & is.na(isced_level)), '0;1', isced_level)) %>%
  mutate(isced_level = ifelse(((grades=='5-13' | grades=='0-12' | grades=='1-8') & is.na(isced_level)), '1;2', isced_level))

# TODO: mutate school:FR tag?

osm_all <- osm_all %>% 
  mutate(type_kindergarten = ifelse((grepl("kindergarten", osm_all$school, ignore.case = TRUE) | amenity=='kindergarten' | grepl("0", osm_all$isced_level, ignore.case = TRUE)), 1, 0),
         type_primary = ifelse((grepl("primary", osm_all$school, ignore.case = TRUE) | grepl("1", osm_all$isced_level, ignore.case = TRUE)), 1, 0),
         type_secondary = ifelse((grepl("secondary", osm_all$school, ignore.case = TRUE)| grepl("2", osm_all$isced_level, ignore.case = TRUE) | grepl("3", osm_all$isced_level, ignore.case = TRUE)), 1, 0),
         type_higher_education = ifelse((amenity=='university' | amenity=='college'), 1, 0))


# filter points with a name
osm_points <- osm_all %>% filter(st_geometry_type(.) %in% c("POINT"))
osm_points <- osm_points %>% filter(!(is.na(name)))

# keep only one case if osm_id has duplicates (unclear how exactly duplicates arise here)
osm_points <- osm_points %>% distinct(osm_id, .keep_all = TRUE)


# filter polygons from the relevant types
osm_mpoly <- osm_all %>% filter(st_geometry_type(.) %in% c("POLYGON", "MULTIPOLYGON"))
# set all geometries as multipolygons
osm_mpoly$geometry <- st_cast(osm_mpoly$geometry, "MULTIPOLYGON")


# paste the list of columns in osm_points
osm_points_cols <- paste0(colnames(osm_points), collapse = ",")
osm_mpoly_cols <- paste0(colnames(osm_mpoly), collapse = ",")
print(osm_points_cols)
print(osm_mpoly_cols)

# Upload to Postgresql ------------------------------
# """""""""""""""""" ----------------------


# Upload VL to PGSQL ----



CreateImportTableVL<-function(dataset, schema, table_name){
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
    
    print("Column Geom")
        ## Creation of column geom ------------------------------------------------
    query <- paste("ALTER TABLE ", table_id_t, "ADD COLUMN geom geometry(Point, 31370);")
    dbExecute(con_pg, query)
    print("update geom from lat/long")
    
    ## Update geom from lat/lon columns ----------------------------------------
    query <- paste('UPDATE ', table_id_t,' SET geom = ST_SetSRID(ST_MakePoint("x", "y"), 31370);')
    dbExecute(con_pg, query)

    ## Close connection --------------------------------------------------------
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
    
  }else{
    print(paste0("Error, the geojson you wanted to import into ", table_id_t, "does not exist, try again"))
  }
}








# Upload BRWA to PGSQL ----

CreateImportTableBRWA<-function(dataset, schema, table_name){
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


# Upload GER to PGSQL ----

CreateImportTableGER<-function(dataset, schema, table_name){
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
    dbWriteTable(con_pg, table_id, ger, overwrite = TRUE, row.names = FALSE )
    
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


# Upload OSM polygons to PGSQL ----

CreateImportTableOSMpoly<-function(dataset, schema, table_name){
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
    
    print("Column Geom")
    ## Making sure geometry is well defined ------------------------------------------------
    query <- paste("ALTER TABLE ", table_id_t, "ALTER COLUMN geometry TYPE geometry(MultiPolygon, 4326);")
    dbExecute(con_pg, query)

    ## Close connection --------------------------------------------------------
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
    
  }else{
    print(paste0("Error, the geojson you wanted to import into ", table_id_t, "does not exist, try again"))
  }
}



# Upload OSM points to PGSQL ----

CreateImportTableOSMpoint<-function(dataset, schema, table_name){
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
    
    print("Column Geom")
    ## Making sure geometry is well defined ------------------------------------------------
    query <- paste("ALTER TABLE ", table_id_t, "ALTER COLUMN geometry TYPE geometry(Point, 4326);")
    dbExecute(con_pg, query)

    ## Close connection --------------------------------------------------------
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
    
  }else{
    print(paste0("Error, the geojson you wanted to import into ", table_id_t, "does not exist, try again"))
  }
}


# Geocoding ----


### BRWA ----


GeocodeAndUploadBRWA <-function(){
brwa_cfwb_odata_schools   <- phaco_geocode(data_to_geocode=t_adresse <- dbGetQuery (con_pg<-get_con(),"SELECT ogc_fid, adresse_de_l_implantation, code_postal_de_l_implantation FROM raw_data.brwa_cfwb_odata_schools") ,colonne_num_rue = "adresse_de_l_implantation",  colonne_code_postal = "code_postal_de_l_implantation")
sf::st_write(obj = brwa_cfwb_odata_schools[["data_geocoded_sf"]], dsn = con_pg<-get_con(), Id(schema="raw_data", table = "brwa_cfwb_odata_schools_gl"))
}

### German speaking area ----

GeocodeAndUploadGer <-function(){
ger_dgov_email_schools  <- phaco_geocode(data_to_geocode=t_adresse <- dbGetQuery (con_pg<-get_con(),"SELECT * FROM raw_data.ger_dgov_email_schools ") ,colonne_num_rue = "straße",  colonne_code_postal = "plz.und.ort")
sf::st_write(obj = ger_dgov_email_schools[["data_geocoded_sf"]], dsn = con_pg<-get_con(), Id(schema="raw_data", table = "ger_dgov_email_schools_g"))
}

# Data transformations ----

### OSM points SQL----



# Define and execute each SQL command separately
sql_commands_osm_points <- c(
  "DROP TABLE IF EXISTS ingestion.osm_school_point CASCADE;",
  "CREATE TABLE IF NOT EXISTS ingestion.osm_school_point (local_id uuid NOT NULL DEFAULT gen_random_uuid(), original_id text, name jsonb, legend_item jsonb, name_source text, categorie_name text, properties jsonb, properties_secondary jsonb, geometry geometry(point, 4326), created_at date, CONSTRAINT osm_school_point_pkey PRIMARY KEY (local_id));",
  "-- prepare the data for OSM school points outside of Belgium
-- spatial join of the region
WITH
join_region AS (
SELECT 
r.niscode as niscode,
osm.osm_id as id,
osm.geometry as osm_geometry
FROM raw_data.ngi_ign_region r, raw_data.osm_school_point osm
WHERE ST_Intersects(r.shape,osm.geometry) 
),

-- prepare data and filter just the data outside of Belgium
final_table AS (
  SELECT
	b.osm_id as id,
	b.name, b.name_nl, b.name_fr, b.name_de,
	CASE WHEN b.short_name IS NULL AND b.official_name IS NULL AND b.alt_name IS NULL AND b.old_name IS NULL THEN NULL 
	ELSE CONCAT_WS('; ',b.short_name, b.official_name, b.alt_name, b.old_name) END AS other_names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(b.addr_street, ' ' || CASE WHEN b.nohousenumber='yes' THEN 'w/n' ELSE b.addr_housenumber END, ', ' || CONCAT((b.addr_postcode || ' '), b.addr_city))) END
	AS address,
CASE WHEN b.contact_email IS NULL AND b.email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',b.contact_email, b.email) END AS local_email,
operator_email,
CASE WHEN b.contact_mobile IS NULL AND b.mobile IS NULL AND b.contact_phone IS NULL AND b.phone IS NULL AND b.phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',b.contact_mobile, b.mobile, b.contact_phone, b.phone, b.phone_2) END AS local_phone,
CASE WHEN b.website IS NULL AND b.contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',b.website, b.contact_website) END AS local_website,
	b.operator_website,
	j.niscode as region,
	b.type_kindergarten, b.type_primary, b.type_secondary, b.type_higher_education,
	b.geometry as geometry
FROM raw_data.osm_school_point b 
LEFT JOIN join_region j ON b.osm_id=j.id
WHERE niscode IS null
)


-- load the data
INSERT INTO ingestion.osm_school_point (original_id, name, legend_item, name_source, properties, geometry, created_at)
SELECT
		CONCAT('https://osm.org/',id) AS original_id,
jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL OR name='' THEN 'school' ELSE name END,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
	jsonb_build_object('und', 'school') AS legend_item,
	'OpenStreetMap (point data)' AS name_source,
    	jsonb_strip_nulls(jsonb_build_object(
        	'other_names', other_names,
			'local_email', local_email,
			'operator_email',operator_email,
			'local_phone',local_phone,
			'local_website', local_website,
			'operator_website', operator_website,
			'type_kindergarten',type_kindergarten,
			'type_primary',type_primary,
			'type_secondary',type_secondary,
			'type_higher_education',type_higher_education)	
    	) AS properties,
    geometry,
	CURRENT_DATE as created_at
FROM final_table;"
)



### OSM polygons SQL----

sql_commands_osm_polygons <- c(
  "-- we make a temporary table with fixed and indexed geometries
  DROP TABLE IF EXISTS tst.osm_school_polygon CASCADE;",
  "CREATE TABLE IF NOT EXISTS tst.osm_school_polygon
  AS SELECT * FROM raw_data.osm_school_polygon;",
  "
  ALTER TABLE tst.osm_school_polygon
  RENAME COLUMN geometry to original_geometry;",
  "ALTER TABLE tst.osm_school_polygon
  ADD wkb_geometry geometry(multipolygon, 4326);",
  "UPDATE tst.osm_school_polygon
  SET wkb_geometry=ST_CollectionExtract(ST_MakeValid(original_geometry),3);",
  "ALTER TABLE tst.osm_school_polygon
  DROP COLUMN original_geometry;",
  "DROP INDEX IF EXISTS tst.osm_idx_fixed_geom CASCADE;",
  "CREATE INDEX IF NOT EXISTS osm_idx_fixed_geom ON tst.osm_school_polygon USING GIST(wkb_geometry);",
  "  
  DROP TABLE IF EXISTS ingestion.osm_school_polygon CASCADE;",
  " CREATE TABLE IF NOT EXISTS ingestion.osm_school_polygon
  (
    local_id uuid NOT NULL DEFAULT gen_random_uuid(),
    original_id text,    
    name jsonb,
    legend_item jsonb,
    name_source text,
    categorie_name text,
    properties jsonb,
    geometry geometry,
    REGION text,
    addr_street text,
    addr_housenumber text,
    created_at date,
    CONSTRAINT osm_school_polygon_pkey PRIMARY KEY (local_id)
  );",
  "  
  -- some steps to create the content
  -- basic problem: some schools have schools on top of them. When one school is entirely on top of another one, we want to merge them into one object
  -- we merge all rows to all rows, keeping only the ones that have an overlapping geometry
  -- we then have a list of 'outers' with their associate 'inners'
  WITH overlapping_poly AS (
    SELECT
        out.osm_id AS outer_id,
	    CASE WHEN 
	  		((out.amenity!='kindergarten' AND out.amenity!='school' AND out.amenity!='university' AND out.amenity!='college') OR out.amenity IS NULL)
	  		AND out.landuse='education' 
	  		THEN '1' END AS landuse_indicator,
        inn.osm_id AS inner_id
    FROM
		tst.osm_school_polygon out
    JOIN
        tst.osm_school_polygon inn
    ON
        out.osm_id <> inn.osm_id
    WHERE
        ST_Contains(out.wkb_geometry, inn.wkb_geometry)
  ),
  
  
  --- we also want background info from points in the polygon
  point_in_poly AS (
    SELECT
        out.osm_id AS outer_id,
        inn.osm_id AS inner_id
    FROM
		tst.osm_school_polygon out
    JOIN
        raw_data.osm_school_point inn
    ON
        out.osm_id <> inn.osm_id
    WHERE
        ST_Contains(out.wkb_geometry, inn.geometry) 
  ),
  overlapping AS (
	  select * from overlapping_poly UNION ALL select outer_id,NULL as landuse_indicator, inner_id  from point_in_poly),
  
  -- we prepare a big attribute table from both points & polygons
  attributes AS (
  	select osm_id,name,name_nl,operator_wikidata,operator,operator_type,addr_city,addr_housenumber,addr_street,addr_postcode,short_name,alt_name,contact_email,email,website,contact_website,phone,contact_phone,opening_hours,check_date,image,wikidata,amenity,faculty,religion,name_fr,name_de,nohousenumber,old_name,official_name,phone_2,mobile,contact_mobile,alt_website,operator_email,operator_website,landuse,grades,isced_level,max_age,min_age,pedagogy,school_language,language_nl,language_de,language_fr,school,language,type_kindergarten,type_primary,type_secondary,type_higher_education,geometry, 'poly' AS source FROM raw_data.osm_school_polygon
	  UNION ALL
    select osm_id,name,name_nl,operator_wikidata,operator,operator_type,addr_city,addr_housenumber,addr_street,addr_postcode,short_name,alt_name,contact_email,email,website,contact_website,phone,contact_phone,opening_hours,check_date,image,wikidata,amenity,faculty,religion,name_fr,name_de,nohousenumber,old_name,official_name,phone_2,mobile,contact_mobile,alt_website,operator_email,operator_website,landuse,grades,isced_level,max_age,min_age,pedagogy,school_language,language_nl,language_de,language_fr,school,language,type_kindergarten,type_primary,type_secondary,type_higher_education,geometry, 'point' AS source FROM raw_data.osm_school_point
  ),
  
  -- we now enrich the entire list with the shared 'outer' identifier
  merged AS (
	  select 
             a.*,
             b.outer_id,
	  		 b.landuse_indicator
             FROM attributes a
             LEFT JOIN overlapping b
             ON a.osm_id=b.inner_id),

  -- if there is no outer, we can just keep using the current id
 finalid AS (
  select * ,
              CASE WHEN outer_id IS NOT null THEN outer_id
              ELSE osm_id
              END
              as final_id
              FROM merged
	  		  ORDER BY final_id
  ),

-- if there are 2 or more osm_id with landuse_indicator='1' with the same final_id in a group overwrite the final_id with osm_id
 count_landuse AS (select *, SUM(landuse_indicator::integer) OVER (PARTITION BY final_id) as landuse_count from finalid),
 dealt_with_landuse AS (select *,
 CASE WHEN landuse_count>1 AND landuse_indicator='1' THEN osm_id
 ELSE final_id END as aggregation_id
 from count_landuse
 --if you want to throw out 'campus' features when they also have underlying schools: WHERE (landuse_indicator IS NULL AND landuse_count IS NULL) OR NOT (landuse_count>1 AND landuse_indicator IS NULL)
 ORDER BY landuse_count DESC, aggregation_id ASC),
  
  -- before we group, we simplify into just the columns we need
  -- the complex concat makes sure all the data that could be under different tags is merged into one column, without there being separators when not needed
  -- we only keep the geometry if you don't have an outer school or if you are the outer school
 simplified AS (
    SELECT b.aggregation_id, b.name, b.name_nl, b.name_fr, b.name_de,
	  	CASE WHEN b.short_name IS NULL AND b.official_name IS NULL AND b.alt_name IS NULL AND b.old_name IS NULL THEN NULL 
	ELSE CONCAT_WS('; ',b.short_name, b.official_name, b.alt_name, b.old_name) END AS other_names,
CASE WHEN b.addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(b.addr_street, ' ' || CASE WHEN b.nohousenumber='yes' THEN 'w/n' ELSE b.addr_housenumber END, ', ' || CONCAT((b.addr_postcode || ' '), b.addr_city))) END
	AS address,
	b.addr_street AS addr_street,
	b.addr_housenumber AS addr_housenumber,
CASE WHEN b.contact_email IS NULL AND b.email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',b.contact_email, b.email) END AS local_email,
operator_email,
CASE WHEN b.contact_mobile IS NULL AND b.mobile IS NULL AND b.contact_phone IS NULL AND b.phone IS NULL AND b.phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',b.contact_mobile, b.mobile, b.contact_phone, b.phone, b.phone_2) END AS local_phone,
CASE WHEN b.website IS NULL AND b.contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',b.website, b.contact_website) END AS local_website,
	b.operator_website,
	b.type_kindergarten, b.type_primary, b.type_secondary, b.type_higher_education, b.landuse_count, b.landuse_indicator,
	  b.source,
     CASE 
	    WHEN aggregation_id=osm_id THEN geometry
    	ELSE NULL
	    END
        AS geometry
    FROM dealt_with_landuse b
	WHERE landuse_count IS NULL OR NOT (landuse_indicator IS NULL AND landuse_count>1 AND name IS NULL) 
  ),
    
  join_region AS (
    SELECT 
    r.niscode as niscode,
    osm.aggregation_id as id,
    osm.geometry as geometry
    FROM raw_data.ngi_ign_region r, simplified osm
    WHERE ST_Intersects(r.shape,osm.geometry)
  ),
  
  
  table_joined AS (
    SELECT
    s.aggregation_id as id,
    STRING_AGG(DISTINCT s.name,'; ') as name,
    STRING_AGG(DISTINCT s.name_nl,'; ') as name_nl,
    STRING_AGG(DISTINCT s.name_fr,'; ') as name_fr,
    STRING_AGG(DISTINCT s.name_de,'; ') as name_de,
    STRING_AGG(DISTINCT s.other_names,'; ') as other_names,
    STRING_AGG(DISTINCT s.address, '; ') AS address,
    STRING_AGG(DISTINCT s.local_email, '; ') AS local_email,
    STRING_AGG(DISTINCT s.operator_email, '; ') AS operator_email,
    STRING_AGG(DISTINCT s.local_phone, '; ') AS local_phone,
    STRING_AGG(DISTINCT s.local_website, '; ') AS local_website,
    STRING_AGG(DISTINCT s.operator_website, '; ') AS operator_website,
    STRING_AGG(DISTINCT s.addr_street, '; ') AS addr_street,
    STRING_AGG(DISTINCT s.addr_housenumber, '; ') AS addr_housenumber,
    max(s.type_kindergarten) AS type_kindergarten,
    max(s.type_primary) AS type_primary,
    max(s.type_secondary) AS type_secondary,
    max(s.type_higher_education) AS type_higher_education,
    MIN(j.niscode) as region,
	STRING_AGG(DISTINCT s.source, '; ') AS source,
    MIN(s.geometry) as geometry
    FROM simplified s 
    LEFT JOIN join_region j ON s.aggregation_id=j.id
    GROUP BY
    aggregation_id),
	

-- remove cases only derived from points
final_table AS (
select * from table_joined t
WHERE source!='point')

  -- include at 
    INSERT INTO ingestion.osm_school_polygon (original_id, name, legend_item, name_source, categorie_name, properties, addr_street, addr_housenumber, REGION, created_at,geometry)
  SELECT
  id AS original_id,
               jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL OR name='' THEN 'school' ELSE name END,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
  jsonb_build_object('und', 'school') AS legend_item,
  'OpenStreetMap (polygon data)' AS name_source,
  'school' as categorie_name,
  jsonb_strip_nulls(jsonb_build_object(
    'other_names',other_names,
	  'address',address,
	  'local_email',local_email,
	  'operator_email',operator_email,
	  'local_phone',local_phone,
	  'local_website',local_website,
	  'operator_website',operator_website,
	  'type_kindergarten',type_kindergarten,
	  'type_primary',type_primary,
	  'type_secondary',type_secondary,
	  'type_higher_education',type_higher_education)	
  ) AS properties,
  addr_street AS addr_street,
  addr_housenumber AS addr_housenumber,
  region AS REGION,
  CURRENT_DATE as created_at,
  geometry AS geometry
  FROM final_table;",
  
  " DROP TABLE IF EXISTS tst.osm_school_polygon CASCADE;",
  " ALTER TABLE ingestion.osm_school_polygon
  DROP COLUMN REGION;")



### BRWA transformation ----

con_pg <- get_con()
brwa <- dbGetQuery(con_pg, " SELECT 
  o.ogc_fid,
  o.ndeg_fase_de_l_etablissement, 
  o.ndeg_fase_de_l_implantation,
  o.nom_de_l_etablissement,
  o.type_d_enseignement, 
  o.niveau,
  o.numero_bce_de_l_etablissement,
  o.genre,
  o.adresse_de_l_implantation,
  o.code_postal_de_l_implantation,
  o.commune_de_l_implantation,
  o.latitude,
  o.longitude,
  ST_AsText(o.geometry) as original_geometry_wkt, --4326
  ST_AsText(g.geometry) as geocoded_geometry_wkt --31370
  FROM raw_data.brwa_cfwb_odata_schools o
  LEFT JOIN raw_data.brwa_cfwb_odata_schools_gl g ON o.ogc_fid=g.ogc_fid;")

osm_cleaned <- dbGetQuery(con_pg, "SELECT 
  original_id, name, legend_item, name_source, categorie_name, properties, addr_street, addr_housenumber, created_at, ST_AsText(geometry) as osm_geometry_wkt
  FROM ingestion.osm_school_polygon;")

dbDisconnect(con_pg)

# make sf and reproject to lambert72
osm_cleaned<-st_as_sf(osm_cleaned, wkt="osm_geometry_wkt")
osm_cleaned$osm_geometry_wkt <- st_set_crs(osm_cleaned$osm_geometry_wkt, 4326)
osm_cleaned_orig <- osm_cleaned
osm_cleaned <- st_transform(osm_cleaned, 31370)


# fix geometry issues caused by geocoding: if ndeg_fase_de_l_etablissement & ndeg_fase_de_l_implantation are identical, copy the first geocoded_geometry_wkt to all the records in the group
brwa <- brwa %>%
  group_by(ndeg_fase_de_l_etablissement, ndeg_fase_de_l_implantation) %>%
  mutate(geocoded_geometry_wkt = ifelse(n() > 1, first(geocoded_geometry_wkt), geocoded_geometry_wkt)) %>%
  ungroup()


# create brwa as sf with the original geometry
brwa_orig<- brwa %>%
  filter(original_geometry_wkt!='POINT EMPTY')
brwa_orig$geometry <- brwa_orig$original_geometry_wkt
brwa_orig <- st_as_sf(brwa_orig, wkt="geometry")
#brwa_orig <- brwa_orig %>% select(ogc_fid)
brwa_orig$geometry <- st_set_crs(brwa_orig$geometry, 4326)
brwa_orig <- st_transform(brwa_orig, 31370)
brwa_orig$source <- "original"

# create brwa as sf with the geocoded geometry
brwa_geocode <- brwa %>%
  filter(!is.na(geocoded_geometry_wkt))
brwa_geocode$geometry <- brwa_geocode$geocoded_geometry_wkt
brwa_geocode <- st_as_sf(brwa_geocode, wkt="geometry")
#brwa_geocode <- brwa_geocode %>% select(ogc_fid)
brwa_geocode$geometry <- st_set_crs(brwa_geocode$geometry, 31370)
brwa_geocode$source <- "geocoded"

# add cases from both brwa geometry versions together
brwa_all <- rbind(brwa_orig, brwa_geocode)



# join nearby objects from both datasets
join <- st_join(brwa_all, osm_cleaned, join = st_is_within_distance, dist = distance_raw_threshold)
join <- as.data.frame(join)

# add the geometry from the osm objects
osm_cleaned_geo<-osm_cleaned %>% select(original_id, osm_geometry_wkt)
osm_cleaned_geo<-as.data.frame(osm_cleaned_geo)
join <- left_join(join, osm_cleaned_geo, by = "original_id")

# Calculate distance between potential matches
#join$distance <- mapply(calculate_distance, join$osm_geometry_wkt, join$geometry)
# we use euclidian distance, as our default Hausdorf distance isn't great for point vs polygon comparisons (de facto it gives the furthest point from the nearby polygons)
join$distance <- mapply(calculate_eucl_distance, join$osm_geometry_wkt, join$geometry)

# keep a backup with all records, including the ones that do not have a link to an OSM geometry
#join_all <- join
#join<-join_all






# if there are two ogc_id with the same osm original_id, keep the one with the lowest distance
join <- join %>%
  group_by(ogc_fid,original_id) %>% 
  arrange(distance) %>%
  mutate(
    count = n(),
    within_id_number = row_number()
  ) %>%
  ungroup()
join <- join %>%
  filter(within_id_number == 1) %>%
  select(-within_id_number, -count)

# keep only the records with a link to an OSM geometry
join <- join %>% filter(!is.na(original_id))

# acceptance elements:
### 1. name similarity ----




#extract name from languaged name + extract other_names from properties
join$name_1<-tolower(join$nom_de_l_etablissement)
join$name_2 <- tolower(mapply(extract_json_value, join$name, "und"))
join$other_names <- tolower(mapply(extract_json_value, join$properties, "other_names"))
#note: extract_json_value requires loading utils.R

# remove accents
join <- join %>%
  mutate(
    name_1=stri_trans_general(name_1, "Latin-ASCII"),
    name_2=stri_trans_general(name_2, "Latin-ASCII"),
    other_names=stri_trans_general(other_names, "Latin-ASCII"),
  )

# check for identical names
join <- join %>%
  mutate(
    same_name = ifelse(name_1==name_2 | name_1==other_names, 1, 0)
    )
  



# apply to these columns:
columns_to_clean <- c("name_1", "name_2", "other_names")

# default removals
patterns_to_remove <- c("\"", "”", "“", "´")
join <- string_removal(join, columns_to_clean, patterns_to_remove)
# string_removal is a function defined in utils.R

# remove words specific to the context
patterns_to_remove <- c("college","institut","instituts","communale","fondamentale","fondamental","enseignement","provincial","specialize", "ecole","athenee","secondair","secondaire")
join <- string_removal(join, columns_to_clean, patterns_to_remove)



# remove leading and trailing whitespaces
join <- join %>%
  mutate(
    name_1 = trimws(name_1),
    name_2 = trimws(name_2),
    other_names = trimws(other_names)
  )

# find the longest common substring
join <- join %>%
  rowwise() %>%
  mutate(lcs=LCSn(c(tolower(name_1), tolower(name_2)))) %>%
  mutate(longest_common_str_length=nchar(lcs)) %>%
  mutate(other_names=ifelse(is.na(other_names), " ", other_names)) %>%
  mutate(alt_lcs=LCSn(c(tolower(name_1), tolower(other_names)))) %>%
  mutate(alt_longest_common_str_length=nchar(alt_lcs)) %>%
  mutate(longest_common_str_length=max(longest_common_str_length, alt_longest_common_str_length))


# shorten name before calculating length
patterns_to_remove <- c("maternelle", "libre", "primaire", "specialisee", "specialise", "anexee")
columns_to_clean <- c("name_1", "name_2")
join <- string_removal(join, columns_to_clean, patterns_to_remove)
join$length_name<-nchar(join$name_1)


# Decide when we consider the names to be the same (length should de significant)
join <- join %>%
  mutate(
    same_name = ifelse(
      ((longest_common_str_length > 10 | (longest_common_str_length > 4 & longest_common_str_length <= 10 & longest_common_str_length > length_name / 2)) | ((name_1==name_2 | name_1==as.character(other_names)) & tolower(mapply(extract_json_value, name, "und"))!="school") | same_name==1),
      1,
      0
    )
  )


join <- join %>%
  select(-lcs, -alt_lcs, -longest_common_str_length, -alt_longest_common_str_length, -length_name)


### 2. street similarity ----


# split street and house number
join <- join %>%
  mutate(
    house_number_1 = ifelse(grepl("\\b[0-9]+\\b", adresse_de_l_implantation), sub("^(.*\\s)(.*[0-9].*)$", "\\2", adresse_de_l_implantation), NA),
    street_1 = tolower(ifelse(grepl("\\b[0-9]+\\b", adresse_de_l_implantation), sub("^(.*\\s)(.*[0-9].*)$", "\\1", adresse_de_l_implantation), adresse_de_l_implantation))
  )

# set hnr_2
join <- join %>%
  mutate(house_number_2 = tolower(addr_housenumber))

# set hnr_t as missing if identical to street_2
join <- join %>%
  mutate(house_number_1 = ifelse(house_number_1 == street_1, NA, house_number_1))


# SIMPLIFY STREET NAMES

# remove accents
join <- join %>%
  mutate(
    street_1 = stri_trans_general(street_1, "Latin-ASCII"),
    street_2 = tolower(stri_trans_general(addr_street, "Latin-ASCII"))
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
 "steenweg","",
 "plein", "",
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
columns_to_clean <- c("street_1", "street_2")
join <- string_replacement(join, columns_to_clean, replacements)
# string_replacement is a function defined in utils.R

# remove leading and trailing whitespaces
join <- join %>%
  mutate(
    street_1 = trimws(street_1),
    street_2 = trimws(street_2)
  )


# if result is empty string, replace with NA
join <- join %>%
  mutate(
    street_1 = ifelse(street_1 == "", NA, street_1),
    street_2 = ifelse(street_2 == "", NA, street_2)
  )
# END SIMPLIFY STREET NAMES

# make sure the result is still text
join$street_1 <- as.character(join$street_1)
join$street_2 <- as.character(join$street_2)


# calculate string distance between streets in both datasets
join <- join %>%
  mutate(streetsim = stringsim(street_1,street_2))

# check if the street on one side is a substring of street on the other side
join <- join %>%
  mutate(
    condition = nchar(street_1) > 3 & nchar(street_2) > 3,
    substring_left_check = ifelse(condition, str_detect(street_2, street_1), FALSE),
    substring_right_check = ifelse(condition, str_detect(street_1, street_2), FALSE),
    streetsim = ifelse((substring_left_check | substring_right_check) & streetsim<0.9, 0.9, streetsim)
  ) %>%
  select(-condition, -substring_right_check, -substring_left_check)

join <- join %>%
  mutate(
    house_number_1 = clean_string(house_number_1),
    house_number_2 = clean_string(house_number_2)
  )
# make sure the housenumber is text
join$house_number_1 <- as.character(join$house_number_1)
join$house_number_2 <- as.character(join$house_number_2)

join <- join %>%
  mutate(hnr_sim = stringsim(as.character(house_number_1), as.character(house_number_2)))


# check if the housenumber on one side is a substring of housenumber on the other side, if the number is at least two digits
join <- join %>%
  mutate(
    condition = nchar(as.character(house_number_2)) > 1 & nchar(as.character(house_number_1)) > 1,
    substring_left_check = ifelse(condition, str_detect(as.character(house_number_2), as.character(house_number_1)), FALSE),
    substring_right_check = ifelse(condition, str_detect(as.character(house_number_1), as.character(house_number_2)), FALSE),
    hnr_sim = ifelse((substring_left_check | substring_right_check) & hnr_sim<0.9 , 0.9, hnr_sim)
  ) %>%
  select(-condition, -substring_right_check, -substring_left_check)

join <- join %>%
  mutate(
    hnr_sim = if_else(
      !is.na(as.numeric(house_number_1)) & !is.na(as.numeric(house_number_2)),
      ifelse(abs(as.numeric(house_number_1) - as.numeric(house_number_2)) < 10 & hnr_sim<0.9, 0.9, hnr_sim),
      hnr_sim
    )
  )



## end street similarity



### 3. type of school ----


# Extract the value of type_primary to a new column
join$type_kindergarten <- as.numeric(sapply(join$properties, extract_json_value, key = "type_kindergarten"))
join$type_primary <- as.numeric(sapply(join$properties, extract_json_value, key = "type_primary"))
join$type_secondary <- as.numeric(sapply(join$properties, extract_json_value, key = "type_secondary"))
join$type_tertiary <- as.numeric(sapply(join$properties, extract_json_value, key = "type_higher_education"))

join$off_kindergarten <- ifelse(grepl("Maternel", join$type_d_enseignement), 1, 0)
join$off_primary <- ifelse(grepl("Primaire", join$type_d_enseignement), 1, 0)
join$off_secondary <- ifelse(join$niveau == "Secondaire", 1, 0)
join$off_tertiary <- ifelse(join$niveau == "Supérieur", 1, 0)

# if any combination of type is both 1, set same_school_type to 1
join$same_school_type <- ifelse(join$off_kindergarten == 1 & join$type_kindergarten == "1", 1, 0) +
  ifelse(join$off_primary == 1 & join$type_primary == "1", 1, 0) +
  ifelse(join$off_secondary == 1 & join$type_secondary == "1", 1, 0) +
  ifelse(join$off_tertiary == 1 & join$type_tertiary == "1", 1, 0)


join <- join %>%
  select(-name_1,-name_2,-other_names,-house_number_1, -house_number_2, -street_1, -street_2, -type_kindergarten, -type_primary, -type_secondary, -type_tertiary, -off_kindergarten, -off_primary, -off_secondary, -off_tertiary)


### Make choices ----
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




# SELECT only if certain quality thresholds met
join <- join %>%
  filter(distance < distance_matched_threshold | address_certainty >= 0.85 | same_school_type == 1 | same_name ==1)

# order by distance
join <- join %>%
  group_by(ogc_fid) %>% 
  arrange(distance) %>%
  mutate(distance_id = row_number()) %>%
  ungroup()

# order by address certainty
join <- join %>%
  group_by(ogc_fid) %>% 
  arrange(desc(address_certainty)) %>%
  mutate(address_certainty_id = row_number()) %>%  
  ungroup()

# create scoring variable:
  # the closest object gets 1 point if it is within threshold, 0.75 if it is within 2xtreshold. Further objects get 0.65 points if they are within the distance threshold
  # the best address gets 0.75 points, other addresses with a decent score get 0.5 points
  # if the names are the same, the object gets 1 point
  # if the school types are the same, the object gets 0.5 points
join <- join %>%
  mutate(same_name=ifelse(is.na(same_name), 0, same_name)) %>%
  mutate(
    score = 
      ifelse(distance_id == 1 & distance < distance_matched_threshold, 1, 0) +
      ifelse(distance_id == 1 & distance > distance_matched_threshold & distance < 2*distance_matched_threshold, 0.75, 0) +
      ifelse(distance_id >1 & distance < distance_matched_threshold, 0.65, 0) +
      ifelse(address_certainty>0 & address_certainty_id == 1, 0.75, 0) + ifelse(address_certainty>0 & address_certainty_id > 1, 0.5, 0) +
      ifelse(same_name == 1, 1, 0) + 
      ifelse(same_school_type == 1, 0.5, 0)
  )


# keep only the cases where the score equals the max score for the ogc_fid
join <- join %>%
  group_by(ogc_fid) %>% 
  filter(score == max(score)) %>%
  ungroup()

# if there's still more than one, keep the one with the closest distance (prepare filter)
join <- join %>%
  group_by(ogc_fid) %>% 
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
  select(ogc_fid, osm_id=original_id) %>%
  distinct()

brwa_joined <- left_join(brwa, join_simplified, by = "ogc_fid")



# reproject geocoded
brwa_geocode <- brwa_joined %>%
  filter(!is.na(geocoded_geometry_wkt)) %>%
  select(ogc_fid, geocoded_geometry=geocoded_geometry_wkt)
brwa_geocode <- st_as_sf(brwa_geocode, wkt="geocoded_geometry")
brwa_geocode$geocoded_geometry <- st_set_crs(brwa_geocode$geocoded_geometry, 31370)
brwa_geocode <- st_transform(brwa_geocode, 4326)

brwa_orig<- brwa_joined %>%
  filter(original_geometry_wkt!='POINT EMPTY') %>%
  select(ogc_fid, original_geometry=original_geometry_wkt)

brwa_orig <- st_as_sf(brwa_orig, wkt="original_geometry")
brwa_orig$original_geometry <- st_set_crs(brwa_orig$original_geometry, 4326)

brwa_joined <- left_join(brwa_joined, brwa_geocode, by = "ogc_fid")
brwa_joined <- left_join(brwa_joined, brwa_orig, by = "ogc_fid")

# create a new geometry column
brwa_joined$geometry <- ifelse(!is.na(brwa_joined$geocoded_geometry_wkt), st_as_text(brwa_joined$geocoded_geometry), st_as_text(brwa_joined$original_geometry))

# select if geometry is not null
brwa_joined <- brwa_joined %>%
  filter(geometry!='POINT EMPTY') 

# aggregation ID
brwa_joined <- brwa_joined %>%
  mutate(agg_id = ifelse(!is.na(osm_id),osm_id,geometry))

# create a clean point geometry
brwa_joined<-st_as_sf(brwa_joined, wkt="geometry")



# merge osm_cleaned_orig

brwa_joined <- left_join(brwa_joined, as.data.frame(osm_cleaned_orig), by = c("osm_id"="original_id"))


# first add the type-d-enseignement numerical fields
brwa_joined$type_kindergarten <- ifelse(grepl("Maternel", brwa_joined$type_d_enseignement), 1, 0)
brwa_joined$type_primary <- ifelse(grepl("Primaire", brwa_joined$type_d_enseignement), 1, 0)
brwa_joined$type_secondary <- ifelse(brwa_joined$niveau == "Secondaire", 1, 0)
brwa_joined$type_tertiary <- ifelse(brwa_joined$niveau == "Supérieur", 1, 0)

# aggregate
brwa_summarized <- brwa_joined %>%
  group_by(agg_id) %>%
  summarize(
    ogc_fid = paste(unique(ogc_fid), collapse = '; '),
    type_d_enseignement = paste(unique(type_d_enseignement), collapse = '; '),
    original_id = paste(unique(paste0(ndeg_fase_de_l_etablissement,'_',ndeg_fase_de_l_implantation),'; '), collapse = '; '),
    numero_bce_de_l_etablissement = paste(unique(numero_bce_de_l_etablissement), collapse = '; '),
    nom_de_l_etablissement = paste(unique(nom_de_l_etablissement), collapse = '; '),
    adresse_de_l_implantation = paste(unique(adresse_de_l_implantation), collapse = '; '),
    code_postal_de_l_implantation = paste(unique(code_postal_de_l_implantation), collapse = '; '),
    commune_de_l_implantation = paste(unique(commune_de_l_implantation), collapse = '; '),
    type_kindergarten = max(type_kindergarten),
    type_primary = max(type_primary),
    type_secondary = max(type_secondary),
    type_tertiary = max(type_tertiary),
    osm_id=first(osm_id),
    name=first(name),
    properties_osm=first(properties),
    geometry_point=first(geometry),
    geometry_osm=first(osm_geometry_wkt))

# create BRWA properties

brwa_summarized <- brwa_summarized %>%
  rowwise() %>%
  mutate(off_properties = jsonlite::toJSON(
    purrr::discard(list(
      official_id = original_id,
      address = adresse_de_l_implantation,
      postcode = code_postal_de_l_implantation,
      municipality = commune_de_l_implantation,
      bce_number=numero_bce_de_l_etablissement,
      school_types = type_d_enseignement,
      name = ifelse(is.na(osm_id), NA, nom_de_l_etablissement),
      type_kindergarten = type_kindergarten,
      type_primary = type_primary,
      type_secondary = type_secondary,
      type_tertiary = type_tertiary
    ), is.na), auto_unbox = TRUE))


brwa_summarized <- as.data.frame(brwa_summarized)



# Ensure the CRS is set correctly for geometry columns if not already
st_crs(brwa_summarized$geometry_point) <- 4326
st_crs(brwa_summarized$geometry_osm) <- 4326

# Use dplyr::case_when to conditionally assign geometries while preserving sf class
brwa_summarized <- brwa_summarized %>%
  rowwise() %>%
  mutate(
    original_id = ifelse(is.na(osm_id), original_id, paste0("https://osm.org/", osm_id)),
    name = ifelse(is.na(osm_id), toJSON(list(fre = nom_de_l_etablissement), auto_unbox = TRUE), name),
    name = ifelse(name == toJSON(list(und = "school"), auto_unbox = TRUE), toJSON(list(fre = nom_de_l_etablissement), auto_unbox = TRUE), name),
    name_source = ifelse(is.na(osm_id), 'Fédération Wallonie-Bruxelles', 'OpenStreetMap + Fédération Wallonie-Bruxelles'),
    geometry = case_when(
      is.na(osm_id) ~ geometry_point,
      TRUE ~ geometry_osm
    )
  ) %>%
  ungroup()

# Remove the unused geometry columns
brwa_summarized <- brwa_summarized %>%
  select(-geometry_point, -geometry_osm)

# Ensure the resulting dataframe is an sf object
brwa_summarized <- st_as_sf(brwa_summarized)

# Check and set CRS if necessary
st_crs(brwa_summarized) <- 4326

# Verify if geometries are valid
invalid_geometries <- brwa_summarized %>%
  filter(!st_is_valid(geometry))



brwa_summarized <- brwa_summarized %>%
  select(original_id,name,properties_osm, off_properties, name_source)




# VLA transformation ----


con_pg <- get_con()
vla <- dbGetQuery(con_pg, "SELECT 
	ogc_fid,
	poid,
	naam,
	notitie,
	telefoon,
	email,
	straat,
	huisnummer,
	busnummer,
	postcode,
	gemeente,
	poitype,
	categorie,
	link1,
	link2,
        ST_AsText(geom) as off_geometry -- is already in Lambert72
FROM raw_data.vla_depov_poiservice_schools;")

dbDisconnect(con_pg)

# create vla as sf with the original geometry
vla <- st_as_sf(vla, wkt="off_geometry")
vla$off_geometry <- st_set_crs(vla$off_geometry, 31370)
#st_write(vla, "C:/temp/logs/vla.geojson", driver = "GeoJSON")





# join nearby objects from both datasets
join <- st_join(vla, osm_cleaned, join = st_is_within_distance, dist = distance_raw_threshold)
join <- as.data.frame(join)

# add the geometry from the osm objects
osm_cleaned_geo<-osm_cleaned %>% select(original_id, osm_geometry_wkt)
osm_cleaned_geo<-as.data.frame(osm_cleaned_geo)
join <- left_join(join, osm_cleaned_geo, by = "original_id")

# Calculate distance between potential matches
#join$distance <- mapply(calculate_distance, join$osm_geometry_wkt, join$geometry)
# we use euclidian distance, as our default Hausdorf distance isn't great for point vs polygon comparisons (de facto it gives the furthest point from the nearby polygons)
join$distance <- mapply(calculate_eucl_distance, join$osm_geometry_wkt, join$off_geometry)

# keep a backup with all records, including the ones that do not have a link to an OSM geometry
#join_all <- join
#join<-join_all



# keep only the records with a link to an OSM geometry
join <- join %>% filter(!is.na(original_id))

# acceptance elements:
### 1. name similarity ----

#extract name from languaged name + extract other_names from properties
join$name_1<-tolower(join$naam)
join$name_2 <- tolower(mapply(extract_json_value, join$name, "und"))
join$other_names <- tolower(mapply(extract_json_value, join$properties, "other_names"))
#note: extract_json_value requires loading utils.R

# remove accents
join <- join %>%
  mutate(
    name_1=stri_trans_general(name_1, "Latin-ASCII"),
    name_2=stri_trans_general(name_2, "Latin-ASCII"),
    other_names=stri_trans_general(other_names, "Latin-ASCII"),
  )

# check for identical names
join <- join %>%
  mutate(
    same_name = ifelse(name_1==name_2 | name_1==other_names, 1, 0)
  )




# apply to these columns:
columns_to_clean <- c("name_1", "name_2", "other_names")

# default removals
patterns_to_remove <- c("\"", "”", "“", "´")
join <- string_removal(join, columns_to_clean, patterns_to_remove)
# string_removal is a function defined in utils.R

# remove words specific to the context
patterns_to_remove <- c("college","instituut","gemeentelijke","gemeentelijk","basisschool","vrije ","lagere school","kleuterschool","onderwijs", "atheneum","middenschool","school","stedelijke","campus","college","basisonderwijs", "provinciaal", "provinciale")
join <- string_removal(join, columns_to_clean, patterns_to_remove)


# remove leading and trailing whitespaces
join <- join %>%
  mutate(
    name_1 = trimws(name_1),
    name_2 = trimws(name_2),
    other_names = trimws(other_names)
  )

# find the longest common substring
join <- join %>%
  rowwise() %>%
  mutate(lcs=LCSn(c(tolower(name_1), tolower(name_2)))) %>%
  mutate(longest_common_str_length=nchar(lcs)) %>%
  mutate(other_names=ifelse(is.na(other_names), " ", other_names)) %>%
  mutate(alt_lcs=LCSn(c(tolower(name_1), tolower(other_names)))) %>%
  mutate(alt_longest_common_str_length=nchar(alt_lcs)) %>%
  mutate(longest_common_str_length=max(longest_common_str_length, alt_longest_common_str_length))


# shorten name before calculating length
patterns_to_remove <- c("go!", "provinciaal", "voor buitengewoon")
columns_to_clean <- c("name_1", "name_2")
join <- string_removal(join, columns_to_clean, patterns_to_remove)
join$length_name<-nchar(join$name_1)





# Decide when we consider the names to be the same (length should de significant)
join <- join %>%
  mutate(
    same_name = ifelse(
      ((longest_common_str_length > 10 | (longest_common_str_length > 4 & longest_common_str_length <= 10 & longest_common_str_length > length_name / 2)) | ((name_1==name_2 | name_1==as.character(other_names)) & tolower(mapply(extract_json_value, name, "und"))!="school") | same_name==1),
      1,
      0
    )
  )

join <- join %>%
  select(-lcs, -alt_lcs, -longest_common_str_length, -alt_longest_common_str_length, -length_name)


### 2. street similarity ----


# split street and house number
join <- join %>%
  mutate(
    house_number_1 = tolower(huisnummer),
    street_1 = tolower(straat)
  )

# set hnr_2
join <- join %>%
  mutate(house_number_2 = addr_housenumber)

# set hnr_t as missing if identical to street_2
join <- join %>%
  mutate(house_number_1 = ifelse(house_number_1 == street_1, NA, house_number_1))


# SIMPLIFY STREET NAMES

# remove accents
join <- join %>%
  mutate(
    street_1 = stri_trans_general(street_1, "Latin-ASCII"),
    street_2 = tolower(stri_trans_general(addr_street, "Latin-ASCII"))
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
columns_to_clean <- c("street_1", "street_2")
join <- string_replacement(join, columns_to_clean, replacements)
# string_replacement is a function defined in utils.R

# remove leading and trailing whitespaces
join <- join %>%
  mutate(
    street_1 = trimws(street_1),
    street_2 = trimws(street_2)
  )


# if result is empty string, replace with NA
join <- join %>%
  mutate(
    street_1 = ifelse(street_1 == "", NA, street_1),
    street_2 = ifelse(street_2 == "", NA, street_2)
  )
# END SIMPLIFY STREET NAMES

# make sure the result is still text
join$street_1 <- as.character(join$street_1)
join$street_2 <- as.character(join$street_2)


# calculate string distance between streets in both datasets
join <- join %>%
  mutate(streetsim = stringsim(street_1,street_2))

# check if the street on one side is a substring of street on the other side
join <- join %>%
  mutate(
    condition = nchar(street_1) > 3 & nchar(street_2) > 3,
    substring_left_check = ifelse(condition, str_detect(street_2, street_1), FALSE),
    substring_right_check = ifelse(condition, str_detect(street_1, street_2), FALSE),
    streetsim = ifelse((substring_left_check | substring_right_check) & streetsim<0.9, 0.9, streetsim)
  ) %>%
  select(-condition, -substring_right_check, -substring_left_check)

join <- join %>%
  mutate(
    house_number_1 = clean_string(house_number_1),
    house_number_2 = clean_string(house_number_2)
  )
# make sure the housenumber is text
join$house_number_1 <- as.character(join$house_number_1)
join$house_number_2 <- as.character(join$house_number_2)

join <- join %>%
  mutate(hnr_sim = stringsim(as.character(house_number_1), as.character(house_number_2)))


# check if the housenumber on one side is a substring of housenumber on the other side, if the number is at least two digits
join <- join %>%
  mutate(
    condition = nchar(as.character(house_number_2)) > 1 & nchar(as.character(house_number_1)) > 1,
    substring_left_check = ifelse(condition, str_detect(as.character(house_number_2), as.character(house_number_1)), FALSE),
    substring_right_check = ifelse(condition, str_detect(as.character(house_number_1), as.character(house_number_2)), FALSE),
    hnr_sim = ifelse((substring_left_check | substring_right_check) & hnr_sim<0.9 , 0.9, hnr_sim)
  ) %>%
  select(-condition, -substring_right_check, -substring_left_check)

join <- join %>%
  mutate(
    hnr_sim = if_else(
      !is.na(as.numeric(house_number_1)) & !is.na(as.numeric(house_number_2)),
      ifelse(abs(as.numeric(house_number_1) - as.numeric(house_number_2)) < 10 & hnr_sim<0.9, 0.9, hnr_sim),
      hnr_sim
    )
  )



## end street similarity



### 3. type of school ----

# frequency table of poitype & categorie



# Extract the value of type_primary to a new column
join$type_kindergarten <- as.numeric(sapply(join$properties, extract_json_value, key = "type_kindergarten"))
join$type_primary <- as.numeric(sapply(join$properties, extract_json_value, key = "type_primary"))
join$type_secondary <- as.numeric(sapply(join$properties, extract_json_value, key = "type_secondary"))
join$type_tertiary <- as.numeric(sapply(join$properties, extract_json_value, key = "type_higher_education"))

join$off_kindergarten <- ifelse(grepl("kleuteronderwijs", join$poitype), 1, 0)
join$off_primary <- ifelse(grepl("lager onderwijs", join$poitype), 1, 0)
join$off_secondary <- ifelse(join$categorie == "Secundair onderwijs", 1, 0)
join$off_tertiary <- ifelse(join$categorie == "Hoger onderwijs", 1, 0)


# if any combination of type is both 1, set same_school_type to 1
join$same_school_type <- ifelse(join$off_kindergarten == 1 & join$type_kindergarten == "1", 1, 0) +
  ifelse(join$off_primary == 1 & join$type_primary == "1", 1, 0) +
  ifelse(join$off_secondary == 1 & join$type_secondary == "1", 1, 0) +
  ifelse(join$off_tertiary == 1 & join$type_tertiary == "1", 1, 0)


join <- join %>%
  select(-name_1,-name_2,-other_names,-house_number_1, -house_number_2, -street_1, -street_2, -type_kindergarten, -type_primary, -type_secondary, -type_tertiary, -off_kindergarten, -off_primary, -off_secondary, -off_tertiary)

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




### Make choices ----
# SELECT only if certain quality thresholds met
join <- join %>%
  filter(distance < distance_matched_threshold | address_certainty >= 0.85 | same_school_type == 1 | same_name ==1)

# order by distance
join <- join %>%
  group_by(ogc_fid) %>% 
  arrange(distance) %>%
  mutate(distance_id = row_number()) %>%
  ungroup()

# order by address certainty
join <- join %>%
  group_by(ogc_fid) %>% 
  arrange(desc(address_certainty)) %>%
  mutate(address_certainty_id = row_number()) %>%  
  ungroup()

# create scoring variable:
# the closest object gets 1 point if it is within threshold, 0.75 if it is within 2xtreshold. Further objects get 0.65 points if they are within the distance threshold
# the best address gets 0.75 points, other addresses with a decent score get 0.5 points
# if the names are the same, the object gets 1 point
# if the school types are the same, the object gets 0.5 points
join <- join %>%
  mutate(same_name=ifelse(is.na(same_name), 0, same_name)) %>%
  mutate(
    score = 
      ifelse(distance_id == 1 & distance < distance_matched_threshold, 1, 0) +
      ifelse(distance_id == 1 & distance > distance_matched_threshold & distance < 2*distance_matched_threshold, 0.75, 0) +
      ifelse(distance_id >1 & distance < distance_matched_threshold, 0.65, 0) +
      ifelse(address_certainty>0 & address_certainty_id == 1, 0.75, 0) + ifelse(address_certainty>0 & address_certainty_id > 1, 0.5, 0) +
      ifelse(same_name == 1, 1, 0) + 
      ifelse(same_school_type == 1, 0.5, 0)
  )



# keep only the cases where the score equals the max score for the ogc_fid
join <- join %>%
  group_by(ogc_fid) %>% 
  filter(score == max(score)) %>%
  ungroup()

# if there's still more than one, keep the one with the closest distance (prepare filter)
join <- join %>%
  group_by(ogc_fid) %>% 
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
  select(ogc_fid, osm_id=original_id) %>%
  distinct()

vla_joined <- left_join(vla, join_simplified, by = "ogc_fid")

vla_joined <- st_as_sf(vla_joined, wkt="off_geometry")
vla_joined$off_geometry <- st_set_crs(vla_joined$off_geometry, 31370)
vla_joined <- st_transform(vla_joined, 4326)


# aggregation ID
vla_joined <- vla_joined %>%
  mutate(agg_id = ifelse(!is.na(osm_id),osm_id,st_as_text(off_geometry)))


# merge osm_cleaned_orig
vla_joined <- left_join(vla_joined, as.data.frame(osm_cleaned_orig), by = c("osm_id"="original_id"))




# create the school types again

vla_joined$type_kindergarten <- ifelse(grepl("kleuteronderwijs", vla_joined$poitype), 1, 0)
vla_joined$type_primary <- ifelse(grepl("lager onderwijs", vla_joined$poitype), 1, 0)
vla_joined$type_secondary <- ifelse(vla_joined$categorie == "Secundair onderwijs", 1, 0)
vla_joined$type_tertiary <- ifelse(vla_joined$categorie == "Hoger onderwijs", 1, 0)

# prepare addresses
# Step 1: Prepare Address Data (prep_add0)
address_unique <- as.data.frame(vla_joined) %>%
  select(agg_id, straat, huisnummer) %>%
  distinct()
# Step 2: Concatenate House Numbers (prep_add1) with reframe and order by straat and huisnummer
address_unique <- address_unique %>%
  group_by(agg_id, straat) %>%
  reframe(
    address = paste0(straat, ' ', str_c(sort(huisnummer), collapse = '-'))
  )
# Step 3: Create Unique Addresses (address_unique) with alphabetical ordering
address_unique <- address_unique %>%
  group_by(agg_id) %>%
  summarize(
    address = paste(unique(address), collapse = '; ')
  )
# join to vla_joined
vla_joined <- left_join(vla_joined, address_unique, by = "agg_id")



# aggregate
vla_summarized <- vla_joined %>%
  group_by(agg_id) %>%
  summarize(
    ogc_fid = paste(unique(ogc_fid), collapse = '; '),
    postcode=first(postcode),
    municipality=first(gemeente),
    address=first(address),
    naam=paste(unique(naam), collapse = '; '),
    original_id=paste(unique(poid), collapse = '; '),
    telefoon=paste(unique(telefoon), collapse = '; '),
    email=paste(unique(email), collapse = '; '),
    poitype=paste(unique(poitype), collapse = '; '),
    link1=paste(unique(link1), collapse = '; '),
    link2=paste(unique(link2), collapse = '; '),
    type_kindergarten = max(type_kindergarten),
    type_primary = max(type_primary),
    type_secondary = max(type_secondary),
    type_tertiary = max(type_tertiary),
    osm_id=first(osm_id),
    name=first(name),
    properties_osm=first(properties),
    geometry_point=first(off_geometry),
    geometry_osm=first(osm_geometry_wkt))


# create vla properties

vla_summarized <- vla_summarized %>%
  rowwise() %>%
  mutate(off_properties = jsonlite::toJSON(
    purrr::discard(list(
      official_id = original_id,
      address = address,
      postcode = postcode,
      municipality = municipality,
      link_fiche = link1,
      link_school = link2,
      phone = telefoon,
      email = email,
      school_types = poitype,
      type_kindergarten = type_kindergarten,
      type_primary = type_primary,
      type_secondary = type_secondary,
      type_tertiary = type_tertiary
    ), is.na), auto_unbox = TRUE))



vla_summarized <- as.data.frame(vla_summarized)

# Ensure the CRS is set correctly for geometry columns if not already
st_crs(vla_summarized$geometry_point) <- 4326
st_crs(vla_summarized$geometry_osm) <- 4326

# Use dplyr::case_when to conditionally assign geometries while preserving sf class
vla_summarized <- vla_summarized %>%
  rowwise() %>%
  mutate(
    original_id = ifelse(is.na(osm_id), original_id, paste0("https://osm.org/", osm_id)),
    name = ifelse(is.na(osm_id), toJSON(list(dut = naam), auto_unbox = TRUE), name),
    name = ifelse(name == toJSON(list(und = "school"), auto_unbox = TRUE), toJSON(list(dut = naam), auto_unbox = TRUE), name),
    name_source = ifelse(is.na(osm_id), 'Departement Onderwijs en Vorming', 'OpenStreetMap + Departement Onderwijs en Vorming'),
    geometry = case_when(
      is.na(osm_id) ~ geometry_point,
      TRUE ~ geometry_osm
    )
  ) %>%
  ungroup()



# Remove the unused geometry columns
vla_summarized <- vla_summarized %>%
  select(-geometry_point, -geometry_osm)

# Ensure the resulting dataframe is an sf object
vla_summarized <- st_as_sf(vla_summarized)

# Check and set CRS if necessary
st_crs(vla_summarized) <- 4326




vla_summarized <- vla_summarized %>%
  select(original_id,name,properties_osm, off_properties, name_source)




# GER transformation ----


con_pg <- get_con()
ger <- dbGetQuery(con_pg, "SELECT 
  \"ID_address\" AS ogc_fid,
	type AS type,
	Schule as school,
	Straße as street,
	\"plz.und.ort\" as zip,
	Gemeinde as municipality,
	Schulleiter AS contact,
	\"e.mail.1\" as email,
  ST_AsText(geometry) as off_geometry -- is already in Lambert72
FROM raw_data.ger_dgov_email_schools_g;")

dbDisconnect(con_pg)


# create ger as sf with the original geometry
ger <- st_as_sf(ger, wkt="off_geometry")
ger$off_geometry <- st_set_crs(ger$off_geometry, 31370)



# join nearby objects from both datasets
join <- st_join(ger, osm_cleaned, join = st_is_within_distance, dist = distance_raw_threshold)
join <- as.data.frame(join)

# add the geometry from the osm objects
osm_cleaned_geo<-osm_cleaned %>% select(original_id, osm_geometry_wkt)
osm_cleaned_geo<-as.data.frame(osm_cleaned_geo)
join <- left_join(join, osm_cleaned_geo, by = "original_id")

# Calculate distance between potential matches
#join$distance <- mapply(calculate_distance, join$osm_geometry_wkt, join$geometry)
# we use euclidian distance, as our default Hausdorf distance isn't great for point vs polygon comparisons (de facto it gives the furthest point from the nearby polygons)
join$distance <- mapply(calculate_eucl_distance, join$osm_geometry_wkt, join$off_geometry)

# keep a backup with all records, including the ones that do not have a link to an OSM geometry
join_all <- join
join<-join_all


# keep only the records with a link to an OSM geometry
join <- join %>% filter(!is.na(original_id))

# acceptance elements:
### 1. name similarity ----

#extract name from languaged name + extract other_names from properties
join$name_1<-tolower(join$school)
join$name_2 <- tolower(mapply(extract_json_value, join$name, "und"))
join$other_names <- tolower(mapply(extract_json_value, join$properties, "other_names"))
#note: extract_json_value requires loading utils.R

# remove accents
join <- join %>%
  mutate(
    name_1=stri_trans_general(name_1, "Latin-ASCII"),
    name_2=stri_trans_general(name_2, "Latin-ASCII"),
    other_names=stri_trans_general(other_names, "Latin-ASCII"),
  )

# check for identical names
join <- join %>%
  mutate(
    same_name = ifelse(name_1==name_2 | name_1==other_names, 1, 0)
  )




# apply to these columns:
columns_to_clean <- c("name_1", "name_2", "other_names")

# default removals
patterns_to_remove <- c("\"", "”", "“", "´")
join <- string_removal(join, columns_to_clean, patterns_to_remove)
# string_removal is a function defined in utils.R

# remove words specific to the context
patterns_to_remove <- c("gemeindegrundschule","gemeindeschule","schule")
join <- string_removal(join, columns_to_clean, patterns_to_remove)


# remove leading and trailing whitespaces
join <- join %>%
  mutate(
    name_1 = trimws(name_1),
    name_2 = trimws(name_2),
    other_names = trimws(other_names)
  )

# find the longest common substring
join <- join %>%
  rowwise() %>%
  mutate(lcs=LCSn(c(tolower(name_1), tolower(name_2)))) %>%
  mutate(longest_common_str_length=nchar(lcs)) %>%
  mutate(other_names=ifelse(is.na(other_names), " ", other_names)) %>%
  mutate(alt_lcs=LCSn(c(tolower(name_1), tolower(other_names)))) %>%
  mutate(alt_longest_common_str_length=nchar(alt_lcs)) %>%
  mutate(longest_common_str_length=max(longest_common_str_length, alt_longest_common_str_length))


# shorten name before calculating length
patterns_to_remove <- c("go!", "provinciaal", "voor buitengewoon")
columns_to_clean <- c("name_1", "name_2")
join <- string_removal(join, columns_to_clean, patterns_to_remove)
join$length_name<-nchar(join$name_1)


# Decide when we consider the names to be the same (length should de significant)
join <- join %>%
  mutate(
    same_name = ifelse(
      ((longest_common_str_length > 10 | (longest_common_str_length > 4 & longest_common_str_length <= 10 & longest_common_str_length > length_name / 2)) | ((name_1==name_2 | name_1==as.character(other_names)) & tolower(mapply(extract_json_value, name, "und"))!="school") | same_name==1),
      1,
      0
    )
  )

join <- join %>%
  select(-lcs, -alt_lcs, -longest_common_str_length, -alt_longest_common_str_length, -length_name)


### 2. street similarity ----


# split street and house number
join <- join %>%
  mutate(
    house_number_1 = ifelse(grepl("\\b[0-9]+\\b", street), sub("^(.*\\s)(.*[0-9].*)$", "\\2", street), NA),
    street_1 = tolower(ifelse(grepl("\\b[0-9]+\\b", street), sub("^(.*\\s)(.*[0-9].*)$", "\\1", street), street))
  )

# set hnr_2
join <- join %>%
  mutate(house_number_2 = addr_housenumber)

# set hnr_t as missing if identical to street_2
join <- join %>%
  mutate(house_number_1 = ifelse(house_number_1 == street_1, NA, house_number_1))


# SIMPLIFY STREET NAMES

# remove accents
join <- join %>%
  mutate(
    street_1 = stri_trans_general(street_1, "Latin-ASCII"),
    street_2 = tolower(stri_trans_general(addr_street, "Latin-ASCII"))
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
columns_to_clean <- c("street_1", "street_2")
join <- string_replacement(join, columns_to_clean, replacements)
# string_replacement is a function defined in utils.R

# remove leading and trailing whitespaces
join <- join %>%
  mutate(
    street_1 = trimws(street_1),
    street_2 = trimws(street_2)
  )


# if result is empty string, replace with NA
join <- join %>%
  mutate(
    street_1 = ifelse(street_1 == "", NA, street_1),
    street_2 = ifelse(street_2 == "", NA, street_2)
  )
# END SIMPLIFY STREET NAMES

# make sure the result is still text
join$street_1 <- as.character(join$street_1)
join$street_2 <- as.character(join$street_2)


# calculate string distance between streets in both datasets
join <- join %>%
  mutate(streetsim = stringsim(street_1,street_2))

# check if the street on one side is a substring of street on the other side
join <- join %>%
  mutate(
    condition = nchar(street_1) > 3 & nchar(street_2) > 3,
    substring_left_check = ifelse(condition, str_detect(street_2, street_1), FALSE),
    substring_right_check = ifelse(condition, str_detect(street_1, street_2), FALSE),
    streetsim = ifelse((substring_left_check | substring_right_check) & streetsim<0.9, 0.9, streetsim)
  ) %>%
  select(-condition, -substring_right_check, -substring_left_check)



join <- join %>%
  mutate(
    house_number_1 = clean_string(house_number_1),
    house_number_2 = clean_string(house_number_2)
  )

# make sure the housenumber is text
join$house_number_1 <- as.character(join$house_number_1)
join$house_number_2 <- as.character(join$house_number_2)

join <- join %>%
  mutate(hnr_sim = stringsim(as.character(house_number_1), as.character(house_number_2)))




# check if the housenumber on one side is a substring of housenumber on the other side, if the number is at least two digits
join <- join %>%
  mutate(
    condition = nchar(as.character(house_number_2)) > 1 & nchar(as.character(house_number_1)) > 1,
    substring_left_check = ifelse(condition, str_detect(as.character(house_number_2), as.character(house_number_1)), FALSE),
    substring_right_check = ifelse(condition, str_detect(as.character(house_number_1), as.character(house_number_2)), FALSE),
    hnr_sim = ifelse((substring_left_check | substring_right_check) & hnr_sim<0.9 , 0.9, hnr_sim)
  ) %>%
  select(-condition, -substring_right_check, -substring_left_check)

join <- join %>%
  mutate(
    hnr_sim = if_else(
      !is.na(as.numeric(house_number_1)) & !is.na(as.numeric(house_number_2)),
      ifelse(abs(as.numeric(house_number_1) - as.numeric(house_number_2)) < 10 & hnr_sim<0.9, 0.9, hnr_sim),
      hnr_sim
    )
  )



## end street similarity



### 3. type of school ----

# Extract the value of type_primary to a new column
join$type_kindergarten <- as.numeric(sapply(join$properties, extract_json_value, key = "type_kindergarten"))
join$type_primary <- as.numeric(sapply(join$properties, extract_json_value, key = "type_primary"))
join$type_secondary <- as.numeric(sapply(join$properties, extract_json_value, key = "type_secondary"))
join$type_tertiary <- as.numeric(sapply(join$properties, extract_json_value, key = "type_higher_education"))

join$ger_kindergarten <- ifelse(join$type == "grundschule", 1, 0)
join$ger_primary <- ifelse(join$type == "grundschule", 1, 0)
join$ger_secondary <- ifelse(join$type == "sekundarschule", 1, 0)
join$ger_tertiary <- ifelse(join$type == "hochschule" | join$type == "förderschule" , 1, 0)


# if any combination of type is both 1, set same_school_type to 1
join$same_school_type <- ifelse(join$ger_kindergarten == 1 & join$type_kindergarten == "1", 1, 0) +
  ifelse(join$ger_primary == 1 & join$type_primary == "1", 1, 0) +
  ifelse(join$ger_secondary == 1 & join$type_secondary == "1", 1, 0) +
  ifelse(join$ger_tertiary == 1 & join$type_tertiary == "1", 1, 0)


join <- join %>%
  select(-name_1,-name_2,-other_names,-house_number_1, -house_number_2, -street_1, -street_2, -type_kindergarten, -type_primary, -type_secondary, -type_tertiary, -ger_kindergarten, -ger_primary, -ger_secondary, -ger_tertiary)


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




### Make choices ----
# SELECT only if certain quality thresholds met
join <- join %>%
  filter(distance < distance_matched_threshold | address_certainty >= 0.85 | same_school_type == 1 | same_name ==1)

# order by distance
join <- join %>%
  group_by(ogc_fid) %>% 
  arrange(distance) %>%
  mutate(distance_id = row_number()) %>%
  ungroup()

# order by address certainty
join <- join %>%
  group_by(ogc_fid) %>% 
  arrange(desc(address_certainty)) %>%
  mutate(address_certainty_id = row_number()) %>%  
  ungroup()

# create scoring variable:
# the closest object gets 1 point if it is within threshold, 0.75 if it is within 2xtreshold. Further objects get 0.65 points if they are within the distance threshold
# the best address gets 0.75 points, other addresses with a decent score get 0.5 points
# if the names are the same, the object gets 1 point
# if the school types are the same, the object gets 0.5 points
join <- join %>%
  mutate(same_name=ifelse(is.na(same_name), 0, same_name)) %>%
  mutate(
    score = 
      ifelse(distance_id == 1 & distance < distance_matched_threshold, 1, 0) +
      ifelse(distance_id == 1 & distance > distance_matched_threshold & distance < 2*distance_matched_threshold, 0.75, 0) +
      ifelse(distance_id >1 & distance < distance_matched_threshold, 0.65, 0) +
      ifelse(address_certainty>0 & address_certainty_id == 1, 0.75, 0) + ifelse(address_certainty>0 & address_certainty_id > 1, 0.5, 0) +
      ifelse(same_name == 1, 1, 0) + 
      ifelse(same_school_type == 1, 0.5, 0)
  )


# keep only the cases where the score equals the max score for the ogc_fid
join <- join %>%
  group_by(ogc_fid) %>% 
  filter(score == max(score)) %>%
  ungroup()

# if there's still more than one, keep the one with the closest distance (prepare filter)
join <- join %>%
  group_by(ogc_fid) %>% 
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
  select(ogc_fid, osm_id=original_id) %>%
  distinct()

ger_joined <- left_join(ger, join_simplified, by = "ogc_fid")

ger_joined <- st_as_sf(ger_joined, wkt="off_geometry")
ger_joined$off_geometry <- st_set_crs(ger_joined$off_geometry, 31370)
ger_joined <- st_transform(ger_joined, 4326)


# aggregation ID
ger_joined <- ger_joined %>%
  mutate(agg_id = ifelse(!is.na(osm_id),osm_id,st_as_text(off_geometry)))


# merge osm_cleaned_orig
ger_joined <- left_join(ger_joined, as.data.frame(osm_cleaned_orig), by = c("osm_id"="original_id"))


# create the school types again
ger_joined$type_kindergarten <- ifelse(ger_joined$type == "grundschule", 1, 0)
ger_joined$type_primary <- ifelse(ger_joined$type == "grundschule", 1, 0)
ger_joined$type_secondary <- ifelse(ger_joined$type == "sekundarschule", 1, 0)
ger_joined$type_tertiary <- ifelse(ger_joined$type == "hochschule" | ger_joined$type == "förderschule" , 1, 0)

# prepare addresses
# Step 1: Prepare Address Data (prep_add0)


# aggregate
ger_summarized <- ger_joined %>%
  group_by(agg_id) %>%
  summarize(
    ogc_fid = paste(unique(ogc_fid), collapse = '; '),
    type=paste(unique(type), collapse = '; '),
    school=paste(unique(school), collapse = '; '),
    address=paste(unique(street), collapse = '; '),
    postcode=paste(unique(zip), collapse = '; '),
    municipality=paste(unique(municipality), collapse = '; '),
    email=paste(unique(email), collapse = '; '),
    contact_person=paste(unique(contact), collapse = '; '),
    type_kindergarten = max(type_kindergarten),
    type_primary = max(type_primary),
    type_secondary = max(type_secondary),
    type_tertiary = max(type_tertiary),
    osm_id=first(osm_id),
    name=first(name),
    properties_osm=first(properties),
    geometry_point=first(off_geometry),
    geometry_osm=first(osm_geometry_wkt))

# create ger properties
ger_summarized <- ger_summarized %>%
  rowwise() %>%
  mutate(off_properties = jsonlite::toJSON(
    purrr::discard(list(
      address = address,
      postcode = postcode,
      municipality = municipality,
      email = email,
      school_types = type,
      contact_person = contact_person,
      type_kindergarten = type_kindergarten,
      type_primary = type_primary,
      type_secondary = type_secondary,
      type_tertiary = type_tertiary
    ), is.na), auto_unbox = TRUE))


ger_summarized <- as.data.frame(ger_summarized)



# Ensure the CRS is set correctly for geometry columns if not already
st_crs(ger_summarized$geometry_point) <- 4326
st_crs(ger_summarized$geometry_osm) <- 4326

# Use dplyr::case_when to conditionally assign geometries while preserving sf class
ger_summarized <- ger_summarized %>%
rowwise() %>%
  mutate(
    original_id = ifelse(is.na(osm_id), "none provided", paste0("https://osm.org/", osm_id)),
    name = ifelse(is.na(osm_id), toJSON(list(ger = school), auto_unbox = TRUE), name),
    name = ifelse(name == toJSON(list(und = "school"), auto_unbox = TRUE), toJSON(list(ger = school), auto_unbox = TRUE), name),
    name_source = ifelse(is.na(osm_id), 'Ostbelgien Bildung', 'OpenStreetMap + Ostbelgien Bildung'),
    geometry = case_when(
      is.na(osm_id) ~ geometry_point,
      TRUE ~ geometry_osm
    )
  ) %>%
  ungroup()


# Remove the unused geometry columns
ger_summarized <- ger_summarized %>%
  select(-geometry_point, -geometry_osm)

# Ensure the resulting dataframe is an sf object
ger_summarized <- st_as_sf(ger_summarized)

# Check and set CRS if necessary
st_crs(ger_summarized) <- 4326

# Verify if geometries are valid
invalid_geometries <- ger_summarized %>%
  filter(!st_is_valid(geometry))

ger_summarized <- ger_summarized %>%
  select(original_id,name,properties_osm, off_properties, name_source)




# Merge the regions ----

all_regions <- rbind(ger_summarized, brwa_summarized, vla_summarized)

# add created at with current date in YYYY-MM-DD format
all_regions$created_at <- Sys.Date()


# add objects from OSM polygons that are not in the regions

# select original_id from all_regions where it starts with https://osm.org
osm_ids <- as.data.frame(all_regions) %>%
  filter(str_detect(original_id, "https://osm.org")) %>%
  select(original_id)

osm_cleaned_id <- osm_cleaned_orig %>%
  mutate(original_id = paste0("https://osm.org/", original_id))


osm_add <- osm_cleaned_id %>%
  anti_join(osm_ids, by = c("original_id" = "original_id")) %>%
  rename(properties_osm = properties) %>%
  rename(geometry = osm_geometry_wkt) %>%
  select(-addr_street,-addr_housenumber, -legend_item,-categorie_name) %>%
  mutate(off_properties=NA)
all_regions <- rbind(all_regions, osm_add)


# add all OSM points outside of Belgium

con_pg <- get_con()
osm_school_point <- dbGetQuery(con_pg, "SELECT 
  original_id, name, name_source, properties AS properties_osm, NULL as off_properties, created_at, ST_AsText(geometry) as geometry
  FROM ingestion.osm_school_point;")
dbDisconnect(con_pg)

osm_school_point <-st_as_sf( osm_school_point, wkt="geometry")
osm_school_point$geometry <- st_set_crs(osm_school_point$geometry, 4326)

all_regions <- rbind(all_regions, osm_school_point)


# merge brussels schools
all_regions <- all_regions %>%
  group_by(original_id) %>% 
  mutate(
    count=n(),
    group_order = row_number()
  ) %>%
  ungroup()


doubles <- all_regions %>%
  filter(count==2)

all_regions <- all_regions %>% 
  mutate(
    name_source = ifelse(count==2, "OpenStreetMap + Fédération Wallonie-Bruxelles + Departement Onderwijs en Vorming", name_source))

# to better handle JSON
library(jqr)

# Define the jq expression
jq_expression_fwb <- '. | {"8be50971-8cc9-4eee-8a3d-538098301926": .}'
jq_expression_dov <- '. | {"7dcbb7cc-434a-4c65-b59b-fc554b844310": .}'

# Apply jq transformation cell by cell within the off_properties column
doubles <- as.data.frame(doubles) %>%
  mutate(off_properties = ifelse(grepl("Fédération Wallonie-Bruxelles", name_source), jqr::jq(off_properties, jq_expression_fwb), jqr::jq(off_properties, jq_expression_dov)))

doubles <-doubles %>%
  group_by(original_id) %>%
  summarize(off_properties_merged=paste(unique(off_properties), collapse = '; '))

# deactivating jqr as it affects general R behavior
detach("package:jqr", unload = TRUE)

# add the result to the all_regions
all_regions <- all_regions %>%
  left_join(doubles, by="original_id") %>%
  mutate(off_properties = ifelse(!is.na(off_properties_merged), off_properties_merged, off_properties)) %>%
  filter((count==2 & group_order==1) | count!= 2) %>%
  select(-count, -group_order, -off_properties_merged)

# end merge brussels schools

# add data_list_id
all_regions <- all_regions %>%
  mutate(data_list_id = case_when(
  name_source == 'Ostbelgien Bildung' ~ '678821e4-1f4f-46ef-8d8f-caca8afabec8',
  name_source == 'OpenStreetMap + Fédération Wallonie-Bruxelles + Departement Onderwijs en Vorming' ~ '2f8f3d0b-1137-44e6-a5cb-4dcf25ef1330',
  name_source == 'OpenStreetMap + Departement Onderwijs en Vorming' ~ '2f8f3d0b-1137-44e6-a5cb-4dcf25ef1330',
  name_source == 'Fédération Wallonie-Bruxelles' ~ '540c2fc1-9fe8-48eb-81eb-ce073b3de521',
  name_source == 'OpenStreetMap + Fédération Wallonie-Bruxelles' ~ '2f8f3d0b-1137-44e6-a5cb-4dcf25ef1330',
  name_source == 'Departement Onderwijs en Vorming' ~ '187cde3b-8036-40e5-a4cb-9e1354a5251b',
  name_source == 'OpenStreetMap (polygon data)' ~ '2f8f3d0b-1137-44e6-a5cb-4dcf25ef1330',
  name_source == 'OpenStreetMap (point data)' ~ '2f8f3d0b-1137-44e6-a5cb-4dcf25ef1330',
  name_source == 'OpenStreetMap + Ostbelgien Bildung' ~ '2f8f3d0b-1137-44e6-a5cb-4dcf25ef1330',
  TRUE ~ NA_character_
)) 

# add legend item
all_regions <- all_regions %>% 
  mutate(legend_item = toJSON(list(
    ger = 'schüle',
    dut = 'school',
    fre = 'école',
    eng = 'school'
  ), auto_unbox = TRUE))

# add properties
all_regions <- all_regions %>% 
  mutate(properties = ifelse(grepl("OpenStreetMap", name_source), properties_osm, off_properties))

# add properties_secondary

library(jqr)
# Define jq expressions
jq_expression_fwb <- '. | {"8be50971-8cc9-4eee-8a3d-538098301926": .}'
jq_expression_dov <- '. | {"7dcbb7cc-434a-4c65-b59b-fc554b844310": .}'
jq_expression_ob <- '. | {"678821e4-1f4f-46ef-8d8f-caca8afabec8": .}'

# Function to apply jq transformation safely
apply_jq_safe <- function(json_str, jq_expr) {
  if (!is.na(json_str) && json_str != "") {
    tryCatch(
      jqr::jq(json_str, jq_expr),
      error = function(e) {
        json_str  # return the original json_str if jq fails
      }
    )
  } else {
    json_str  # return the original json_str if it is NA or empty
  }
}

# Apply jq transformation cell by cell within the off_properties column
all_regions <- all_regions %>%
  mutate(properties_secondary = case_when(
    "OpenStreetMap + Fédération Wallonie-Bruxelles"== name_source ~ sapply(off_properties, apply_jq_safe, jq_expression_fwb),
    "OpenStreetMap + Departement Onderwijs en Vorming"== name_source ~ sapply(off_properties, apply_jq_safe, jq_expression_dov),
    "OpenStreetMap + Ostbelgien Bildung"== name_source ~ sapply(off_properties, apply_jq_safe, jq_expression_ob),
    "OpenStreetMap + Fédération Wallonie-Bruxelles + Departement Onderwijs en Vorming"==name_source ~ off_properties
  ))
# deactivating jqr as it affects general R behavior
detach("package:jqr", unload = TRUE)

# set risk_level
all_regions <- all_regions %>%
  rowwise() %>%
  mutate(
    type_kindergarten_off = extract_json_value(off_properties, "type_kindergarten"),
    type_kindergarten_osm = extract_json_value(properties_osm, "type_kindergarten"),
    type_primary_off = extract_json_value(off_properties, "type_primary"),
    type_primary_osm = extract_json_value(properties_osm, "type_primary"),
    risk_level = case_when(
      type_kindergarten_off == 1 | type_kindergarten_osm == 1 ~ 3,
      type_primary_off == 1 | type_primary_osm == 1 ~ 2,
      TRUE ~ 1
    )
  ) %>%
  select(-type_kindergarten_off, -type_kindergarten_osm, -type_primary_off, -type_primary_osm)  # Remove intermediate columns

# check for invalid, missing, empty geometries and geometrycollections
test_raw <- all_regions %>%
  filter(is.na(geometry) |  !st_is_valid(geometry) | st_is(geometry, "GEOMETRYCOLLECTION") | st_is_empty(geometry))


# generate UUID & keep only relevant columns
all_regions <- all_regions %>%
  mutate(id = uuid::UUIDgenerate()) %>%
  select(id,original_id, name, legend_item, data_list_id, risk_level, properties, properties_secondary, created_at)



# Upload to ingestion table ----

# Importing JSONB is apparently an issue, so we first turn those fields into text
table_without_jsonb<- all_regions
table_without_jsonb$name <- as.character(table_without_jsonb$name)
table_without_jsonb$legend_item <- as.character(table_without_jsonb$legend_item)
table_without_jsonb$properties <- as.character(table_without_jsonb$properties)
table_without_jsonb$properties_secondary <- as.character(table_without_jsonb$properties_secondary)

# slight reformat to make valid JSON
table_without_jsonb$properties_secondary <- gsub("}}; {", "},", table_without_jsonb$properties_secondary, fixed = TRUE)

# there's issues with the geo, this is fixed below

# make sure dataset is correctly formatted
table_without_jsonb<-st_as_sf(table_without_jsonb)
st_crs(table_without_jsonb) <- 4326

# save as geojson
#st_write(table_without_jsonb, paste0(log_folder, "schools2.geojson"), delete_layer = TRUE)

table_without_jsonb <- as.data.frame(table_without_jsonb)
table_without_jsonb$geometry_wkt <- as.character(st_as_text(table_without_jsonb$geometry, digits = 15))
table_without_jsonb <- table_without_jsonb %>% select(-geometry)

# now push it to a Postgres test table
con_pg<-get_con()
table_id <- DBI::Id(
  schema  = "ingestion",
  table   = "schools")
table_id_t <- paste0("ingestion.schools")
print(paste0("Import ingestion table to postgres at ", table_id_t))
dbWriteTable(con_pg, table_id, table_without_jsonb, overwrite = TRUE, row.names = FALSE )
dbDisconnect(con_pg)

# Add the geometry column using an SQL command
sql <- c(
  "ALTER TABLE ingestion.schools ADD COLUMN geometry geometry;",
  "UPDATE ingestion.schools SET geometry = ST_GeomFromText(geometry_wkt, 4326);",
  "ALTER TABLE ingestion.schools DROP COLUMN geometry_wkt;",
  "UPDATE ingestion.schools SET geometry = ST_MakeValid(geometry) WHERE NOT ST_IsValid(geometry)"
)
con_pg<-get_con()

for (sql_command in sql) {
  dbExecute(con_pg, sql_command)
}

# SQL statement to update the table structure
sql_update_table <- "
ALTER TABLE ingestion.schools 
    ALTER COLUMN id SET DATA TYPE uuid USING id::uuid,
    ALTER COLUMN id SET DEFAULT gen_random_uuid(),
    ALTER COLUMN name SET DATA TYPE jsonb USING name::jsonb,
    ALTER COLUMN legend_item SET DATA TYPE jsonb USING legend_item::jsonb,
    ALTER COLUMN data_list_id SET DATA TYPE uuid USING data_list_id::uuid,
    ALTER COLUMN risk_level SET DATA TYPE integer USING risk_level::integer,
    ALTER COLUMN properties SET DATA TYPE jsonb USING properties::jsonb,
    ALTER COLUMN properties_secondary SET DATA TYPE jsonb USING properties_secondary::jsonb,
    ALTER COLUMN created_at SET DATA TYPE timestamp with time zone USING created_at::timestamp with time zone,
    ADD COLUMN imported_at timestamp with time zone,
    ADD COLUMN tags jsonb,
    ADD COLUMN deleted_at timestamp with time zone,
    ADD COLUMN updated_at timestamp with time zone,
    ADD COLUMN created_by uuid,
    ADD COLUMN updated_by uuid,
    ALTER COLUMN geometry TYPE geometry(Geometry,4326) USING geometry::geometry(Geometry,4326);
"

# Execute the SQL statement

dbExecute(con_pg, sql_update_table)

# Close the connection
dbDisconnect(con_pg)


# Update transformation




### Create fdw views ----
fdw_views_sql <- c("
CREATE OR REPLACE VIEW fdw.fdw_schools
AS
SELECT id,
original_id,
name,
legend_item,
NULL as best_address_id,
NULL as capakey_id,
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
FROM transformation.schools;
","
ALTER TABLE fdw.fdw_schools
OWNER TO paragon;
","
GRANT SELECT ON TABLE fdw.fdw_schools TO fdw4dev;
","
GRANT ALL ON TABLE fdw.fdw_schools TO paragon;
")



### Execute the SQL commands ----

TransformOSMpoints <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in sql_commands_osm_points) {
        dbExecute(con_pg, sql_command)
      }
      print("The SQL functions for OSMpoints ran without error")
    },
    error = function(err) {
      print("The SQL functions for OSMpoints failed")
      print(err)
    }
  )
  dbDisconnect(con_pg)
}

TransformOSMpolygons <-function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in sql_commands_osm_polygons) {
        dbExecute(con_pg, sql_command)
      }
      print("The SQL functions for OSMpolygons ran without error")
    },
    error = function(err) {
      print("The SQL functions for OSMpolygons failed")
      print(err)
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
      print("The SQL functions for the FDW views ran without error")
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

main_function = function() {
  geojson_vl<-DownloadFlanders()
  geojson_brwa<-DownloadBRWA()
  ger<-DownloadGER()
  #osm_data<-DownloadOSMdata()
  #osm_points<-FilterOSMpoints(osm_data)
  #osm_polygon_data_to_import<-FilterOSMpolygons(osm_data)
  CreateImportTableVL(dataset = geojson_vl, schema = "raw_data", table_name = "vla_depov_poiservice_schools")  
  CreateImportTableBRWA(dataset = geojson_brwa, schema = "raw_data", table_name = "brwa_cfwb_odata_schools")  
  CreateImportTableGER(dataset = ger, schema = "raw_data", table_name = "ger_dgov_email_schools")  
  CreateImportTableOSMpoly(dataset = osm_mpoly, schema = "raw_data", table_name = "osm_school_polygon")  
  CreateImportTableOSMpoint(dataset = osm_points, schema = "raw_data", table_name = "osm_school_point")  
  GeocodeAndUploadBRWA()
  GeocodeAndUploadGer()
  TransformOSMpoints()
  TransformOSMpolygons()
  #TransfromVL()
  #TransformBRWA()
  #TransformGer()
  #TransformMergeAllData()
  #create_fdw_views()
}


if(F){
  main_function()
}


