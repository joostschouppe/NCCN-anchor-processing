## ---------------------------
##
## Script name: ETL flow for police stations
##
## Purpose of script: Merge data about police stations from several sources and transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-10-30
##
##
## ---------------------------

#TODO: TEST police processing: this combo should not be possible data_list_id='c418b715-4e56-4c27-83bf-bd303a779d56' AND original_id!='none provided'


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------



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

# Data list IDs
data_list_id_osm<-"9223ce7e-9c3e-461c-87b1-94e8aea59a13"
data_list_id_dri<- "c418b715-4e56-4c27-83bf-bd303a779d56"



# Set location for files received by email
offline_storage <-Sys.getenv("OFFLINE_STORAGE")
data_folder <- paste0(offline_storage,"/police/")
local_police_data <- "T_ZPZ_Geol_Pseudo_Mercator_20241204_cleaned.csv"
federal_police_data <- "20231211_PolFed_adresses_principale_secondaires.xlsx"

# Set log folder
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))



# Extra libraries -------------------------------
# """""""""""""""""" ----------------------

## Geocoding libraries
library(devtools)
library(phacochr)



## Wikidata library
library(WikidataQueryServiceR)

# Excel reading
library(readxl)



# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    
    # Only load phaco data if we're actually going to use it
    phaco_setup_data()
    phacochr::phaco_best_data_update()
    
# Open local police data ----

## Source: Mieke.Louwage@police.belgium.eu; DRI.Business.PoliceAccounting@police.belgium.eu

## open the csv
police_csv <- read.csv(paste0(data_folder,local_police_data), sep=";")

## we like our variable names in lowercase
names(police_csv) <- tolower(names(police_csv))

## add row numbers
police_csv <- police_csv %>% mutate(row_number = row_number())

# derive geometry from x_long and y_lat columns
police_full <- police_csv %>%
  mutate(
    geometry = st_sfc(lapply(seq_along(x_long), function(i) st_point(c(x_long[i], y_lat[i])))),
    .keep = "all"
  )
# set as SF
police_full <- st_as_sf(police_full, coords = c("x_long", "y_lat"), crs = 3857)
#  tranform to 31370
police_full <- st_transform(police_full, 31370)
police_full <- police_full %>% rename(geometry_official = geometry)

# remove cases that are in the official data, but we decided to erase
police_full <- police_full %>% filter(paragon!="erase")

# rename hoofd..wijkcommissariaat to hoofd_wijkcommissariaat
police_full <- police_full %>% rename(hoofd_wijkcommissariaat = hoofd..wijkcommissariaat)

# make it available outside the function
police_full <<- police_full

# check unicity of id_dri
if (length(unique(police_full$id_dri)) != nrow(police_full)) {
  stop("id_dri is not unique")
}
print("Loaded local police geodata")


# Open federal police data ----
## Source: Humblet Isabelle (DRI) <Isabelle.Humblet@police.belgium.eu> (and DRI)

fedpol_xlsx <- read_excel(paste0(data_folder,federal_police_data))

# aggregate by unique address & summarize relevant info

fedpol <- fedpol_xlsx %>%
  group_by(STRTextBD, STRTextBF, LPLHouseNr,LPLZip,PRVTextBD) %>%
  summarise(
    Count = n(),
    enterprisenr = list(unique(LPLEntrerpiseNr)),
    police_users_nl=list(unique(OCPTextBD)),
    police_users_fr=list(unique(OCPTextBF)),
    ocpkeys=list(unique(OCPKey))
  ) %>%
  ungroup()

## we like our variable names in lowercase
names(fedpol) <- tolower(names(fedpol))

## add row numbers
fedpol <- fedpol %>% mutate(row_number = row_number())


## geocode
geocode_fedpol <- phaco_geocode(data_to_geocode=t_adresse <- fedpol, colonne_rue= "strtextbd", colonne_num="lplhousenr", colonne_code_postal="lplzip")
geocode_fedpol_bis <- phaco_geocode(data_to_geocode=t_adresse <- fedpol, colonne_rue= "strtextbf", colonne_num="lplhousenr", colonne_code_postal="lplzip")

## change geometry column name and add results to all records
geocode_fedpol <- geocode_fedpol$data_geocoded_sf[, c("row_number")]
geocode_fedpol <- geocode_fedpol %>% rename(geometry_official = geometry)
geocode_fedpol_bis <- geocode_fedpol_bis$data_geocoded_sf[, c("row_number")]
geocode_fedpol_bis <- geocode_fedpol_bis %>% rename(geometry_official = geometry)
geocode_fedpol <- rbind(geocode_fedpol, geocode_fedpol_bis)
# remove duplicates
geocode_fedpol <- geocode_fedpol %>% distinct()


fedpol <- left_join(fedpol, geocode_fedpol, by = "row_number")

# turn lists into comma separated values
process_data <- function(x) {
  if (is.numeric(x)) {
    # Convert numeric values to strings
    x <- as.character(x)
  } else {
    # For lists, convert to string and remove "c(" and ")"
    x <- gsub("^c\\(|\\)$", "", toString(x))
  }
  return(x)
}

# Apply the function to the entire column
fedpol$enterprisenr <- as.character(sapply(fedpol$enterprisenr, process_data))
fedpol$police_users_nl <- as.character(sapply(fedpol$police_users_nl, process_data))
fedpol$police_users_fr <- as.character(sapply(fedpol$police_users_fr, process_data))
fedpol$ocpkeys <- as.character(sapply(fedpol$ocpkeys, process_data))

# set geometry
fedpol <- st_set_geometry(fedpol, "geometry_official")

# make available outside the function
fedpol <<- fedpol



# Download wikidata ----
### make a query (start at https://query.wikidata.org/querybuilder/?uselang=nl and use the "show query in the query service" interface)
sparql_query <- "SELECT DISTINCT ?zone ?nl ?fr ?de ?zoneId ?website WHERE {
  ?zone wdt:P31 wd:Q2621126.
  OPTIONAL { ?zone wdt:P856 ?website. }
  OPTIONAL { ?zone wdt:P10450 ?zoneId. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"nl\". ?zone rdfs:label ?nl. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"fr\". ?zone rdfs:label ?fr. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"de\". ?zone rdfs:label ?de. }
  FILTER NOT EXISTS { ?zone wdt:P576 ?dissolvedDate. }
}"

### load the actual data
zones <- query_wikidata(sparql_query, format = c("simple", "smart"))
### give nice names
zones_cleaned <- zones %>% rename(operator_wikidata=zone,zone_name_nl=nl,zone_name_fr=fr,zone_name_de=de)
### create a simple wikidata number variable
zones_cleaned$operator_wikidata <- gsub("^http://www.wikidata.org/entity/", "", zones_cleaned$operator_wikidata)
### remove fake names
zones_cleaned <- zones_cleaned %>%
  mutate(
    zone_name_nl = ifelse(grepl("^Q[0-9]+$", zone_name_nl), NA, zone_name_nl),
    zone_name_fr = ifelse(grepl("^Q[0-9]+$", zone_name_fr), NA, zone_name_fr),
    zone_name_de = ifelse(grepl("^Q[0-9]+$", zone_name_de), NA, zone_name_de)
    )

### de-duplicate (caused by the website, which can have multiple values)
zones_cleaned <- zones_cleaned %>%
  group_by(zoneId) %>%
  summarize(
    zone_name_nl = first(zone_name_nl),
    zone_name_fr = first(zone_name_fr),
    zone_name_de = first(zone_name_de),
    operator_in_wikidata = first(operator_wikidata),
    website_wikidata = paste(website, collapse = "; ")
  )

# make available outside the function
zones_cleaned <<- zones_cleaned



# Download OSM data ----

# Define the list of features
features_list <- list("amenity" = "police")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
extra_columns <- c("police:type")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})





# Upload to raw data ----
# CreateImportTable is loaded via utils and called in the main function
osm_all_raw<<-osm_all


# TRANSFORM ----
# """""""""""""""""" ----

# Merge all official police data


police_all_official <- bind_rows(police_full, fedpol)




## Join wikidata to police data ----
police_all_official <- left_join(police_all_official, zones_cleaned, by = c("po2key"="zoneId"))


### Transform police data to simple features and reproject ----

# Create an sf object from the data frame
police_all_official <- st_set_crs(police_all_official, 31370)

# and add row number
police_all_official <- police_all_official %>% mutate(police_all_row = row_number()) 


### Split off the unsuccessfully geocoded ----

# Optional: save unsuccessful police stations
#police_no_luck <- police_all_official %>% filter(st_is_empty(police_all_official$geometry_official))
#write.csv(police_no_luck, file = paste0(log_folder, "no_geocode_all_police.csv"), row.names = FALSE)

# Filter successfully geocoded police
police_geocoded <- police_all_official %>% filter(!st_is_empty(police_all_official$geometry_official))

# do spatial join to OSM data

# Data preparation
## Reproject OSM to Lambert72
osm_all <- st_transform(osm_all, crs = 31370)


# Execute spatial join: adjust distance as necessary
join <- st_join(police_geocoded, osm_all, join = st_is_within_distance, dist = 100)
join <- as.data.frame(join)

osm_all <- as.data.frame(osm_all)

osm_geometry <- osm_all %>%
  select(osm_id,geometry)

distance <- left_join(join, as.data.frame(osm_geometry), by = "osm_id")

# Calculate distance between potential matches
distance <- distance %>%
  rowwise %>%
  mutate(distance = st_distance(geometry, geometry_official))
distance$distance <- as.numeric(distance$distance)

distance <- distance %>%
  group_by(osm_id) %>%
  mutate(osm_count = n()) %>%
  ungroup()

distance <- distance %>%
  group_by(police_all_row) %>%
  mutate(police_count = n()) %>%
  ungroup()

# also check count of the police stations
# if police station mapped to more than one object, link to closest. Buffer 75m? Make sure to treat n=1 different from n>1
# if osm is still duplicate after this, merge into single feature

# sort cases
distance  <- distance  %>%
  arrange(police_all_row, distance)

# Selecting the first row within each 'police_all_row' group
distance  <- distance %>%
  group_by(police_all_row) %>%
  slice(1)


# use the OSM geometry where available
distance<-distance %>%
  mutate(geometry = ifelse(is.na(osm_id), geometry_official, geometry),
       geometry_official = NULL)
distance <- st_set_geometry(distance, "geometry")


# aggregate rows with the same geometry

distance <- distance %>%
  group_by(st_as_text(geometry)) %>%
  summarise_all(~paste(unique(na.omit(.)), collapse = ",")) %>%
  select(-`st_as_text(geometry)`)




# replace empty with nulls
replace_empty_with_null <- function(x) {
  ifelse(x == "", NA_character_, x)
}
string_columns <- names(distance)[sapply(distance, is.character) & names(distance) != "geometry"]
distance <- distance %>%
  mutate_at(vars(all_of(string_columns)), ~ replace_empty_with_null(.))


# make sure geometry is used in sf way
distance <- st_as_sf(distance)
distance <- st_set_crs(distance, 31370)
distance <<- st_transform(distance, crs = 4326)



### Upload semi processed police data ----
#this happens in the main function


  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
}


# LOAD ----
# """""""""""""""""" ----

### Create SQL for local police official data ingestion table ----

# link NGI to local police to get municipality name, which is then used to name the local police objects



sql_local_police <- c("
-- in the end, we will fill up this ingestion table


DROP TABLE IF EXISTS ingestion.local_police CASCADE;",
                      "CREATE TABLE IF NOT EXISTS ingestion.local_police
  (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    original_id text,    
    name jsonb,
    legend_item jsonb,
    name_source text,
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
    CONSTRAINT police_cleaned_pkey PRIMARY KEY (id)
  );",
  "WITH merge AS (
    SELECT 
    p.ogc_fid as ogc_fid,
    m.fid as m_fid,
    m.languagestatute as languagestatute,
    m.nameger as nameger,
    m.namefre as namefre,
    m.namedut as namedut,
    CASE WHEN languagestatute=1 THEN 'dut'
    WHEN languagestatute=5 THEN 'dut'
    WHEN languagestatute=4 THEN 'brussels'	
    WHEN languagestatute=8 THEN 'ger'	
    WHEN languagestatute=7 THEN 'fre'		
    WHEN languagestatute=6 THEN 'fre'
    WHEN languagestatute=2 THEN 'fre'
    ELSE 'und' END as language
    FROM ingestion.police_prep p, raw_data.ngi_ign_municipality m
    WHERE ST_Intersects(p.geometry,m.shape)),
  
  -- set the source
  
  -- fill in all the names, not just the local name!
    
prep AS (
-- find the municipality (to get the language)
     SELECT m.languagestatute, m.nameger, m.namefre, m.namedut, m.language, p.* ,
      CASE WHEN osm_id IS NULL THEN 'DRI'
      ELSE 'OSM' END AS source,	
      CASE 
      WHEN LOWER(p.hoofd_wijkcommissariaat)='wijk' THEN 'Politie antenne'
      WHEN LOWER(p.hoofd_wijkcommissariaat)='hoofd' THEN 'Politiecommissariaat'
	  WHEN p.hoofd_wijkcommissariaat IS NULL AND p.enterprisenr IS NOT NULL THEN 'Kantoor Federale Politie'
      ELSE 'Politiekantoor (gemengd gebruik)' END as type_post_dut,
      CASE 
      WHEN LOWER(p.hoofd_wijkcommissariaat)='wijk' THEN 'Antenne de police'
      WHEN LOWER(p.hoofd_wijkcommissariaat)='hoofd' THEN 'Commissariat de police'
	  	  WHEN p.hoofd_wijkcommissariaat IS NULL AND p.enterprisenr IS NOT NULL THEN 'Office de la Police Fédérale'
      ELSE 'Commissariat (usage mixte)' END as type_post_fre,
      CASE 
      WHEN LOWER(p.hoofd_wijkcommissariaat)='wijk' THEN 'Polizeiwache (Nachbarschaft)'
      WHEN LOWER(p.hoofd_wijkcommissariaat)='hoofd' THEN 'Polizeiwache'
	  	  WHEN p.hoofd_wijkcommissariaat IS NULL AND p.enterprisenr IS NOT NULL THEN 'Bundespolizeiamt'
      ELSE 'Polizeistation (Mischnutzung)' END as type_post_ger,
	  CASE 
      WHEN LOWER(p.hoofd_wijkcommissariaat)='wijk' THEN 'Police neighborhood office'
      WHEN LOWER(p.hoofd_wijkcommissariaat)='hoofd' THEN 'Police station'
	  	  WHEN p.hoofd_wijkcommissariaat IS NULL AND p.enterprisenr IS NOT NULL THEN 'Federal police office'
      ELSE 'Police station (mixed use)' END as type_post_eng,
      CASE
      WHEN osm_id IS NULL THEN null
      ELSE name END AS osm_name,
      NULLIF(CONCAT_WS('; ',contact_email, email),'') AS osm_email,
      NULLIF(CONCAT_WS('; ', phone, contact_phone, contact_mobile, phone_2, mobile),'') AS osm_phone, 
      NULLIF(CONCAT_WS('; ', website, contact_website),'') AS osm_website,
	  NULLIF(CONCAT_WS('; ',short_name, official_name, alt_name, old_name),'') AS osm_other_names,
	CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END
	AS osm_address,
	  opening_hours AS osm_opening_hours,
      CASE WHEN zone_name_de IS null and zone_name_fr IS NOT null THEN zone_name_fr
      WHEN zone_name_de IS null and zone_name_fr IS null THEN zone_name_nl
      ELSE zone_name_de END
      AS zone_name_de_cleaned,
      CASE WHEN zone_name_nl IS null and zone_name_fr IS NOT null THEN zone_name_fr
      WHEN zone_name_nl IS null and zone_name_fr IS null THEN zone_name_de
      ELSE zone_name_nl END
      AS zone_name_nl_cleaned,
      CASE WHEN zone_name_fr IS null and zone_name_de IS NOT null THEN zone_name_de
      WHEN zone_name_fr IS null and zone_name_de IS null THEN zone_name_nl
      ELSE zone_name_fr END
      AS zone_name_fr_cleaned,
	CASE 
	WHEN enterprisenr IS NULL THEN NULL
	WHEN language='brussels' THEN CONCAT(strtextbd,'/',strtextbf,' ',lplhousenr,', ',lplzip,' ',namedut,'/',namefre)
	WHEN language='ger' THEN CONCAT(strtextbd,' ',lplhousenr,', ',lplzip,' ',nameger)
	WHEN language='dut' THEN CONCAT(strtextbd,' ',lplhousenr,', ',lplzip,' ',namedut)
	WHEN language='fre' THEN CONCAT(strtextbf,' ',lplhousenr,', ',lplzip,' ',namefre)
	ELSE CONCAT(strtextbd,' ',lplhousenr,', ',lplzip) END AS fedpol_address,
	enterprisenr as fedpol_enterprisenr,
	police_users_nl as fedpol_police_users_nl,
	police_users_fr as fedpol_police_users_fr,
	ocpkeys as fedpol_ocpkeys
      FROM ingestion.police_prep p
      LEFT JOIN merge m ON p.ogc_fid=m.ogc_fid),

prep2 AS (
SELECT *,
CASE WHEN language='fre' THEN type_post_fre 
WHEN language='dut' THEN type_post_dut 
WHEN language='ger' THEN type_post_ger 
WHEN language='brussels' THEN type_post_fre || ' / ' || type_post_dut END
as type_post_und,
CASE WHEN language='fre' THEN namefre 
WHEN language='dut' THEN namedut 
WHEN language='ger' THEN nameger 
WHEN language='brussels' AND namefre=namedut THEN namefre
WHEN language='brussels' THEN namefre || ' / ' || namedut END
as nameund
from prep)


INSERT INTO ingestion.local_police 
(original_id, name, legend_item, name_source, properties, properties_secondary, geometry, created_at)
  SELECT
  CASE WHEN source='DRI' AND id_dri IS NOT NULL THEN id_dri
  WHEN source='DRI' THEN 'none provided'
  ELSE 'https://osm.org/' || osm_id END as original_id,
  CASE WHEN source='DRI' THEN 
  JSONB_STRIP_NULLS(jsonb_build_object('dut', CONCAT(type_post_dut,(' ' || namedut)),
                     'fre', CONCAT(type_post_fre,(' ' ||namefre)),
                     'ger', CONCAT(type_post_ger,(' ' ||nameger)),
					           'und', type_post_und || ' ' || nameund))
       WHEN osm_name IS NULL THEN jsonb_build_object('und', 'police station')
  ELSE jsonb_build_object('und', osm_name) END
  as name,
  JSONB_BUILD_OBJECT(
    'dut', concat(type_post_dut),
    'fre', concat(type_post_fre),
    'ger', concat(type_post_ger)  
  ) as legend_item,
  source as name_source, 
  CASE WHEN source='DRI' AND po2key IS NOT NULL THEN
  JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
		'address', CONCAT(street,' ',house,', ',zip,' ',city),
	  	'zone_name', CASE WHEN region='Vlaanderen' THEN CONCAT(po2textbd,' (',po2key,')') ELSE CONCAT(po2textbf,' (',po2key,')') END
  		))
  WHEN source='DRI' THEN
  	JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
		'address', fedpol_address,
		'police_users_nl', fedpol_police_users_nl,
	  'police_users_fr', fedpol_police_users_fr))
  ELSE JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
    'website', osm_website,
    'phone', osm_phone,
    'email', osm_email,
    'opening_hours', osm_opening_hours,
  	'address', osm_address,
  	'other_names', osm_other_names))
  END as properties,
  CASE WHEN source='OSM' AND po2key IS NOT NULL THEN
    JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
		'address', CONCAT(street,' ',house,', ',zip,' ',city),
		'zone_name', CASE WHEN region='Vlaanderen' THEN CONCAT(po2textbd,' (',po2key,')') ELSE CONCAT(po2textbf,' (',po2key,')') END,
	    'website_via_wikidata', website,
    	'wikidata',  operator_wikidata))
  WHEN source='OSM' AND po2key IS NULL THEN
  	JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
      'address', fedpol_address,
		  'police_users_nl', fedpol_police_users_nl,
	    'police_users_fr', fedpol_police_users_fr,
	    'website (via wikidata)', website,
      'wikidata',  operator_wikidata))
  ELSE JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
    'website', osm_website,
    'phone', osm_phone,
    'email', osm_email,
	'opening_hours', osm_opening_hours,
  	'address', osm_address,
  	'other_names', osm_other_names,
    'website_via_wikidata', website,
    'wikidata',  operator_wikidata))
  END as properties_secondary,
  geometry AS geometry,
  CURRENT_DATE as created_at
  FROM prep2")




### Create SQL for OSM ingestion table ----

sql_osm <- c(
    "DROP TABLE IF EXISTS ingestion.police_osm CASCADE;",
    "CREATE TABLE IF NOT EXISTS ingestion.police_osm
  (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    original_id text,    
    name jsonb,
    legend_item jsonb,
    name_source text,
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
    CONSTRAINT police_osm_cleaned_pkey PRIMARY KEY (id)
  );
  ","  
  WITH osm AS (SELECT
               *, 
               'OSM' as source
               from raw_data.osm_police),

name_cleaned AS (
SELECT *, 'https://osm.org/' || osm_id as osm_id_full,
	CASE WHEN name IS NOT null THEN name
  		 WHEN addr_city IS NOT null THEN addr_city
		 ELSE 'police station' END as name_clean
from osm)


-- left join on OSM id to find the stations already used as local police geometry (we keep the ones that cannot be found in the police table)
INSERT INTO ingestion.police_osm (original_id, name, legend_item, name_source, properties, geometry, created_at)
  SELECT 
  o.osm_id_full AS original_id,
  JSONB_BUILD_OBJECT('und', o.name_clean)
   as name,
  JSONB_BUILD_OBJECT('und', 'police station') as legend_item,
  o.source as name_source,
  JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	  'other_names', NULLIF(CONCAT_WS('; ',short_name, official_name, alt_name, old_name),''),
	  'address', CASE WHEN addr_street IS NULL THEN NULL 
		ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END,
	  'website', NULLIF(CONCAT_WS('; ', o.website, o.contact_website),''),
    'email', NULLIF(CONCAT_WS('; ', o.email, o.contact_email),''),
    'phone', NULLIF(CONCAT_WS('; ', o.phone, o.contact_phone, o.mobile, o.contact_mobile, o.phone_2),''),
    'wikidata', NULLIF(CONCAT_WS('; ', o.wikidata, o.operator_wikidata),''),
    'quality_remark',
       CASE WHEN o.operator_wikidata is not NULL OR EXTRACT(YEAR FROM CURRENT_DATE)-CAST(LEFT(o.check_date, 4) AS NUMERIC)<3 THEN 'not found in official sources but high confidence'
       ELSE 'not found in official sources' END
  ))
  as properties,
  o.geometry as geometry,
  CURRENT_DATE as created_at
  FROM name_cleaned o
  LEFT JOIN ingestion.local_police p ON o.osm_id_full = p.original_id
  WHERE p.name_source IS null;
")



### Create SQL for final ingestion table ----

sql_merge <- c(
  "DROP TABLE IF EXISTS ingestion.police CASCADE;",
  "CREATE TABLE IF NOT EXISTS ingestion.police
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
  CONSTRAINT ing_police_pkey PRIMARY KEY (id)
);",
paste0("  

WITH merge AS 
(SELECT * FROM ingestion.local_police
  UNION ALL
  SELECT * FROM ingestion.police_osm),

alldata AS (SELECT *, 
CASE WHEN name_source='OSM' THEN '",data_list_id_osm,"'
ELSE '",data_list_id_dri,"' END as data_list_id 
FROM merge)
  

INSERT INTO ingestion.police (id, original_id, name, legend_item, data_list_id, risk_level, properties, properties_secondary, geometry, created_at)
select id, original_id, name, legend_item,
data_list_id::uuid,
0 as risk_level, properties, 
CASE WHEN name_source='OSM' AND properties_secondary <> '{}' THEN 
jsonb_build_object('",data_list_id_dri,"', properties_secondary)
WHEN name_source='DRI' AND properties_secondary <> '{}' THEN 
jsonb_build_object('",data_list_id_osm,"', properties_secondary)
ELSE properties_secondary
END AS properties_secondary,	
geometry as geometry,
created_at as created_at
FROM alldata
WHERE geometry IS NOT NULL;
"),"
--add this if you want to be able to easily test the data in QGIS
--DROP TABLE IF EXISTS tst.transf_police CASCADE;
","
--CREATE TABLE tst.transf_police AS
--SELECT *,
--CASE WHEN ST_GeometryType(geometry) = 'ST_Point' THEN geometry END as geometry_point,
--CASE WHEN ST_GeometryType(geometry) = 'ST_MultiPolygon' THEN geometry END AS geometry_poly
--FROM ingestion.police;"
)



### Execute the SQL commands ----


TransformLocalPolice <- function() {execute_sql_commands(sql_local_police, "Official police data transformation")}
TransformOSM <- function() {execute_sql_commands(sql_osm, "OSM police data transformation")}
TransformMergeAll <- function() {execute_sql_commands(sql_merge, "Merge police data")}

#When running manually: use the parameters at the start of the code to change defaults
run_smart_update = function() {
  smart_update_process("police", 50, 40, 40, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = police_full, schema = "raw_data", table_name = "fed_dri_police_local_mail")  
    CreateImportTable(dataset = fedpol, schema = "raw_data", table_name = "fed_dri_police_fed_mail")  
    CreateImportTable(dataset = osm_all_raw, schema = "raw_data", table_name = "osm_police")  
    CreateImportTable(dataset = zones_cleaned, schema = "raw_data", table_name = "wikidata_be_local_police")  
    CreateImportTable(dataset = distance, schema = "ingestion", table_name = "police_prep")  
    TransformLocalPolice()
    TransformOSM()
    TransformMergeAll()
  }
  run_smart_update()
  #create_transformation_table()
}


if(run_status){
  main_function()
}