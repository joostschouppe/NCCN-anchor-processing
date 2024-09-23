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
# https://osm.org/ to osm original_id; keep in account this has to be done manually on the old transformation table to be able to match on unique id!


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

data_list_id<-"b3f833af-1c7a-4d03-8ceb-ffd55bfea5f8"
log_folder <- "C:/temp/logs/"

data_folder <- "C:/projects/proto-anchors/raw-data/police/"
local_police_data <- "20231017_9833_onthaalpunten Lokale Politie.csv"
federal_police_data <- "20231211_PolFed_adresses_principale_secondaires.xlsx"



### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))



# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(sf)
library(RPostgres)

## Geocoding libraries
library(devtools)
devtools::install_github("phacochr/phacochr")
library(phacochr)
phaco_setup_data()
phacochr::phaco_best_data_update()

library(DBI)

## Data processing libraries
library(dplyr)

## Wikidata library
library(WikidataQueryServiceR)

## OSM library
library(osmdata)

library(readxl)



# EXTRACT ----
# """""""""""""""""" ----

# Open local police data ----

## Source: Mieke.Louwage@police.belgium.eu; DRI.Business.PoliceAccounting@police.belgium.eu

## open the csv
police_csv <- read.csv(paste0(data_folder,local_police_data), sep=";")

## we like our variable names in lowercase
names(police_csv) <- tolower(names(police_csv))

## add row numbers
police_csv <- police_csv %>% mutate(row_number = row_number())

## geocode
geocode_police <- phaco_geocode(data_to_geocode=t_adresse <- police_csv, colonne_rue= "straatnaam", colonne_num="nummer", colonne_code_postal="postcode")

## change geometry column name and add results to all records
simple_geocode <- geocode_police$data_geocoded_sf[, c("row_number")]
simple_geocode <- simple_geocode %>% rename(geometry_geocoding = geometry)
police_full <- left_join(police_csv, simple_geocode, by = "row_number")
police_full <- st_set_geometry(police_full, "geometry_geocoding")


# Open federal police data

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
geocode_fedpol <- geocode_fedpol %>% rename(geometry_geocoding = geometry)
geocode_fedpol_bis <- geocode_fedpol_bis$data_geocoded_sf[, c("row_number")]
geocode_fedpol_bis <- geocode_fedpol_bis %>% rename(geometry_geocoding = geometry)
geocode_fedpol <- rbind(geocode_fedpol, geocode_fedpol_bis)
# remove duplicates
geocode_fedpol <- geocode_fedpol %>% distinct()


fedpol <- left_join(fedpol, geocode_fedpol, by = "row_number")

# turn lists into comma separeted values
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
fedpol <- st_set_geometry(fedpol, "geometry_geocoding")




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





# Download OSM data ----

# Define the list of features
features_list <- list("amenity" = "police")
# If default server fails, set to TRUE to use mail.ru server (older data)
alternative_overpass_server<-FALSE
# Define extra tags to use as columns for properties
# TODO: this doesn't seem to work properly, but it causes no errors 
extra_columns <- c("police:type")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----

tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, alternative_overpass_server)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})





# Upload to raw data ----
# CreateImportTable is loaded via utils and called in the main function
osm_all_raw<-osm_all


# TRANSFORM ----
# """""""""""""""""" ----

# Merge all official police data


police_all_official <- bind_rows(police_full, fedpol)




## Join wikidata to police data ----
police_all_official <- left_join(police_all_official, zones_cleaned, by = c("zonenummer"="zoneId"))


### Transform police data to simple features and reproject ----
# Create an sf object from the data frame


police_all_official <- st_set_crs(police_all_official, 31370)

# and add row number
police_all_official <- police_all_official %>% mutate(police_all_row = row_number()) 


### Split off the unsuccessfully geocoded ----

# Optional: save unsuccessful police stations
police_no_luck <- police_all_official %>% filter(st_is_empty(police_all_official$geometry))
write.csv(police_no_luck, file = paste0(data_folder, "no_geocode_all_police.csv"), row.names = FALSE)

# Filter successfully geocoded police
police_geocoded <- police_all_official %>% filter(!st_is_empty(police_all_official$geometry))

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
  mutate(distance = st_distance(geometry, geometry_geocoding))
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
  mutate(geometry = ifelse(is.na(osm_id), geometry_geocoding, geometry),
       geometry_geocoding = NULL)
distance <- st_set_geometry(distance, "geometry")


# aggregate rows with the same geometry

distance <- distance %>%
  group_by(st_as_text(geometry)) %>%
  summarise_all(~paste(unique(na.omit(.)), collapse = ",")) %>%
  select(-`st_as_text(geometry)`)


distance <- st_set_crs(distance, 31370)
distance <- st_transform(distance, crs = 4326)

#distance0 <- distance %>%
#  select(osm_id,police_all_row,hoofd_wijkcommissariaat , distance, osm_count, name, straatnaam, nummer, gemeente, strtextbd, lplhousenr, lplzip, geometry, #geometry_geocoding)


# replace empty with nulls
replace_empty_with_null <- function(x) {
  ifelse(x == "", NA, x)
}
string_columns <- names(distance)[sapply(distance, is.character) & names(distance) != "geometry"]
distance <- distance %>%
  mutate_at(vars(all_of(string_columns)), ~ replace_empty_with_null(.))


### Upload semi processed police data ----
#this happens in the main function



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
                      "  WITH merge AS (
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
      CASE
      WHEN osm_id IS NULL THEN null
      WHEN contact_email IS NULL AND email IS NULL THEN NULL
		ELSE CONCAT_WS('; ',contact_email, email) END AS osm_email,
      CASE
      WHEN osm_id IS NULL THEN null
      WHEN CONCAT_WS('; ', phone, contact_phone, contact_mobile, phone_2, mobile) != '' THEN CONCAT_WS('; ', phone, contact_phone, contact_mobile, phone_2, mobile)
      ELSE NULL END AS osm_phone, 
      CASE
      WHEN osm_id IS NULL THEN null
      WHEN CONCAT_WS('; ', website, contact_website) != '' THEN CONCAT_WS('; ', website, contact_website)
      ELSE NULL END AS osm_website,
CASE WHEN short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL 
	WHEN osm_id IS NOT NULL THEN CONCAT_WS('; ',short_name, official_name, alt_name, old_name) 
	ELSE NULL END AS osm_other_names,
CASE WHEN addr_street IS NULL THEN NULL 
	WHEN osm_id IS NOT NULL THEN LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || addr_postcode, ' ' || addr_city),', ')
	ELSE NULL END AS osm_address,
	  CASE
      WHEN osm_id IS NULL THEN null
      ELSE opening_hours END AS osm_opening_hours,
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
  CASE WHEN source='DRI' THEN 'none provided'
  ELSE osm_id END as original_id,
  CASE WHEN source='DRI' THEN 
  jsonb_build_object('dut', CONCAT(type_post_dut,' ',namedut),
                     'fre', CONCAT(type_post_fre,' ',namefre),
                     'ger', CONCAT(type_post_ger,' ',nameger),
					           'und', CONCAT(type_post_und,' ',nameund))
       WHEN osm_name IS NULL THEN jsonb_build_object('und', 'police station')
  ELSE jsonb_build_object('und', osm_name) END
  as name,
  JSONB_BUILD_OBJECT(
    'dut', concat(type_post_dut),
    'fre', concat(type_post_fre),
    'ger', concat(type_post_ger)  
  ) as legend_item,
  source as name_source, 
  CASE WHEN source='DRI' AND zonenummer IS NOT NULL THEN
  JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
		'address', CONCAT(straatnaam,' ',nummer),
    	'postcode', postcode,
    	'municipality', gemeente,
	  	'zone_name', CONCAT(zone,' (',zonenummer,')')
  		))
  WHEN source='DRI' THEN
  	JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
		'address', fedpol_address,
		'police_users_nl', fedpol_police_users_nl,
	  'police_users_fr', fedpol_police_users_fr,
	  'ocpkeys', fedpol_ocpkeys))
  ELSE JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
    'website', osm_website,
    'phone', osm_phone,
    'email', osm_email,
    'opening hours', osm_opening_hours,
  	'address', osm_address,
  	'other_names', osm_other_names))
  END as properties,
  CASE WHEN source='OSM' AND zonenummer IS NOT NULL THEN
    JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
		'address', CONCAT(straatnaam,' ',nummer),
    	'postcode', postcode,
    	'municipality', gemeente,
		'zone_name', CONCAT(zone,' (',zonenummer,')'),
	    'website (via wikidata)', website,
    	'wikidata',  operator_wikidata))
  WHEN source='OSM' AND zonenummer IS NULL THEN
  	JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
      'address', fedpol_address,
		  'police_users_nl', fedpol_police_users_nl,
	    'police_users_fr', fedpol_police_users_fr,
	    'ocpkeys', fedpol_ocpkeys,
	    'website (via wikidata)', website,
      'wikidata',  operator_wikidata))
  ELSE JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
    'website', osm_website,
    'phone', osm_phone,
    'email', osm_email,
	'opening hours', osm_opening_hours,
  	'address', osm_address,
  	'other_names', osm_other_names,
    'website (via wikidata)', website,
    'wikidata',  operator_wikidata))
  END as properties_secondary,
--  CASE WHEN source='DRI' THEN 'data delivered by DRI.Business.PoliceAccounting at police.belgium.eu>'
--  ELSE CONCAT('https://osm.org/',osm_final_id) END AS metadata,  
  geometry AS geometry,
  CURRENT_DATE as created_at
  FROM prep2;
")




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
SELECT *, 
	CASE WHEN name IS NOT null THEN name
  		 WHEN addr_city IS NOT null THEN addr_city
		 ELSE 'police station' END as name_clean
from osm)


-- left join on OSM id to find the stations already used as local police geometry (we keep the ones that cannot be found in the police table)
INSERT INTO ingestion.police_osm (original_id, name, legend_item, name_source, properties, geometry, created_at)
  SELECT 
  o.osm_id AS original_id,
  JSONB_BUILD_OBJECT('und', o.name_clean)
   as name,
  JSONB_BUILD_OBJECT('und', 'police station') as legend_item,
  o.source as name_source,
  JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	  'other_names', CASE WHEN short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL 
						ELSE CONCAT_WS('; ',short_name, official_name, alt_name, old_name) END,
	  'address', CASE WHEN addr_street IS NULL THEN NULL 
		ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END,
	  'website', 
    CASE WHEN CONCAT_WS('; ', o.website, o.contact_website)='' THEN NULL 
    ELSE CONCAT_WS('; ', o.website, o.contact_website) END,
    'email', 
       CASE WHEN CONCAT_WS('; ', o.email, o.contact_email) ='' THEN NULL
       ELSE CONCAT_WS('; ', o.email, o.contact_email) END,
    'phone', 
       CASE WHEN CONCAT_WS('; ', o.phone, o.contact_phone, o.mobile, o.contact_mobile, o.phone_2) ='' THEN NULL
       ELSE CONCAT_WS('; ', o.phone, o.contact_phone, o.mobile, o.contact_mobile, o.phone_2) END,
    'wikidata', 
       CASE WHEN CONCAT_WS('; ', o.wikidata, o.operator_wikidata) ='' THEN NULL
       ELSE CONCAT_WS('; ', o.wikidata, o.operator_wikidata) END,
    'quality_remark',
       CASE WHEN o.operator_wikidata is not NULL OR EXTRACT(YEAR FROM CURRENT_DATE)-CAST(LEFT(o.check_date, 4) AS NUMERIC)<3 THEN 'not found in official sources but high confidence'
       ELSE 'not found in official sources' END
  ))
  as properties,
  o.geometry as geometry,
  CURRENT_DATE as created_at
  FROM name_cleaned o
  LEFT JOIN ingestion.local_police p ON o.osm_id = p.original_id
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
  "  

WITH merge AS 
(SELECT * FROM ingestion.local_police
  UNION ALL
  SELECT * FROM ingestion.police_osm),

alldata AS (SELECT *, 
CASE WHEN name_source='OSM' THEN '9223ce7e-9c3e-461c-87b1-94e8aea59a13'
ELSE 'c418b715-4e56-4c27-83bf-bd303a779d56' END as data_list_id 
FROM merge)
  

INSERT INTO ingestion.police (id, original_id, name, legend_item, data_list_id, risk_level, properties, properties_secondary, geometry, created_at)
select id, original_id, name, legend_item,
data_list_id::uuid,
0 as risk_level, properties, 
CASE WHEN name_source='OSM' AND properties_secondary <> '{}' THEN 
jsonb_build_object('c418b715-4e56-4c27-83bf-bd303a779d56', properties_secondary)
WHEN name_source='DRI' AND properties_secondary <> '{}' THEN 
jsonb_build_object('9223ce7e-9c3e-461c-87b1-94e8aea59a13', properties_secondary)
ELSE properties_secondary
END AS properties_secondary,	
geometry as geometry,
created_at as created_at
FROM alldata
WHERE geometry IS NOT NULL;
","
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

TransformLocalPolice <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in sql_local_police) {
        dbExecute(con_pg, sql_command)
      }
      print("The SQL functions TransformLocalPolice ran without error")
    },
    error = function(err) {
      print("The SQL functions TransformLocalPolice failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


TransformOSM <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in sql_osm) {
        dbExecute(con_pg, sql_command)
      }
      print("The SQL functions TransformOSM ran without error")
    },
    error = function(err) {
      print("The SQL functions OSMpoints failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


TransformMergeAll <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in sql_merge) {
        dbExecute(con_pg, sql_command)
      }
      print("The SQL functions TransformMergeAll ran without error")
    },
    error = function(err) {
      print("The SQL functions for the merge failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}





### Create fdw views ----
fdw_views_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_police CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_police
AS
SELECT id,
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
FROM transformation.police;
","
ALTER TABLE fdw.fdw_police
OWNER TO paragon;
","
GRANT SELECT ON TABLE fdw.fdw_police TO fdw4dev;
","
GRANT ALL ON TABLE fdw.fdw_police TO paragon;
")

create_fdw_views <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in fdw_views_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("The SQL functions create FDW views ran without error")
    },
    error = function(err) {
      print("The SQL functions for the FDW views failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}



# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

# set to TRUE if you just want to generate data comparison updates but not actually update the database
dry_run<-FALSE

run_smart_update = function() {
  smart_update_process("police", 50, 40, 40, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail,dry_run)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = police_full, schema = "raw_data", table_name = "fed_dri_police_local_mail")  
  CreateImportTable(dataset = fedpol, schema = "raw_data", table_name = "fed_dri_police_fed_mail")  
  CreateImportTable(dataset = osm_all_raw, schema = "raw_data", table_name = "osm_police")  
  CreateImportTable(dataset = zones_cleaned, schema = "raw_data", table_name = "wikidata_be_local_police")  
  CreateImportTable(dataset = distance, schema = "ingestion", table_name = "police_prep")  
  TransformLocalPolice()
  TransformOSM()
  TransformMergeAll()
  run_smart_update()
  #create_transformation_table()
  #create_fdw_views()
}


if(F){
  main_function()
}




# TODO - Processing for OSM improvement (pending DRI permission) ----
# """""""""""""""""" ----
## under the same conditions, create a suggested update for the OSM object

# Select OSM data that might be wrong 
## In Belgium, split by language-region so we can pick the right name
## Not near to a police csv point

## and then the other way around:
#- if the OSM data can be found in the police CSV, we are sure
#- if the OSM data is not found, we can use it as risky data
#- if the police data is found in OSM, we enrich and use the OSM geometry
#- if the police data is not found in OSM, we simply use it as is