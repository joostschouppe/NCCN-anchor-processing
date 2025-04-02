## ---------------------------
##
## Script name: Import embassies from OSM
##
## Purpose of script: Load OSM embassies data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-28
##
##
## ---------------------------


# Docs: https://dev.azure.com/NCCN-Paragon/Paragon/_wiki/wikis/Paragon.wiki/418/Embassies-European-institutions

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


data_list_id_embassy<-"ff7308f6-1aed-46d5-988d-354fd0cdc4f6"
data_list_id_eu<- "300fde98-8679-4e47-815e-c2329c74abad"
data_list_id_nato<- "e7f60861-e27a-4941-95b5-ad8c90135792"

log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))

# Libraries -------------------------------
# """""""""""""""""" ----------------------

library(WikidataQueryServiceR)



# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----
process_and_upload_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
  
# Download embassy & EU OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Parameters for EU & embassies
## we download them together, because there is some overlap

### Wikidata query to find relevant keys for EU institutions ----

sparql_query <- "SELECT ?entity ?entityLabel
WHERE {
  {
    # Select entities that are instances of EU institutions
    VALUES ?type { wd:Q748720 wd:Q4936585 }
    ?entity wdt:P31 ?type.
  }
  UNION
  {
    # Select entities that have any of the above as their parent organization (P749)
    VALUES ?type { wd:Q748720 wd:Q4936585 }
    ?parent wdt:P31 ?type.  
    ?entity wdt:P749 ?parent.
  }
  UNION
  {
    # Select entities that are instances of any of the above-found entities
    VALUES ?type { wd:Q748720 wd:Q4936585 }
    ?parent wdt:P31 ?type.
    ?intermediate wdt:P749 ?parent.
    ?entity wdt:P31 ?intermediate.
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\". }
}"

### load the actual data
eu <- query_wikidata(sparql_query, format = c("simple", "smart"))
# remove http://www.wikidata.org/entity/ from entity column
eu$entity <- gsub("http://www.wikidata.org/entity/", "", eu$entity)

# group by unique entity
eu <- unique(eu)

### Download OSM data based on wikidata keys ----



# create a features list based on the entity values, with wikidata=Qxxxxx and operator:wikidata=Qxxxxx and add "office" = "diplomatic" for the embassies, plus "country"="EU" for EU objects missing a wikidata tag
features_list <- c(
  list("office" = "diplomatic", "country" = "EU", "wikidata", "operator:wikidata")
)


# Define extra tags to use as columns for properties
extra_columns <- c("consulate", "country", "target", "diplomatic", "embassy", "liaison", "office", "man_made", "amenity", "government")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation for embassy and EU ----

tryCatch({
  # Call the large function
  osm_all<-download_osm_process(features_list, datatypes, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})


# remove flagpoles
osm_all <- osm_all %>% filter(man_made != "flagpole" | is.na(man_made) )


# indicate that the object is in the list of wikidata items
osm_all <- osm_all %>%
  mutate(wikidata_eu = if_else(wikidata %in% eu$entity | operator_wikidata %in% eu$entity, 1, 0))

# only select relevant items
osm_all <- osm_all %>% filter(wikidata_eu == 1 | office == "diplomatic" | country == "EU")


# classify between embassies and EU institutions
osm_all$category <- ifelse(osm_all$office == "diplomatic", "embassy", "EU institution")



# Download NATO data ----
### Wikidata query to find relevant keys for NATO institutions ----

sparql_query <- "SELECT ?entity ?entityLabel
WHERE {
  ?entity wdt:P361 wd:Q7184.  # Select entities that are part of NATO (Q7184)
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\". }  # Get the labels in English or the auto language
}
"

### load the actual data
nato <- query_wikidata(sparql_query, format = c("simple", "smart"))
# remove http://www.wikidata.org/entity/ from entity column
nato$entity <- gsub("http://www.wikidata.org/entity/", "", nato$entity)
# insert record for NATO itself (Q7184)
nato <- rbind(nato, data.frame(entity="Q7184", entityLabel="NATO"))

### Download OSM data based on wikidata keys ----


# create a features list based on the entity values, with wikidata=Qxxxxx and operator:wikidata=Qxxxxx
features_list <- c(
  set_names(rep(nato$entity, each = 2), rep(c("wikidata", "operator:wikidata"), length(nato$entity)))
)



# Define extra tags to use as columns for properties
extra_columns <- c(
  "landuse", "amenity", "type")
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation for embassy and EU ----

tryCatch({
  # Call the large function
  osm_nato<-download_osm_process(features_list, datatypes, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})


# remove parking
osm_nato <- osm_nato %>% filter(amenity != "parking" | is.na(amenity) )

# set category
osm_nato$category <- "NATO"

# combine all data
osm_all <- bind_rows(osm_all, osm_nato)

# Upload to raw data ----
CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_embassy")

} else {
  print("No fresh data downloaded because user requested to re-use existing data")
  }
}






# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.embassy CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.embassy
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
  CONSTRAINT embassy_pkey PRIMARY KEY (id)
);",paste0("WITH 
cleaned as (SELECT
            'https://osm.org/' || osm_id as original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', CASE WHEN name IS NULL THEN category ELSE name END,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
            CASE WHEN category='embassy' THEN
            jsonb_build_object(
              'dut', 'ambassade',
              'fre', 'ambassade',
              'ger', 'Botschaft',
              'eng', 'embassy') 
            WHEN category='EU institution' THEN
            jsonb_build_object(
              'dut', 'EU-instelling',
              'fre', 'institution de l''UE',
              'ger', 'EU-Institution',
              'eng', 'EU institution')
            ELSE
            jsonb_build_object(
              'dut', 'NATO-instelling',
              'fre', 'institution de l''OTAN',
              'ger', 'NATO-Institution',
              'eng', 'NATO institution')
            END
              as legend_item,
           CASE WHEN short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL 
	ELSE CONCAT_WS('; ',short_name, official_name, alt_name, old_name) END AS other_names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || addr_postcode, ' ' || addr_city),', ') END
	AS address,
CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_email, email) END AS local_email,
operator_email,
CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS local_phone,
CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS local_website,
	operator_website,
            diplomatic as mission_type,
            embassy as embassy_type,
            category,
            geometry
            FROM raw_data.osm_embassy)
INSERT INTO ingestion.embassy 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
CASE WHEN category='embassy' THEN'",data_list_id_embassy,"'
WHEN category='EU institution' THEN'",data_list_id_eu,"'
ELSE'",data_list_id_nato,"' END as data_list_id,
1 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'other_names', other_names,
  'address', address,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
  'mission_type',mission_type,
  'embassy_type',embassy_type
)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"),
"ALTER TABLE IF EXISTS ingestion.bus_tram_metro_routes OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.bus_tram_metro_routes TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.bus_tram_metro_routes TO pgn_user_airflow;"
  )
                            
### Create transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.embassy CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.embassy
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
    CONSTRAINT embassy_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.embassy
(id, original_id, name, legend_item, data_list_id, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id::uuid, properties, geometry, created_at FROM ingestion.embassy;
")








### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}

run_smart_update = function() {
  smart_update_process("embassy", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  process_and_upload_fresh_data()
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
}

if(run_status){
  main_function()
}



