## ---------------------------
##
## Script name: Ingest inland ports for Belgium from Wikidata
##
## Purpose of script: Load Wikidata inland ports & transform into proto-anchors for Paragon
##
## Author: Ric Janssens
##
## Date Created: 2025-02-20
##
##
## ---------------------------

# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------


readRenviron("C:/Code base/RAirflow/pgn-data-airflow/.Renviron")

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


data_list_id<-"90cb62ad-6b6b-4bd4-90fd-e5e71f817c5b" # make one for inland ports

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
    
    # Download inland ports from Wikidata ----
    
    ## Prepare the query
    sparql_query <- "
    SELECT 
    ?entity ?entityLabel ?altLabel (LANG(?entityLabel) AS ?labelLang) (LANG(?altLabel) AS ?altLabelLang) ?coordinate ?geoshape ?territorialEntity ?territorialEntityLabel ?headQuarters ?headQuartersLabel ?location ?locationLabel ?postalCode ?street ?website ?phone ?kbo ?operator ?operatorLabel ?partOf ?partOfLabel ?bodyOfWater ?bodyOfWaterLabel ?inception
    WHERE {
      ?entity wdt:P31 wd:Q863915.  # Instance of inland port
      ?entity wdt:P17 wd:Q31.     # Country = Belgium
      OPTIONAL { ?entity rdfs:label ?entityLabel. FILTER(LANG(?entityLabel) IN (\"nl\", \"fr\", \"de\", \"en\")) }
      OPTIONAL { ?entity skos:altLabel ?altLabel. FILTER(LANG(?altLabel) IN (\"nl\", \"fr\", \"de\")) }
      OPTIONAL { ?entity wdt:P625 ?coordinate. } # Get coordinates (point)
      OPTIONAL { ?entity wdt:P3896 ?geoshape. } # Get geoshape (polygon)
      OPTIONAL { ?entity wdt:P131 ?territorialEntity. }
      OPTIONAL { ?entity wdt:P159 ?headQuarters. }
      OPTIONAL { ?entity wdt:P276 ?location. }
      OPTIONAL { ?entity wdt:P281 ?postalCode. }
      OPTIONAL { ?entity wdt:P6375 ?street. }
      OPTIONAL { ?entity wdt:P856 ?website. }
      OPTIONAL { ?entity wdt:P1329 ?phone. }
      OPTIONAL { ?entity wdt:3376 ?kbo. }
      OPTIONAL { ?entity wdt:P137 ?operator. }
      OPTIONAL { ?entity wdt:361 ?partOf. }
      OPTIONAL { ?entity wdt:P206 ?bodyOfWater. }
      OPTIONAL { ?entity wdt:P571 ?inception. }
      SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\". }
    }"
    
    ## Load the data
    data <- query_wikidata(sparql_query, format = c("simple", "smart"))
    
    # Prepare data for ingestion ----
    
    ## Rows to columns
    inland_ports <- data %>%
      mutate(name = if_else(labelLang == "en", entityLabel, NA_character_),
             name_nl = if_else(labelLang == "nl", entityLabel, NA_character_),
             name_fr = if_else(labelLang == "fr", entityLabel, NA_character_),
             name_de = if_else(labelLang == "de", entityLabel, NA_character_)) %>%
      group_by(entity) %>%
      fill(name, .direction = "downup") %>%
      fill(name_nl, .direction = "downup") %>%
      fill(name_fr, .direction = "downup") %>%
      fill(name_de, .direction = "downup") %>%
      mutate(altLabel = paste0(unique(altLabel), collapse ="; "),
             altLabel = gsub("\\bNA\\b", NA_character_, altLabel)) %>%
      mutate(bodyOfWater = paste0(unique(bodyOfWater), collapse ="; "),
             bodyOfWaterLabel = paste0(unique(bodyOfWaterLabel), collapse = "; ")) %>%
      ungroup() %>%
      distinct(entity, .keep_all = T)
      
    ## Coordinates to geometry
    sf_inland_ports <- st_as_sf(inland_ports, wkt="coordinate")
    sf_inland_ports$coordinate <- st_set_crs(sf_inland_ports$coordinate, 4326)
    
    sf_inland_ports <<- sf_inland_ports
  }
}


# LOAD ----
# """""""""""""""""" ----

# Create SQL for ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.inland_ports CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.inland_ports
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
  CONSTRAINT inland_port_pkey PRIMARY KEY (id)
);",paste0("WITH 
cleaned as (SELECT
            entity as original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', name,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) AS name,
            jsonb_build_object(
              'dut', 'binnenhaven',
              'fre', 'port intérieur',
              'ger', 'Binnenhafen',
              'eng', 'inland port') AS legend_item,
              CASE WHEN \"altLabel\" IS NULL THEN NULL 
                ELSE \"altLabel\" END AS other_names,
              CASE WHEN street IS NULL THEN NULL
                ELSE LTRIM(CONCAT(street, ' ' || \"postalCode\", ' ')) END AS address,
              CASE WHEN \"locationLabel\" IS NULL THEN NULL
                ELSE \"locationLabel\" END AS location,
              \"territorialEntityLabel\" AS territorial_entity,
              CASE WHEN website IS NULL THEN NULL
                ELSE website END AS website,
              CASE WHEN phone IS NULL THEN NULL
                ELSE phone END AS phone,
              \"operatorLabel\" AS operator,
              inception,
              \"bodyOfWaterLabel\" AS body_of_water,
              coordinate AS geometry
              FROM raw_data.wikidata_inland_ports)
INSERT INTO ingestion.inland_ports
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,'",
data_list_id, "' AS data_list_id,
1 AS risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'alt_label', alt_label,
  'address', address,
  'location', location,
  'territorial_entity', territorial_entity,
  'website', website,
	'phone', phone,
	'operator', operator,
  'inception', inception,
  'body_of_water', body_of_water
)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;
"),
  "ALTER TABLE IF EXISTS ingestion.inland_ports OWNER to pgn_group_data_team_w;",
  "GRANT ALL ON TABLE ingestion.inland_ports TO pgn_group_data_team_w;",
  "GRANT ALL ON TABLE ingestion.inland_ports TO pgn_user_airflow;"
)

# Create transformation table ----

transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.inland_ports CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.inland_ports
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
    CONSTRAINT inland_port_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.inland_ports
(id, original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id::uuid, risk_level, properties, geometry, created_at FROM ingestion.inland_ports;",
"ALTER TABLE IF EXISTS transformation.inland_ports OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE transformation.inland_ports TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE transformation.inland_ports TO pgn_user_airflow;"
)

# Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}

run_smart_update = function() {
  smart_update_process("inland_ports", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run, reuse_ingestion_data=reuse_ingestion_data)
}


# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  process_and_upload_fresh_data()
  CreateImportTable(dataset = sf_inland_ports, schema = "raw_data", table_name = "wikidata_inland_ports")
  create_ingestion_table()
  run_smart_update()
  #create_transformation_table()
}

if(run_status){
  main_function()
}









