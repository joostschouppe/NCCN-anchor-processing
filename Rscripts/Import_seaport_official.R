## ---------------------------
##
## Script name: ETL flow for seaport "recht van voorkoop"
##
## Purpose of script: Load seaport areas for harbour organizations
##
## Author: Joost Schouppe
##
## Date Created: 2024-11-27
##
##
## ---------------------------


# Load environment ----
#  """""""""""""""""" ----------------------

# load external IDs
data_list_id<-"6fdc8724-2b7f-48a2-a755-72bea03903b3"
legend_item_id <- "2c7ed5dc-59ae-4dec-92c7-c40bfec1c683"

readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")


log_folder <- "C:/temp/logs/"
local_folder <- "C:/temp/"

### Load external functions ------

rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))



# Libraries -------------------------------
# """""""""""""""""" ----------------------
library(sf) # simple features packages for handling vector GIS data
library(httr) # generic webservice package
library(tidyverse) # a suite of packages for data wrangling, transformation, plotting, ...
library(ows4R) # interface for OGC webservices


# Download data -----------------------------------------------------------
# """"""""""""""""""----------------------
### Vlaanderen ----

# WFS download as explained on https://inbo.github.io/tutorials/tutorials/spatial_wfs_services/

# Define the base WFS URL
wfs_server <- "https://geo.api.vlaanderen.be/RVV/wfs?service=WFS&version=1.1.0&request=GetCapabilities"

# Build the request URL
build_request_url <- function(start_index) {
  url <- parse_url(wfs_server)
  url$query <- list(
    service = "wfs",
    request = "GetFeature",
    typename = "RVV:Rvvhaven",
    srsName = "EPSG:4326",
    startIndex = start_index,
    maxFeatures = 10000,
    outputFormat = "application/json"
  )
  return(build_url(url))
}

# Initialize variables
all_features <- list()
start_index <- 0
batch_size <- 10000
has_more_features <- TRUE

# Loop to fetch data in batches
while (has_more_features) {
  # Build the request URL for the current batch
  request_url <- build_request_url(start_index)
  
  # Fetch the data
  batch <- read_sf(request_url)
  
  # Check if there are no more features to fetch
  if (nrow(batch) == 0) {
    has_more_features <- FALSE
  } else {
    # Append the fetched features to the list
    all_features <- append(all_features, list(batch))
    # Increment the start index for the next batch
    start_index <- start_index + batch_size
  }
}

# Combine all fetched features into a single data frame
havens <- do.call(rbind, all_features)



# Print the number of features retrieved
cat("Total records retrieved:", nrow(havens), "\n")

# set all column names to lowercase
colnames(havens) <- tolower(colnames(havens))

# set haven to Port of Bruges-Antwerp if haven is "Antwerpen" or "Brugge"
havens <- havens %>%
  mutate(haven = if_else(haven %in% c("Antwerpen", "Brugge"), "Port of Antwerp-Bruges", haven)) %>%
  mutate(haven = if_else(haven =="Gent", "North Sea Port (Belgian part)", haven)) %>%
  mutate(haven = if_else(haven =="Oostende", "Port of Oostende", haven))


#merge polygons with the same name
havens <- havens %>%
  group_by(haven) %>%
  summarise(
    geometry = st_union(geometry),  # Combine geometries
    id = str_c(id, collapse = ", ")  # Concatenate ids with a separator (e.g., ", ")
  ) %>%
  ungroup()
plot(havens$geometry)

# LOAD ----
# """""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.seaport_right_of_first_refusal CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.seaport_right_of_first_refusal
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
  CONSTRAINT seaport_official_pkey PRIMARY KEY (id)
);",paste0("
WITH cleaned AS (
  SELECT
  id as original_id,
  jsonb_build_object('eng',haven)
  as name,
  jsonb_build_object(
    'dut', 'zeehaven (recht van voorkoop)',
    'eng', 'seaport (right of first refusal)',
    'fre', 'port maritime (droit de préemption)',
    'ger', 'Seehafen (Vorkaufsrecht)'
  ) as legend_item,
  '",legend_item_id,"'::uuid as legend_item_id,
  '",data_list_id,"'::uuid as data_list_id,
  1 as risk_level,
  geometry 
  FROM raw_data.vla_dv_havens)

INSERT INTO ingestion.seaport_right_of_first_refusal 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, geometry, created_at)
SELECT
original_id,
name,
legend_item,
legend_item_id,
data_list_id,
1 as risk_level,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.seaport_right_of_first_refusal OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.seaport_right_of_first_refusal TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.seaport_right_of_first_refusal TO pgn_user_airflow;")




### Create transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.seaport_right_of_first_refusal  CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.seaport_right_of_first_refusal 
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
    CONSTRAINT seaport_official_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.seaport_right_of_first_refusal
(id, original_id, name, legend_item, data_list_id, geometry, created_at)
SELECT id, original_id, name, legend_item, data_list_id, geometry, created_at FROM ingestion.seaport_right_of_first_refusal;
","
ALTER TABLE IF EXISTS transformation.seaport_right_of_first_refusal
OWNER to pgn_group_data_team_w;")



### Execute the SQL commands ----
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}



# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-TRUE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("seaport_right_of_first_refusal", 500, 2000, 500, format(Sys.Date(), "%Y-%m-%d"), update_even_if_checks_fail)
}





# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  CreateImportTable(dataset = havens, schema = "raw_data", table_name = "vla_dv_havens") 
  create_ingestion_table()
  #create_transformation_table()
  run_smart_update()

}

if(F){
  main_function()
}



