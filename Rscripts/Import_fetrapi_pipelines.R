## ---------------------------
##
## Script name: ETL flow for pipelines
##
## Purpose of script: Upload pipeline data and upload to GISGOV & Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-12-18
##
##
## ---------------------------


# Load variables ------
#  """""""""""""""""" ------

data_list_id <- "8e9397ed-e8c2-490a-b0e7-32f4bf4e3f84"

li_fetrapi_beacon <- "ce68e5a4-13a5-4eee-804f-1c7840998337"
li_fetrapi_pipeline <- "34457d0f-2269-484b-8ad5-0ab99470ffdd"
li_fetrapi_site <- "0e744e22-d66e-4381-aa89-2dc8cace8868"
li_fetrapi_station <- "264567ce-f82e-4b60-b690-7b668c435804"

dl_fetrapi_beacons <- "05bceaac-ade0-4473-a5a0-bedd17b3d8f5"
dl_fetrapi_pipelines <- "8f0b24b4-cacb-4749-aebf-c684df625caa"
dl_fetrapi_sites_stations <- "b9dabf12-50a9-4137-bb20-39b58a49af87"

# Set parameters ------

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

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

# Set location for files that need to be downloaded manually
offline_storage <-Sys.getenv("OFFLINE_STORAGE")

log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

# Load external functions ------
rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(file.path(rscript_folder,"utils_updated_check_protoanchors.R"))
source(file.path(rscript_folder,"utils.R"))

# Library -----------------------------------------------------------------
# """""""""""""""""" ----------------------
# all are loaded via the utils

### Paragon connection ----

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

get_con<-function(){
  con_pg <- dbConnect(Postgres(),
                      user=postgres_user, 
                      password=postgres_password,
                      host=db_host_name,
                      dbname=db_name,
                      port=5432, 
                      sslmode = 'prefer')
  return(con_pg)
}


### Fetrapi connection ----

base_uri <- Sys.getenv("FETRAPI_BASE_URI")
tenant_id <- Sys.getenv("FETRAPI_TENANT_ID")
client_id <- Sys.getenv("FETRAPI_CLIENT_ID")
# will be updated in 2025
client_secret <- Sys.getenv("FETRAPI_CLIENT_SECRET")
resource_uri <- Sys.getenv("FETRAPI_RESOURCE_URI")
username <- Sys.getenv("FETRAPI_USERNAME")
password <- Sys.getenv("FETRAPI_PASSWORD")
token_url <- paste0("https://login.microsoftonline.com/", tenant_id, "/oauth2/token")




# EXTRACT ----
# """""""""""""""""" ----------------------

check_need_for_download <- function() {

# Find out last extraction date loaded to Curation ----
con_pg<-get_con()
previous_date<- dbGetQuery(con_pg,
                     "SELECT max(extraction_date) as previous_date FROM raw_data.fetrapi_adcr_site")
dbDisconnect(con_pg)


# Download data from Fetrapi ----
# Get Token
body <- list(
  client_id = client_id,
  client_secret = client_secret,
  grant_type = "password",
  resource = resource_uri,
  username = username,
  password = password
)
response <- POST(token_url, body = body)
access_token <- content(response)$access_token

# stop if there is an issue with the token
if (is.null(access_token) || is.na(access_token) || length(access_token) != 1) {
  stop("No access token returned, contact Fetrapi team")
}

# Get list of Datasets Id

find_datasets_url <- paste0(base_uri, "/FindDatasets")

request_body <- '{ "filterCriteria": { "dateTimeFrom": "2020-05-20T19:24:59.123Z", "dateTimeTo": "2030-05-20T19:24:59.123Z", "datasetTypes": [ "ZoI", "Chapter3Export", "ADCRExport" ], "statusCodes": [ "Active", "ValidationSucceeded","MemberApproved","MemberRejected","ValidationFailed","AdminRejected" ] } }'

response_datasets <- POST(
  find_datasets_url,
  add_headers(Authorization = paste("Bearer", access_token)),
  body = request_body,
  add_headers("Content-Type" = "application/json")
)

datasets <- jsonlite::fromJSON(rawToChar(response_datasets$content))
dataset_id <- datasets$datasetId[datasets$datasetType=="ADCRExport"]

dataset_timestamp <<- datasets$dateTimeCreated[datasets$datasetType=="ADCRExport"]

if (previous_date$previous_date[1] == dataset_timestamp) {
  stop('There is no new data to process. Stopping.')
  } else print(paste0('Last processed data was from ',previous_date$previous_date[1],'. Now processing data released at ',dataset_timestamp))


# Get Download link for ADCRExport

get_download_link_url <- paste0(base_uri, "/Dataset/", dataset_id)

response_link <<- GET(
  get_download_link_url,
  add_headers(Authorization = paste("Bearer", access_token))
)

download_link <<- content(response_link, as = "text")
}

# Download the zip of the data

download_and_preprocess_data <- function() {
  
downloaded_data <- GET(gsub('"', '',rawToChar(response_link$content)))


# get the file name
filename <- headers(downloaded_data)$`content-disposition`
filename_start <- regexpr("filename=", filename)
filename <- substr(filename, filename_start + 9, nchar(filename))
date_part <<- substr(filename, 15, 22)
print(paste0("Data being processed for dataset version date: ", date_part))

# write the file
## make sure the local folder exists
dir.create(offline_storage, recursive = TRUE, showWarnings = FALSE)
# remove old content if exists
temp_folder <- file.path(offline_storage, "fetrapi_dumps")
if(run_status){
  unlink(temp_folder, recursive = TRUE)
}
## recreate it as empty folder
dir.create(temp_folder)

writeBin(content(downloaded_data, "raw"), file.path(temp_folder,filename))

#unzip it
unzip(file.path(temp_folder,filename),exdir = temp_folder)



### DATASET "b" ----

b <- st_read(file.path(temp_folder,paste0("FETRAPI_ADCR_b_", date_part, ".shp")))
names(b) <- tolower(names(b))

#add an ID field
b <- b %>%
  mutate(id = row_number())
#set projection
st_crs(b) <- st_crs(31370)



### DATASET "s" ----

s <- st_read(file.path(temp_folder,paste0("FETRAPI_ADCR_s_", date_part, ".shp")))
#set names to lowercase
names(s) <- tolower(names(s))
#add an ID field
s <- s %>%
  mutate(id = row_number())
#set projection
st_crs(s) <- st_crs(31370)



### DATASET "p" ----

p <- st_read(file.path(temp_folder,paste0("FETRAPI_ADCR_p_", date_part, ".shp")))
#set names to lowercase
names(p) <- tolower(names(p))

#add an ID field
p <- p %>%
  mutate(id = row_number())
#set projection
st_crs(p) <- st_crs(31370)

### DATASET "Site" ----

Site <- st_read(file.path(temp_folder,paste0("FETRAPI_ADCR_Site_", date_part, ".shp")))
#set names to lowercase
names(Site) <- tolower(names(Site))
Site <- Site %>%
  mutate(id = row_number())


#add an ID field
Site <- Site %>%
  mutate(id = row_number())
#set projection
st_crs(Site) <- st_crs(31370)



s <- st_transform(s, crs = 4326)
p <- st_transform(p, crs = 4326)
b <- st_transform(b, crs = 4326)
Site <- st_transform(Site, crs = 4326)




# TRANSFORM ----
# """""""""""""""""" ----------------------

# Do checks, filter wrong data, then do checks again ----


# Create empty dataframe to store check results
check_newdata_raw <- data.frame(name = character(),
                                check = character(),
                                results = numeric(),
                                stringsAsFactors = FALSE)

# Perform checks
check_newdata_raw <- perform_check(b, "b", b$geometry, check_newdata_raw, "new data raw")
check_newdata_raw <- perform_check(p, "p", p$geometry, check_newdata_raw, "new data raw")
check_newdata_raw <- perform_check(s, "s", s$geometry, check_newdata_raw, "new data raw")
check_newdata_raw <- perform_check(Site, "Site", Site$geometry, check_newdata_raw, "new data raw")

### CLEAN THE DATA ----
# Fix geometry
p$geometry <- st_make_valid(p$geometry)
b$geometry <- st_make_valid(b$geometry)
s$geometry <- st_make_valid(s$geometry)
Site$geometry <- st_make_valid(Site$geometry)

# Filter missing geometries of all datasets
p <- p[!is.na(p$geometry) & !st_is_empty(p$geometry),]
b <- b[!is.na(b$geometry) & !st_is_empty(b$geometry),]
s <- s[!is.na(s$geometry) & !st_is_empty(s$geometry),]
Site <- Site[!is.na(Site$geometry) & !st_is_empty(Site$geometry),]

# Perform checks again
check_newdata_cleaned <- data.frame(name = character(),
                                    check = character(),
                                    results = numeric(),
                                    stringsAsFactors = FALSE)
check_newdata_cleaned <- perform_check(b, "b", b$geometry, check_newdata_cleaned,"new data cleaned")
check_newdata_cleaned <- perform_check(p, "p", p$geometry, check_newdata_cleaned,"new data cleaned")
check_newdata_cleaned <- perform_check(s, "s", s$geometry, check_newdata_cleaned,"new data cleaned")
check_newdata_cleaned <- perform_check(Site, "Site", Site$geometry, check_newdata_cleaned,"new data cleaned")

bad <- subset(
  check_newdata_cleaned,
  grepl("invalid geometry|missing geometry", check) & check_newdata_cleaned[[3]] > 0
)

if (nrow(bad) > 0) {
  print(bad)
  stop("Invalid or missing geometries found in new_data_cleaned - aborting.")
}

# Prepare for paragon ----

b_paragon <- b %>%
  mutate(id_number = paste0(id_number,"|",owner))

p_paragon <- p %>%
  mutate(id_number=paste0(id_number,"|",id_name,"|",state,"|",product_1,"|",product_2,"|",owner))

s_paragon <- s %>%
  mutate(id_number = paste0(id_number,"|",id_name,"|",`function`,"|",state,"|",product_1,"|",product_2,"|",owner))

# for sites, we create a multipolygon based on all objects that share the same id_number
Site_paragon <- Site %>%
  group_by(id_number) %>%
  summarise(
    geometry = st_union(geometry),
    across(
      .cols = c(id_name,`function`, state, mop,product_1,product_2,owner,operator,ope_t_em_1,delivery_d,id),  # Include only the specified columns
      .fns = ~ .[1]  # Keep the first value of each column
    )
  ) %>%
  ungroup()


b_paragon <<- summarize_by_value(
  df           = b_paragon,
  group_col    = "geometry",
  id_col       = "id_number",
  summary_cols = c("owner", "delivery_d")
)

p_paragon <<- summarize_by_value(
  df           = p_paragon,
  group_col    = "geometry",
  id_col       = "id_number",
  summary_cols = c("id_name", "state", "mop", "dn", "product_1", "product_2", "owner", "operator", "ope_t_em_1", "delivery_d")
)

s_paragon <<- summarize_by_value(
  df           = s_paragon,
  group_col    = "geometry",
  id_col       = "id_number",
  summary_cols = c("id_name", "function", "state", "mop", "product_1", "product_2", "owner", "operator", "ope_t_em_1", "delivery_d")
)

Site_paragon <- summarize_by_value(
  df           = Site_paragon,
  group_col    = "geometry",
  id_col       = "id_number",
  summary_cols = c("id_name", "function", "state", "mop", "product_1", "product_2", "owner", "operator", "ope_t_em_1", "delivery_d")
)

# make the last dataset date available in curation
Site_paragon$extraction_date <- dataset_timestamp

Site_paragon <<- Site_paragon

}
 
# LOAD ----
# """""""""""""""""" ----------------------

# create Paragon anchors ----

### Import to raw data ----
# using  existing function

### Create SQL for ingestion tables ----

ingestion_table_sql_site <- c("
DROP TABLE IF EXISTS ingestion.pipelines_sites CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.pipelines_sites
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
    CONSTRAINT pipelines_sites_pkey PRIMARY KEY (id)
  );
",
paste0("WITH cleaned as (SELECT
id_number AS original_id,
jsonb_build_object('und', id_name) as name,			
jsonb_build_object(
	'eng', 'pipeline sites',
	'dut', 'pijpleiding sites',
	'fre', 'pipeline sites ',
	'ger', 'Rohrleitungstandorte') as legend_item,
function, state, mop,product_1,
NULLIF(product_2, '-') as product_2,
owner,operator,ope_t_em_1 AS operator_phone, delivery_d,
geometry
FROM raw_data.fetrapi_adcr_site)

INSERT INTO ingestion.pipelines_sites
(original_id, name, legend_item, legend_item_id, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",li_fetrapi_site,"'::uuid as legend_item_id,
'",dl_fetrapi_sites_stations,"'::uuid as data_list_id,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'function', function,
	'state', state,
	'mop', mop,
	'product_1', product_1,
	'product_2', product_2,
	'owner', owner,
	'operator', operator,
	'operator_phone', operator_phone,
	'delivery_d', delivery_d)),
ST_ReducePrecision(geometry, 1e-8),
CURRENT_DATE as created_at
FROM cleaned;
"),
"ALTER TABLE ingestion.pipelines_sites OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.pipelines_sites TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.pipelines_sites TO pgn_user_airflow;")

ingestion_table_sql_s <- c("
DROP TABLE IF EXISTS ingestion.pipelines_stations CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.pipelines_stations
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
    CONSTRAINT pipeline_stations_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
id_number AS original_id,
jsonb_build_object('und', id_name) as name,			
jsonb_build_object(
	'eng', 'pipeline stations',
	'dut', 'pijpleiding stations',
	'fre', 'pipeline stations',
	'ger', 'Rohrleitungstationen') as legend_item,
function, state, mop,product_1,
NULLIF(product_2, '-') as product_2,
owner,operator,ope_t_em_1 AS operator_phone, delivery_d,
geometry
FROM raw_data.fetrapi_adcr_s)

INSERT INTO ingestion.pipelines_stations
(original_id, name, legend_item, legend_item_id, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",li_fetrapi_station,"'::uuid as legend_item_id,
'",dl_fetrapi_sites_stations,"'::uuid as data_list_id,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'function', function,
	'state', state,
	'mop', mop,
	'product_1', product_1,
	'product_2', product_2,
	'owner', owner,
	'operator', operator,
	'operator_phone', operator_phone,
	'delivery_d', delivery_d)),
ST_ReducePrecision(geometry, 1e-8),
CURRENT_DATE as created_at
FROM cleaned;
"),
"ALTER TABLE ingestion.pipelines_stations OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.pipelines_stations TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.pipelines_stations TO pgn_user_airflow;")


# NOTE: select code manually
ingestion_table_sql_b <- c("DROP TABLE IF EXISTS ingestion.pipelines_beacons CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.pipelines_beacons
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
  CONSTRAINT pipeline_beacons_pkey PRIMARY KEY (id)
);
",paste0("
WITH cleaned as (SELECT
  id_number AS original_id,
  jsonb_build_object('und', id_number) as name,			
  jsonb_build_object(
    'eng', 'pipeline beacons',
    'dut', 'pijpleiding bakens',
    'fre', 'pipeline balise',
    'ger', 'Rohrleitungleuchtfeuer') as legend_item,
  owner,delivery_d,
  geometry
  FROM raw_data.fetrapi_adcr_b)
INSERT INTO ingestion.pipelines_beacons
(original_id, name, legend_item, legend_item_id, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",li_fetrapi_beacon,"'::uuid as legend_item_id,
'",dl_fetrapi_beacons,"'::uuid as data_list_id,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'owner', owner,
  'delivery_d', delivery_d)),
ST_ReducePrecision(geometry, 1e-8),
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE ingestion.pipelines_beacons OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.pipelines_beacons TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.pipelines_beacons TO pgn_user_airflow;")

                           
ingestion_table_sql_p <- c("
DROP TABLE IF EXISTS ingestion.pipelines CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.pipelines
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
    CONSTRAINT pipelines_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
id_number AS original_id,
jsonb_build_object('und', id_name) as name,			
jsonb_build_object(
	'eng', 'pipelines',
	'dut', 'pijpleidingen',
	'fre', 'pipelines ',
	'ger', 'Rohrleitungen') as legend_item,
state, mop, dn as diameter, product_1,
NULLIF(product_2, '-') as product_2,
owner,operator,ope_t_em_1 AS operator_phone, delivery_d,
ST_LineMerge(geometry) as geometry
FROM raw_data.fetrapi_adcr_p)

INSERT INTO ingestion.pipelines
(original_id, name, legend_item, legend_item_id, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",li_fetrapi_pipeline,"'::uuid as legend_item_id,
'",dl_fetrapi_pipelines,"'::uuid as data_list_id,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'state', state,
	'mop', mop,
	'diameter', diameter,
	'product_1', product_1,
	'product_2', product_2,
	'owner', owner,
	'operator', operator,
	'operator_phone', operator_phone,
	'delivery_d', delivery_d)),
ST_ReducePrecision(geometry, 1e-8),
CURRENT_DATE as created_at
FROM cleaned;
"),
"ALTER TABLE ingestion.pipelines OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.pipelines TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.pipelines TO pgn_user_airflow;")



### Execute the SQL commands ----

create_ingestion_table_site <- function() {execute_sql_commands(ingestion_table_sql_site, "Site ingestion table")}
create_ingestion_table_b <- function() {execute_sql_commands(ingestion_table_sql_b, "B ingestion table")}
create_ingestion_table_s <- function() {execute_sql_commands(ingestion_table_sql_s, "S ingestion table")}
create_ingestion_table_p <- function() {execute_sql_commands(ingestion_table_sql_p, "P ingestion table")}


# Run the functions ----


upload_data = function() {  
  CreateImportTable(dataset = Site_paragon, schema = "raw_data", table_name = "fetrapi_adcr_site")
  CreateImportTable(dataset = s_paragon, schema = "raw_data", table_name = "fetrapi_adcr_s")
  CreateImportTable(dataset = p_paragon, schema = "raw_data", table_name = "fetrapi_adcr_p")
  CreateImportTable(dataset = b_paragon, schema = "raw_data", table_name = "fetrapi_adcr_b")
}
make_ingestion_tables = function() {  
  create_ingestion_table_site()
  create_ingestion_table_s()
  create_ingestion_table_p()
  create_ingestion_table_b()
}


run_smart_update = function() {
  smart_update_process("pipelines_beacons", 5, 250, 50, date_part, allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run, reuse_ingestion_data=reuse_ingestion_data )
  smart_update_process("pipelines", 5, 250, 50, date_part, allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,  reuse_ingestion_data=reuse_ingestion_data, same_id_size_factor = 1.25,different_id_size_factor = 1.25)
  smart_update_process("pipelines_sites", 5, 250, 50, date_part, allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run, reuse_ingestion_data=reuse_ingestion_data)
  smart_update_process("pipelines_stations", 5, 250, 50, date_part, allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run, reuse_ingestion_data=reuse_ingestion_data)
}


main_function = function() {
  if (!reuse_ingestion_data) {
    check_need_for_download()
    download_and_preprocess_data()
    upload_data()
    make_ingestion_tables()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}