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



# Set parameters ------



readRenviron("C:/projects/pgn-data-airflow/.Renviron")
local_folder <- "C:/temp/fetrapi/"
rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"

# Folder for logs
log_folder <- "C:/temp/logs/"

# Load external functions ------
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))



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




### GISGOV connection ----
db_host_name_gg <- Sys.getenv("GISGOV_HOST_NAME")
postgres_user_gg <- Sys.getenv("GISGOV_USERNAME")
postgres_password_gg <- Sys.getenv("GISGOV_PASSWORD")
#postgres_user_gg <- Sys.getenv("TOKEN_GISGOV_USERNAME")
#postgres_password_gg <- Sys.getenv("POSTGRES_PASSWORD")
db_name_gg<- Sys.getenv("GISGOV_DBNAME")

get_con_gg<-function(){
  con_pg_gg <- dbConnect(Postgres(),
                         user=postgres_user_gg, 
                         password=postgres_password_gg,
                         host=db_host_name_gg,
                         dbname=db_name_gg,
                         port=5432, 
                         sslmode = 'prefer')
  return(con_pg_gg)
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
# if access token is empty, update password via fetrapi website
cat("Access token:", access_token, "\n")

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


# Get Download link for ADCRExport

get_download_link_url <- paste0(base_uri, "/Dataset/", dataset_id)

response_link <- GET(
  get_download_link_url,
  add_headers(Authorization = paste("Bearer", access_token))
)

download_link <- content(response_link, as = "text")
cat("Safe download link :", download_link)


# Download the zip of the data


downloaded_data <- GET(gsub('"', '',rawToChar(response_link$content)))


# get the file name
filename <- headers(downloaded_data)$`content-disposition`
filename_start <- regexpr("filename=", filename)
filename <- substr(filename, filename_start + 9, nchar(filename))
date_part <- substr(filename, 15, 22)
print(paste0("Data being processed for dataset version date: ", date_part))

# write the file
dir.create(local_folder, recursive = TRUE, showWarnings = FALSE)
writeBin(content(downloaded_data, "raw"), paste0(local_folder,filename))

#unzip it
unzip(paste0(local_folder,filename),exdir = local_folder)



### DATASET "b" ----

b <- st_read(paste0(local_folder,paste0("FETRAPI_ADCR_b_", date_part, ".shp")))
names(b) <- tolower(names(b))

#add an ID field
b <- b %>%
  mutate(id = row_number())
#set projection
st_crs(b) <- st_crs(31370)



### DATASET "s" ----

s <- st_read(paste0(local_folder,paste0("FETRAPI_ADCR_s_", date_part, ".shp")))
#set names to lowercase
names(s) <- tolower(names(s))
#add an ID field
s <- s %>%
  mutate(id = row_number())
#set projection
st_crs(s) <- st_crs(31370)



### DATASET "p" ----

p <- st_read(paste0(local_folder,paste0("FETRAPI_ADCR_p_", date_part, ".shp")))
#set names to lowercase
names(p) <- tolower(names(p))

#add an ID field
p <- p %>%
  mutate(id = row_number())
#set projection
st_crs(p) <- st_crs(31370)

### DATASET "Site" ----

Site <- st_read(paste0(local_folder,paste0("FETRAPI_ADCR_Site_", date_part, ".shp")))
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


    
# LOAD ----
# """""""""""""""""" ----------------------

# upload new data to GISGOV ----

CreateImportTableGG<-function(dataset, schema, table_name){
  if(exists("dataset")){
    con_pg<-get_con_gg()
    table_id <- DBI::Id(
      schema  = schema,
      table   = table_name
    )
    table_id_t <- paste0(schema,".",table_name)
    start<-Sys.time()
    print(paste0("Start :",format(Sys.time(), "%a %b %d %X %Y")))
    # The table should not exist yet, but if it does (usually because the script is run more than once) we can drop it
    query_drop_table <- paste(
      "DROP TABLE IF EXISTS ", table_id_t, " CASCADE;"
    )
    dbExecute(con_pg, query_drop_table)
    print(paste0("Table ", table_id_t, " dropped (if existed)"))
    
    print(paste0("Insert new data into postgresql table ", table_id_t))
    dbWriteTable(con_pg, table_id, dataset, overwrite = TRUE, row.names = FALSE )
    
    print("ID primary key")
    # Check if a primary key constraint already exists on the table
    query_check_pk <- paste(
      "SELECT a.attname AS pk_column
      FROM   pg_index i
      JOIN   pg_attribute a ON a.attrelid = i.indrelid
                        AND a.attnum = ANY(i.indkey)
      WHERE  i.indrelid = '", table_id_t, "'::regclass
      AND    i.indisprimary"
    )
    result <- dbGetQuery(con_pg, query_check_pk)
    if (nrow(result) > 0) {
      print(paste("Primary key", result$pk_column[1], "already exists."))
    } else {
      query_add_pk <- paste(
        "ALTER TABLE ", table_id_t,
        " ADD PRIMARY KEY (id);"
      )
      dbExecute(con_pg, query_add_pk)
      print("Primary key added.")
    }
    
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
    
  }else{
    print(paste0("Error, the geojson you wanted to import into ", table_id_t, "does not exist, try again"))
  }
}




# Update the GISGOV Materialized View ----



gg_mview_sql_b <- c("
DROP MATERIALIZED VIEW IF EXISTS pipelines.beacons;
",
paste0("CREATE MATERIALIZED VIEW IF NOT EXISTS pipelines.beacons
TABLESPACE pg_default
AS
 SELECT id,
   id_number,
   owner,
   delivery_d,
    geometry::geography AS the_geog
   FROM pipelines.fetrapi_adcr_b_", date_part, "
WITH DATA;
"),"
ALTER TABLE IF EXISTS pipelines.beacons
    OWNER TO gisgov;
","
GRANT SELECT ON TABLE pipelines.beacons TO geoserver;
","
GRANT ALL ON TABLE pipelines.beacons TO gisgov;
","
GRANT SELECT ON TABLE pipelines.beacons TO \"joost.schouppe@nccn.fgov.be\";
")

gg_mview_sql_s <- c("
DROP MATERIALIZED VIEW IF EXISTS pipelines.stations;
",paste0("
CREATE MATERIALIZED VIEW IF NOT EXISTS pipelines.stations
TABLESPACE pg_default
AS
 SELECT id,
    id_number,
    id_name,
    function,
    state,
    mop,
    product_1,
    product_2,
    owner,
    operator,
    ope_t_em_1,
    delivery_d,
    geometry::geography AS the_geog
   FROM pipelines.fetrapi_adcr_s_",date_part,"
WITH DATA;
"),"
ALTER TABLE IF EXISTS pipelines.stations
    OWNER TO gisgov;
","
GRANT SELECT ON TABLE pipelines.stations TO geoserver;
","
GRANT ALL ON TABLE pipelines.stations TO gisgov;
","
GRANT SELECT ON TABLE pipelines.stations TO \"joost.schouppe@nccn.fgov.be\";
")

gg_mview_sql_p <- c("
DROP MATERIALIZED VIEW IF EXISTS pipelines.pipelines;
",paste0("
CREATE MATERIALIZED VIEW IF NOT EXISTS pipelines.pipelines
TABLESPACE pg_default
AS
 SELECT id,
    id_number,
    id_name,
    state,
    mop,
    dn,
    product_1,
    product_2,
    owner,
    operator,
    ope_t_em_1,
    delivery_d,
    geometry::geography AS the_geog
   FROM pipelines.fetrapi_adcr_p_",date_part,"
WITH DATA;
"),"
ALTER TABLE IF EXISTS pipelines.pipelines
    OWNER TO gisgov;
","
GRANT SELECT ON TABLE pipelines.pipelines TO geoserver;
","
GRANT ALL ON TABLE pipelines.pipelines TO gisgov;
","
GRANT SELECT ON TABLE pipelines.pipelines TO \"joost.schouppe@nccn.fgov.be\";
")

gg_mview_sql_site <- c("
DROP MATERIALIZED VIEW IF EXISTS pipelines.sites;
",paste0("
CREATE MATERIALIZED VIEW IF NOT EXISTS pipelines.sites
TABLESPACE pg_default
AS
 SELECT id,
    id_number,
    id_name,
    function,
    state,
    mop,
    product_1,
    product_2,
    owner,
    operator,
    ope_t_em_1,
    delivery_d,
    geometry::geography AS the_geog
   FROM pipelines.fetrapi_adcr_site_",date_part,"
WITH DATA;
"),"
ALTER TABLE IF EXISTS pipelines.sites
    OWNER TO gisgov;
","
GRANT SELECT ON TABLE pipelines.sites TO geoserver;
","
GRANT ALL ON TABLE pipelines.sites TO gisgov;
","
GRANT SELECT ON TABLE pipelines.sites TO \"joost.schouppe@nccn.fgov.be\";
")


execute_sql_commands_gg <- function(sql_commands, task_name) {
  con_pg <- get_con_gg()
  tryCatch(
    {
      for (sql_command in sql_commands) {
        dbExecute(con_pg, sql_command)
      }
      print(paste(task_name, "SQL ran without error"))
    },
    error = function(err) {
      message(paste("The SQL functions for", task_name, "failed"))
      message(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}

gg_create_mview_b <- function() {execute_sql_commands_gg(gg_mview_sql_b, "GISGOV materialized view b")}
gg_create_mview_p <- function() {execute_sql_commands_gg(gg_mview_sql_p, "GISGOV materialized view p")}
gg_create_mview_s <- function() {execute_sql_commands_gg(gg_mview_sql_s, "GISGOV materialized view s")}
gg_create_mview_site <- function() {execute_sql_commands_gg(gg_mview_sql_site, "GISGOV materialized view site")}




#END GISGOV SECTION----







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
function, state, mop,product_1,product_2,owner,operator,ope_t_em_1 AS operator_phone, delivery_d,
geometry
FROM raw_data.fetrapi_adcr_site)

INSERT INTO ingestion.pipelines_sites
(original_id, name, legend_item, legend_item_id, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",li_fetrapi_site,"'::uuid as legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
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
geometry,
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
function, state, mop,product_1,product_2,owner,operator,ope_t_em_1 AS operator_phone, delivery_d,
geometry
FROM raw_data.fetrapi_adcr_s)

INSERT INTO ingestion.pipelines_stations
(original_id, name, legend_item, legend_item_id, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",li_fetrapi_station,"'::uuid as legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
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
geometry,
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
'",data_list_id,"'::uuid as data_list_id,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'owner', owner,
  'delivery_d', delivery_d)),
geometry,
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
state, mop, dn as diameter, product_1,product_2,owner,operator,ope_t_em_1 AS operator_phone, delivery_d,
ST_LineMerge(geometry) as geometry
FROM raw_data.fetrapi_adcr_p)

INSERT INTO ingestion.pipelines
(original_id, name, legend_item, legend_item_id, data_list_id, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",li_fetrapi_pipeline,"'::uuid as legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
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
geometry,
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








# Raw data QA check ----

### Generic checks ----

# Analyze the old data

### Get the old data
con_pg <- get_con_gg()
b_old <- dbGetQuery(con_pg, "SELECT *, ST_AsText(the_geog::geometry) AS geometry FROM pipelines.beacons")
p_old <- dbGetQuery(con_pg, "SELECT *, ST_AsText(the_geog::geometry) AS geometry FROM pipelines.pipelines")
s_old <- dbGetQuery(con_pg, "SELECT *, ST_AsText(the_geog::geometry) AS geometry FROM pipelines.stations")
site_old <- dbGetQuery(con_pg, "SELECT *, ST_AsText(the_geog::geometry) AS geometry FROM pipelines.sites")
dbDisconnect(con_pg)

### Convert to sf
b_old<-st_as_sf(b_old, wkt="geometry")
b_old$geometry <- st_set_crs(b_old$geometry, 4326)
p_old<-st_as_sf(p_old, wkt="geometry")
p_old$geometry <- st_set_crs(p_old$geometry, 4326)
s_old<-st_as_sf(s_old, wkt="geometry")
s_old$geometry <- st_set_crs(s_old$geometry, 4326)
site_old<-st_as_sf(site_old, wkt="geometry")
site_old$geometry <- st_set_crs(site_old$geometry, 4326)


### Create empty dataframe to store check results
check_old_data <- data.frame(name = character(),
                                check = character(),
                                results = numeric(),
                                stringsAsFactors = FALSE)

### Perform checks
check_old_data <- perform_check(b_old, "b", b_old$geometry, check_old_data, "old data")
check_old_data <- perform_check(p_old, "p", p_old$geometry, check_old_data, "old data")
check_old_data <- perform_check(s_old, "s", s_old$geometry, check_old_data, "old data")
check_old_data <- perform_check(site_old, "Site", site_old$geometry, check_old_data, "old data")

# Full outer join of check_old_data and check_newdata_cleaned
check_old_data <- full_join(check_old_data, check_newdata_cleaned, by = c("name", "check"))

# Calculate the difference between old and new data and decides which checks did not pass
# avoid scientific notation
options(scipen = 999)
# allow scientific notation again
#options(scipen = 0)
check_old_data$diff <- check_old_data[["new data cleaned"]] - check_old_data[["old data"]]
check_old_data$diff_p <- abs(check_old_data$diff/check_old_data[["old data"]]*100)


# check failed if any of the following conditions are met
# bbox size or n is different by more than 10% and absolute diff is bigger than 5
check_old_data$failed_check <- ifelse((check_old_data$check == 'bbox size' | check_old_data$check == 'n') & check_old_data$diff > 5 & check_old_data$diff_p > 10, 1, 0)
# invalid or empty geometries are present in the new data
check_old_data$failed_check <- ifelse((check_old_data$check == 'invalid geometry' | check_old_data$check == 'missing geometry') & check_old_data[["new data cleaned"]] > 0, 1, check_old_data$failed_check)


### Context-specific checks ----

# Frequency table of "operator" in old stations dataset
con_pg <- get_con_gg()
old_operator_freq <- dbGetQuery(con_pg, "SELECT operator, count(*) as count_old FROM pipelines.stations GROUP BY operator")
dbDisconnect(con_pg)

# in new dataset
new_operator_freq <- as.data.frame(table(s$operator))
colnames(new_operator_freq) <- c("operator", "count_new")

# Full outer join of old and new operator frequency tables
operator_freq <- full_join(old_operator_freq, new_operator_freq, by = "operator")

# perform checks
operator_freq$diff_abs <- abs(operator_freq$count_old - operator_freq$count_new)
operator_freq$diff_p <- abs(operator_freq$count_old - operator_freq$count_new)/operator_freq$count_old*100

# check fails if there is a big difference in the frequency of operators
operator_freq$failed_check <- ifelse(operator_freq$diff_p > 50 & operator_freq$diff_abs > 5 , 1, 0)
# check fails if there's a new category in the data, or an old one that does not exist anymore
operator_freq$failed_check <- ifelse(is.na(operator_freq$count_old) | is.na(operator_freq$count_new) , 1, operator_freq$failed_check)



# Summarize the results
checks_failed <- sum(check_old_data$failed_check) + sum(operator_freq$failed_check)

# In check_old_data, set all numeric to zero decimals
check_old_data[,3:6] <- lapply(check_old_data[,3:6], function(x) formatC(x, format = "f", digits = 0))
print(check_old_data, row.names=FALSE)

# Write a report ----
source_identifier <- date_part
filename <- paste0(log_folder,format(Sys.time(), "%Y%m%d_%H%M%S"),"_dataprep_check_",source_identifier,".txt")

# Report generic checks
write.table(check_old_data, filename, sep = "\t", quote = FALSE, row.names=FALSE, append = TRUE)
cat("# This is the comparison of old and new data after the new data was cleaned.
  The number of cases with an invalid or missing geometry should always be 0, since missings are removed and invalid geometries are fixed.Check the table below to see if we have had to delete cases.
  We provide the number of cases in the old and new data. If the difference is big, the process will fail.
  We also provide the old and new size in km² of the bbox enclosing all the data.
  The process tries to fix the invalid geometries, and removes the cases with missing geometries. If the difference is big, the process will fail.\n\n", file = filename, append = TRUE)

# Check geometry cleaning
write.table(check_newdata_raw, filename, sep = "\t", quote = FALSE, row.names=FALSE, append = TRUE)
cat("# These are the statistics before we deleted missing geometries and tried to fix invalid geometries.\n\n", file = filename, append = TRUE)

# Add specific checks
write.table(operator_freq, filename, sep = "\t", quote = FALSE, row.names=FALSE, append = TRUE)
cat("# For the 's' dataset, it is expected that the list of operators stays exactly the same. If there is a big shift in the frequencies, or categories are added or disappear, then the process is designed to fail. \n\n", file = filename, append = TRUE)
print(paste0("Report about changes in the data written to ", filename))

# add a stop if checks_failed > 0
if (checks_failed > 0) {
  stop("Quality checks failed. Check the report for more information.")
}

  
 
## GISGOV checks are generic checks to do BEFORE touching our database.
## smart update check comes later, before updating the transformation table.
  
  
  
  # Run the functions ----



gisgov = function() {
  CreateImportTableGG(dataset = Site, schema = "pipelines", table_name = paste0('fetrapi_adcr_site_',date_part)) 
  CreateImportTableGG(dataset = s, schema = "pipelines", table_name = paste0('fetrapi_adcr_s_',date_part)) 
  CreateImportTableGG(dataset = p, schema = "pipelines", table_name = paste0('fetrapi_adcr_p_',date_part)) 
  CreateImportTableGG(dataset = b, schema = "pipelines", table_name = paste0('fetrapi_adcr_b_',date_part)) 
  gg_create_mview_b()
  gg_create_mview_p()
  gg_create_mview_s()
  gg_create_mview_site()
}


# So for paragon, the createimporttable + ingestion functions etc functions are conditional upon the gisgov checks, and the transformation check is conditional on that AND the smart update check itself

paragon_import = function() {  
  CreateImportTable(dataset = Site_paragon, schema = "raw_data", table_name = "fetrapi_adcr_site")
  CreateImportTable(dataset = s_paragon, schema = "raw_data", table_name = "fetrapi_adcr_s")
  CreateImportTable(dataset = p_paragon, schema = "raw_data", table_name = "fetrapi_adcr_p")
  CreateImportTable(dataset = b_paragon, schema = "raw_data", table_name = "fetrapi_adcr_b")
  create_ingestion_table_site()
  create_ingestion_table_s()
  create_ingestion_table_p()
  create_ingestion_table_b()
}


# Smart update parameters (with examples):
# Name of the table in Postgres
# pgsql_table_name<-"pipelines_beacons"
# Max allowed distance for objects with the same ID to be considered the same object
# same_id_distance_threshold<-5
# Max distance to be allowed to be taken in account for "nearby" features (multiple can be left over)
# different_id_distance_raw_threshold<-250
# Threshold distance to decide a new feature is the same if there is only one nearby existing feature
# different_id_distance_unique_threshold<-50
# Identifying name for the version of the dataset that was used for this process
# source_identifier<-date_part
# source_identifier<-"20250703"
# If TRUE, the transformation table will be updated, even if the tests fail. Do this if you have verified that the changes in the data are understandable and acceptable.
# allow_update_even_if_checks_fail <- FALSE






# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

run_smart_update = function() {
  smart_update_process("pipelines_beacons", 5, 250, 50, date_part, update_even_if_checks_fail)
  smart_update_process("pipelines", 5, 250, 50, date_part, update_even_if_checks_fail)
  smart_update_process("pipelines_sites", 5, 250, 50, date_part, update_even_if_checks_fail)  #technical issue; parameters need to be verified when there's actually some change in the data
  smart_update_process("pipelines_stations", 5, 250, 50, date_part, update_even_if_checks_fail)
}



main_function = function() {
  paragon_import()
  run_smart_update()
  gisgov()
}

# uncomment below to ignore failed checks of the raw data (after you have reviewed and verified there is no real issue)
#checks_failed <- 0
# The smart_update_process has built in safeguards on top of the checks_failed. If you want to ignore the checks_failed, you need to set the allow_update_even_if_checks_fail to TRUE in the smart_update_process function (see above).

if(F){
  if (checks_failed == 0) {
    main_function()
  } else {
    stop("Paragon & GISGOV not updated due to failed checks in the initial validation. Please check the reports for details.")
  }
}
